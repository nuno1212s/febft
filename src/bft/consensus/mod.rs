//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use ::log::debug;
use chrono::offset::Utc;
use either::{
    Left,
    Right,
};
use intmap::IntMap;
use parking_lot::{Mutex, MutexGuard};

use crate::bft::communication::{
    Node,
    NodeId,
};
use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, Header, Message, RequestMessage, SerializedMessage, StoredMessage, StoredSerializedSystemMessage, SystemMessage, WireMessage};
use crate::bft::communication::serialize::DigestData;
use crate::bft::consensus::log::Log;
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{
    Reply,
    Request,
    Service,
    State,
};
use crate::bft::ordering::{
    Orderable,
    SeqNo,
    tbo_advance_message_queue,
    tbo_pop_message,
    tbo_queue_message,
};
use crate::bft::sync::Synchronizer;
use crate::bft::threadpool;
use crate::bft::timeouts::TimeoutsHandle;

pub mod log;

/// Represents the status of calling `poll()` on a `Consensus`.
pub enum ConsensusPollStatus<O> {
    /// The `Replica` associated with this `Consensus` should
    /// poll its main channel for more messages.
    Recv,
    /// The `Replica` associated with this `Consensus` should
    /// propose a new client request to be ordered, if it is
    /// the leader, and then it should poll its main channel
    /// for more messages. Alternatively, if the request has
    /// already been decided, it should be queued for
    /// execution.
    TryProposeAndRecv,
    /// A new consensus message is available to be processed.
    NextMessage(Header, ConsensusMessage<O>),
}

impl<O> Debug for ConsensusPollStatus<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusPollStatus::Recv => {
                write!(f, "Recv")
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                write!(f, "TryPropose and recv")
            }
            ConsensusPollStatus::NextMessage(_, _) => {
                write!(f, "Next message")
            }
        }
    }
}

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchrony of the Internet, messages may arrive out of
/// context, e.g. for the same consensus instance, a `PRE-PREPARE` reaches
/// a node after a `PREPARE`. A `TboQueue` arranges these messages to be
/// processed in the correct order.
pub struct TboQueue<O> {
    curr_seq: SeqNo,
    get_queue: bool,
    pre_prepares: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
    prepares: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
    commits: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
}

impl<O> Orderable for TboQueue<O> {
    /// Reports the id of the consensus this `TboQueue` is tracking.
    fn sequence_number(&self) -> SeqNo {
        self.curr_seq
    }
}

impl<O> TboQueue<O> {
    fn new(curr_seq: SeqNo) -> Self {
        Self {
            curr_seq,
            get_queue: false,
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// consensus messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    /// Returns the seqno of the next consensus instance
    fn next_instance_no_advance(&self) -> SeqNo {
        self.curr_seq.clone().next()
    }

    /// Advances the message queue, and updates the consensus instance id.
    fn next_instance_queue(&mut self) {
        self.curr_seq = self.curr_seq.next();
        tbo_advance_message_queue(&mut self.pre_prepares);
        tbo_advance_message_queue(&mut self.prepares);
        tbo_advance_message_queue(&mut self.commits);
    }

    /// Queues a consensus message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    pub fn queue(&mut self, h: Header, m: ConsensusMessage<O>) {
        match m.kind() {
            ConsensusMessageKind::PrePrepare(_) => self.queue_pre_prepare(h, m),
            ConsensusMessageKind::Prepare(_) => self.queue_prepare(h, m),
            ConsensusMessageKind::Commit(_) => self.queue_commit(h, m),
        }
    }

    /// Queues a `PRE-PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_pre_prepare(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.curr_seq, &mut self.pre_prepares, StoredMessage::new(h, m))
    }

    /// Queues a `PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_prepare(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.curr_seq, &mut self.prepares, StoredMessage::new(h, m))
    }

    /// Queues a `COMMIT` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_commit(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.curr_seq, &mut self.commits, StoredMessage::new(h, m))
    }
}

/// Repreents the current phase of the consensus protocol.
#[derive(Debug, Copy, Clone)]
enum ProtoPhase {
    /// Start of a new consensus instance.
    Init,
    /// Running the `PRE-PREPARE` phase.
    PrePreparing,
    /// A replica has accepted a `PRE-PREPARE` message, but
    /// it doesn't have the entirety of the requests it references
    /// in its log.
    PreparingRequests,
    /// Running the `PREPARE` phase. The integer represents
    /// the number of votes received.
    Preparing(usize),
    /// Running the `COMMIT` phase. The integer represents
    /// the number of votes received.
    Committing(usize),
}

/// Contains the state of an active consensus instance, as well
/// as future instances.
pub struct Consensus<S: Service> {
    // can be smaller than the config's max batch size,
    // but never longer
    batch_size: usize,
    node_id: NodeId,
    phase: ProtoPhase,
    tbo: TboQueue<Request<S>>,
    current: Vec<Digest>,
    current_digest: Digest,
    //voted: HashSet<NodeId>,
    missing_requests: VecDeque<Digest>,
    missing_swapbuf: Vec<usize>,
    speculative_commits: Arc<Mutex<IntMap<StoredSerializedSystemMessage<S::Data>>>>,
    consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
    consensus_guard: Arc<AtomicBool>,
}

/// Status returned from processing a consensus message.
pub enum ConsensusStatus<'a> {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    Decided(Digest, &'a [Digest]),
}

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        extract_msg!({}, ConsensusPollStatus::Recv, $g, $q)
    };

    ($opt:block, $rsp:expr, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ConsensusMessage<_>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            ConsensusPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            $rsp
        }
    };
}

impl<S> Consensus<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    /// Starts a new consensus protocol tracker.
    pub fn new(initial_seq_no: SeqNo, id: NodeId, batch_size: usize,
               consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
               consensus_guard: Arc<AtomicBool>) -> Self {
        Self {
            batch_size: 0,
            node_id: id,
            phase: ProtoPhase::Init,
            missing_swapbuf: Vec::new(),
            missing_requests: VecDeque::new(),
            //voted: collections::hash_set(),
            tbo: TboQueue::new(initial_seq_no),
            speculative_commits: Arc::new(Mutex::new(IntMap::new())),
            current_digest: Digest::from_bytes(&[0; Digest::LENGTH][..]).unwrap(),
            current: std::iter::repeat_with(|| Digest::from_bytes(&[0; Digest::LENGTH][..]))
                .flat_map(|d| d) // unwrap
                .take(batch_size)
                .collect(),
            consensus_lock,
            consensus_guard,
        }
    }

    /// Update the consensus protocol phase, according to the state
    /// received from peer nodes in the CST protocol.
    pub fn install_new_phase(
        &mut self,
        recovery_state: &RecoveryState<State<S>, Request<S>>,
    ) {
        // get the latest seq no
        let seq_no = {
            let pre_prepares = recovery_state
                .decision_log()
                .pre_prepares();
            if pre_prepares.is_empty() {
                self.sequence_number()
            } else {
                // FIXME: `pre_prepares` len should never be more than one...
                // unless some replica thinks it is the leader, when it fact
                // it is not! we ought to check for such cases! e.g. check
                // if the message was sent to us by the current leader
                pre_prepares[pre_prepares.len() - 1]
                    .message()
                    .sequence_number()
            }
        };

        // skip old messages
        self.install_sequence_number(seq_no);

        // try to fetch msgs from tbo queue
        self.signal();
    }

    pub fn advance_init_phase(&mut self) {
        match self.phase {
            ProtoPhase::Init => self.phase = ProtoPhase::PrePreparing,
            _ => return
        }
    }

    /// Proposes a new request with digest `dig`.
    ///
    /// This function will only succeed if the `node` is
    /// the leader of the current view and the `node` is
    /// in the phase `ProtoPhase::Init`.
    pub fn propose(
        &mut self,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        synchronizer: &Synchronizer<S>,
        node: &Node<S::Data>,
        log: &Log<State<S>, Request<S>, Reply<S>>
    ) {
        //debug!("Phase {:?}", self.phase);

        match self.phase {
            ProtoPhase::Init => self.phase = ProtoPhase::PrePreparing,
            _ => return,
        }

        let view = synchronizer.view();

        //debug!("Is leader? {:?} {:?} = {:?}", node.id(), view.leader(),
        //         node.id() == view.leader());

        if node.id() != view.leader() {
            return;
        }

        let message = SystemMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            view.sequence_number(),
            ConsensusMessageKind::PrePrepare(requests),
        ));

        let targets = NodeId::targets(0..view.params().n());

        node.broadcast(message, targets, Arc::clone(log.batch_meta()));
    }

    /// Returns true if there is a running consensus instance.
    pub fn is_deciding(&self) -> bool {
        match self.phase {
            ProtoPhase::Init => false,
            _ => true,
        }
    }

    /// Create a fake `PRE-PREPARE`. This is useful during the view
    /// change protocol.
    pub fn forge_propose(
        &self,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        synchronizer: &Synchronizer<S>,
    ) -> SystemMessage<State<S>, Request<S>, Reply<S>> {
        SystemMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            synchronizer.view().sequence_number(),
            ConsensusMessageKind::PrePrepare(requests),
        ))
    }

    /// Finalizes the view change protocol, by updating the consensus
    /// phase to `ProtoPhase::Preparing` and broadcasting a `PREPARE`
    /// message.
    pub fn finalize_view_change(
        &mut self,
        digest: Digest,
        synchronizer: &Synchronizer<S>,
        log: &Log<State<S>, Request<S>, Reply<S>>,
        node: &Node<S::Data>,
    ) {
        // update phase
        self.phase = ProtoPhase::Preparing(1);

        // copy digests from PRE-PREPARE
        self.current_digest = digest;

        let dec_log_borrow = log.decision_log().borrow();

        let pre_prepares = dec_log_borrow.pre_prepares();
        let last = &pre_prepares[pre_prepares.len() - 1];

        match last.message().kind() {
            ConsensusMessageKind::PrePrepare(requests) => {
                self.batch_size = requests.len();

                for (i, stored) in requests.iter().enumerate() {
                    self.current[i] = stored.header().unique_digest();
                }
            }
            _ => unreachable!(),
        }

        if node.id() != synchronizer.view().leader() {
            let message = SystemMessage::Consensus(ConsensusMessage::new(
                self.sequence_number(),
                synchronizer.view().sequence_number(),
                ConsensusMessageKind::Prepare(self.current_digest.clone()),
            ));

            let targets = NodeId::targets(0..synchronizer.view().params().n());
            node.broadcast(message, targets, Arc::clone(log.batch_meta()));
        }
    }

    /// Check if we can process new consensus messages.
    pub fn poll(&mut self, log: &Log<State<S>, Request<S>, Reply<S>>) -> ConsensusPollStatus<Request<S>> {
        match self.phase {
            ProtoPhase::Init if self.tbo.get_queue => {
                log.batch_meta().lock().consensus_start_time = Utc::now();

                extract_msg!(
                    { self.phase = ProtoPhase::PrePreparing; },
                    ConsensusPollStatus::TryProposeAndRecv,
                    &mut self.tbo.get_queue,
                    &mut self.tbo.pre_prepares
                )
            }
            ProtoPhase::Init => {
                log.batch_meta().lock().consensus_start_time = Utc::now();

                ConsensusPollStatus::TryProposeAndRecv
            }
            ProtoPhase::PrePreparing if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.pre_prepares)
            }
            ProtoPhase::PreparingRequests => {
                let iterator = self.missing_requests
                    .iter()
                    .enumerate()
                    .filter(|(_index, digest)| log.has_request(digest));
                for (index, _) in iterator {
                    self.missing_swapbuf.push(index);
                }
                for index in self.missing_swapbuf.drain(..) {
                    self.missing_requests.swap_remove_back(index);
                }
                if self.missing_requests.is_empty() {
                    extract_msg!(
                        { self.phase = ProtoPhase::Preparing(1); },
                        ConsensusPollStatus::Recv,
                        &mut self.tbo.get_queue,
                        &mut self.tbo.prepares
                    )
                } else {
                    ConsensusPollStatus::Recv
                }
            }
            ProtoPhase::Preparing(_) if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.prepares)
            }
            ProtoPhase::Committing(_) if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.commits)
            }
            _ => ConsensusPollStatus::Recv,
        }
    }

    /// Starts a new consensus instance.
    pub fn next_instance(&mut self, sync: &Arc<Synchronizer<S>>) {
        self.tbo.next_instance_queue();

        let mut guard = self.consensus_lock.lock();

        *guard = (self.curr_seq, sync.view());

        self.consensus_guard.store(false, Ordering::SeqCst);
    }

    /// Sets the id of the current consensus.
    pub fn install_sequence_number(&mut self, seq: SeqNo) {
        // drop old msgs
        match seq.index(self.sequence_number()) {
            // nothing to do if we are on the same seq
            Right(0) => return,
            // drop messages up to `limit`
            Right(limit) if limit >= self.tbo.pre_prepares.len() => {
                // NOTE: optimization to avoid draining the `VecDeque`
                // structures when the difference between the seq
                // numbers is huge
                self.tbo.pre_prepares.clear();
                self.tbo.prepares.clear();
                self.tbo.commits.clear();
            }
            Right(limit) => {
                let iterator = self.tbo.pre_prepares
                    .drain(..limit)
                    .chain(self.tbo.prepares
                        .drain(..limit)
                        .chain(self.tbo.commits
                            .drain(..limit)));
                for _ in iterator {
                    // consume elems
                }
            }
            // drop all messages
            Left(_) => {
                // NOTE: same as NOTE on the match branch above
                self.tbo.pre_prepares.clear();
                self.tbo.prepares.clear();
                self.tbo.commits.clear();
            }
        }

        // install new phase
        //
        // NOTE: using `ProtoPhase::Init` forces us to queue
        // all messages, which is fine, until we call `install_new_phase`
        self.tbo.curr_seq = seq;
        self.tbo.get_queue = true;
        self.phase = ProtoPhase::Init;

        // FIXME: do we need to clear the missing requests buffers?
    }

    fn take_speculative_commits(&self) -> IntMap<StoredSerializedSystemMessage<S::Data>> {
        let mut map = self.speculative_commits.lock();
        std::mem::replace(&mut *map, IntMap::new())
    }

    /// Process a message for a particular consensus instance.
    pub fn process_message<'a>(
        &'a mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        synchronizer: &Arc<Synchronizer<S>>,
        log: &Log<State<S>, Request<S>, Reply<S>>,
        node: &Node<S::Data>,
    ) -> ConsensusStatus<'a> {
        // FIXME: make sure a replica doesn't vote twice
        // by keeping track of who voted, and not just
        // the amount of votes received
        match self.phase {
            ProtoPhase::Init => {
                log.batch_meta().lock().consensus_start_time = Utc::now();
                // in the init phase, we can't do anything,
                // queue the message for later
                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(_) => {
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                }
            }
            ProtoPhase::PrePreparing => {
                // queue message if we're not pre-preparing
                // or in the same seq as the message
                let view = synchronizer.view();

                let pre_prepare_received_time;

                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_) if message.sequence_number() != self.sequence_number() => {
                        debug!("{:?} // Queued pre prepare message because of seq num {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), self.sequence_number());

                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(request_batch) => {

                        //Acquire the consensus lock as we have officially started the consensus
                        self.consensus_guard.store(true, Ordering::Relaxed);

                        pre_prepare_received_time = Utc::now();

                        let mut digests = request_batch_received(
                            header.digest().clone(),
                            request_batch.clone(),
                            timeouts,
                            synchronizer,
                            log,
                        );

                        self.batch_size = digests.len();
                        self.current_digest = header.digest().clone();
                        self.current.clear();
                        self.current.append(&mut digests);
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!("{:?} // Received prepare message {:?} while in prepreparing ", self.node_id, d);
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!("{:?} // Received commit message {:?} while in pre preparing", self.node_id, d);
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                }

                // start speculatively creating COMMIT messages,
                // which involve potentially expensive signing ops
                let my_id = node.id();

                let seq = self.sequence_number();
                let view_seq = view.sequence_number();

                let sign_detached = node.sign_detached();
                let current_digest = self.current_digest.clone();
                let speculative_commits = Arc::clone(&self.speculative_commits);
                let n = view.params().n();

                //Speculate in another thread.
                threadpool::execute_replicas(move || {

                    // create COMMIT
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        seq,
                        view_seq,
                        ConsensusMessageKind::Commit(current_digest),
                    ));

                    // serialize raw msg
                    let mut buf = Vec::new();
                    let digest = <S::Data as DigestData>::serialize_digest(
                        &message,
                        &mut buf,
                    ).unwrap();

                    for peer_id in NodeId::targets(0..n) {

                        // create header
                        let (header, _) = WireMessage::new(
                            my_id,
                            peer_id,
                            &buf[..],
                            // NOTE: nonce not too important here,
                            // since we already contain enough random
                            // data with the unique digest of the
                            // PRE-PREPARE message
                            0,
                            Some(digest),
                            Some(sign_detached.key_pair()),
                        ).into_inner();

                        // store serialized header + message
                        let serialized = SerializedMessage::new(message.clone(),
                                                                buf.clone());

                        let stored = StoredMessage::new(header, serialized);

                        let mut map = speculative_commits.lock();
                        map.insert(peer_id.into(), stored);
                    }
                });

                // leader can't vote for a PREPARE
                // Since the preprepare message is already "equivalent" to the leaders prepare message
                if node.id() != view.leader() {
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        self.sequence_number(),
                        view.sequence_number(),
                        ConsensusMessageKind::Prepare(self.current_digest.clone()),
                    ));

                    let targets = NodeId::targets(0..view.params().n());

                    node.broadcast(message, targets, Arc::clone(log.batch_meta()));
                }

                {
                    //Update batch meta
                    let mut meta_guard = log.batch_meta().lock();

                    meta_guard.prepare_sent_time = Utc::now();
                    meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                }

                // add message to the log
                log.insert(header, SystemMessage::Consensus(message));

                // try entering preparing phase
                //for digest in self.current.iter().take(self.batch_size).filter(|d| !log.has_request(d)) {
                //    self.missing_requests.push_back(digest.clone());
                //}
                //self.phase = if self.missing_requests.is_empty() {
                //    ProtoPhase::Preparing(1)
                //} else {
                //    ProtoPhase::PreparingRequests
                //};

                //Start the count at one since the leader always agrees with his own pre-prepare message
                self.phase = ProtoPhase::Preparing(1);
                ConsensusStatus::Deciding
            }
            ProtoPhase::PreparingRequests => {
                // can't do anything while waiting for client requests,
                // queue the message for later
                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        debug!("{:?} // Received pre prepare message while in preparing requests", self.node_id);
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        debug!("{:?} // Received prepare while in preparing requests", self.node_id);
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(_) => {
                        debug!("{:?} // Received commit message while in preparing requests", self.node_id);
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                }
            }
            ProtoPhase::Preparing(i) => {
                // queue message if we're not preparing
                // or in the same seq as the message
                let curr_view = synchronizer.view();

                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        debug!("{:?} // Received pre prepare {:?} message while in preparing", self.node_id,
                            header.digest());
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) if message.view() != curr_view.sequence_number() => {
                        // drop msg in a different view

                        debug!("{:?} // Dropped prepare message {:?} because of view {:?} vs {:?} (ours)",
                            self.node_id, d, message.view(), synchronizer.view().sequence_number());
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) if d != &self.current_digest => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped prepare message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.current_digest);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) if message.sequence_number() != self.sequence_number() => {
                        debug!("{:?} // Queued prepare message {:?} because of seqnumber {:?} vs {:?} (ours)",
                            self.node_id, d, message.sequence_number(), self.sequence_number());

                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => i + 1,
                    ConsensusMessageKind::Commit(d) => {
                        debug!("{:?} // Received commit message {:?} while in preparing", self.node_id, d);

                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                };

                // add message to the log
                log.insert(header, SystemMessage::Consensus(message));

                if i == 2 {
                    //Log the time of the first received prepare message
                    log.batch_meta().lock().first_prepare_received = Utc::now();
                }

                // check if we have gathered enough votes,
                // and transition to a new phase
                self.phase = if i == curr_view.params().quorum() {
                    let speculative_commits = self.take_speculative_commits();

                    if valid_spec_commits(&speculative_commits, self, synchronizer) {
                        for (_, msg) in speculative_commits.iter() {
                            debug!("{:?} // Broadcasting speculative commit message {:?} (total of {} messages) to {} targets",
                                 self.node_id, msg.message().original(), speculative_commits.len(), synchronizer.view().params().n());
                            break;
                        }

                        node.broadcast_serialized(speculative_commits, Arc::clone(log.batch_meta()));
                    } else {
                        let message = SystemMessage::Consensus(ConsensusMessage::new(
                            self.sequence_number(),
                            curr_view.sequence_number(),
                            ConsensusMessageKind::Commit(self.current_digest.clone()),
                        ));

                        debug!("{:?} // Broadcasting commit consensus message {:?}", self.node_id, message);

                        let targets = NodeId::targets(0..curr_view.params().n());

                        node.broadcast_signed(message, targets, Arc::clone(log.batch_meta()));
                    }

                    log.batch_meta().lock().commit_sent_time = Utc::now();

                    //Preemptively store the next instance and view and allow the rq handler
                    //to start sending the propose request for the next batch

                    //We set at 0 since we broadcast the messages above, meaning we will also receive the message.
                    ProtoPhase::Committing(0)
                } else {
                    ProtoPhase::Preparing(i)
                };

                ConsensusStatus::Deciding
            }
            ProtoPhase::Committing(i) => {
                let batch_digest;

                // queue message if we're not committing
                // or in the same seq as the message
                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        debug!("{:?} // Received pre prepare message {:?} while in committing", self.node_id,
                         header.digest());
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!("{:?} // Received prepare message {:?} while in committing", self.node_id, d);
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) if message.view() != synchronizer.view().sequence_number() => {
                        // drop msg in a different view
                        debug!("{:?} // Dropped commit message {:?} because of view {:?} vs {:?} (ours)",
                            self.node_id, d, message.view(), synchronizer.view().sequence_number());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) if d != &self.current_digest => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped commit message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.current_digest);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) if message.sequence_number() != self.sequence_number() => {
                        debug!("{:?} // Queued commit message {:?} because of seqnumber {:?} vs {:?} (ours)",
                            self.node_id, d, message.sequence_number(), self.sequence_number());

                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) => {
                        batch_digest = d.clone();

                        i + 1
                    }
                };

                // add message to the log
                log.insert(header, SystemMessage::Consensus(message));

                if i == 1 {
                    //Log the first received commit message
                    log.batch_meta().lock().first_commit_received = Utc::now();
                }

                // check if we have gathered enough votes,
                // and transition to a new phase
                if i == synchronizer.view().params().quorum() {
                    // we have reached a decision,
                    // notify core protocol
                    self.phase = ProtoPhase::Init;
                    log.batch_meta().lock().consensus_decision_time = Utc::now();

                    ConsensusStatus::Decided(batch_digest, &self.current[..self.batch_size])
                } else {
                    self.phase = ProtoPhase::Committing(i);
                    ConsensusStatus::Deciding
                }
            }
        }
    }
}

impl<S> Deref for Consensus<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    type Target = TboQueue<Request<S>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tbo
    }
}

impl<S> DerefMut for Consensus<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tbo
    }
}

#[inline]
fn request_batch_received<S>(
    batch_digest: Digest,
    requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    timeouts: &TimeoutsHandle<S>,
    synchronizer: &Arc<Synchronizer<S>>,
    log: &Log<State<S>, Request<S>, Reply<S>>,
) -> Vec<Digest>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    let mut batch_guard = log.batch_meta().lock();

    batch_guard.batch_size = requests.len();
    batch_guard.reception_time = Utc::now();

    //Tell the synchronizer to watch this request batch
    //The synchronizer will also add the batch into the log of requests
    synchronizer.watch_request_batch(
        batch_digest,
        requests,
        timeouts,
        log,
    )
}

#[inline]
fn valid_spec_commits<S>(
    speculative_commits: &IntMap<StoredSerializedSystemMessage<S::Data>>,
    consensus: &Consensus<S>,
    synchronizer: &Synchronizer<S>,
) -> bool
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    let len = speculative_commits.len();

    let n = synchronizer.view().params().n();
    if len != n {
        debug!("{:?} // Failed to read speculative commits, {} vs {}", consensus.node_id, len, n);

        return false;
    }

    let seq_no = consensus.sequence_number();
    speculative_commits
        .values()
        .map(|stored| {
            match stored.message().original() {
                SystemMessage::Consensus(c) => c,
                _ => unreachable!(),
            }
        })
        .all(|commit| commit.sequence_number() == seq_no)
}
