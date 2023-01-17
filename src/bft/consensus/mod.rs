//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use ::log::debug;
use chrono::Utc;
use either::Either::{Left, Right};

use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, StoredMessage,
};

use self::log::Log;
use self::log::persistent::PersistentLogModeTrait;
use self::replica_consensus::ReplicaConsensus;

use super::communication::message::{RequestMessage, SystemMessage};
use super::communication::{Node, NodeId};
use super::core::server::follower_handling::FollowerHandle;
use super::core::server::observer::ObserverHandle;
use super::executable::Reply;
use super::globals::ReadOnly;
use super::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{Request, Service, State};
use crate::bft::ordering::{
    tbo_advance_message_queue, tbo_pop_message, tbo_queue_message, Orderable, SeqNo,
};
use crate::bft::timeouts::Timeouts;

pub mod follower_consensus;
pub mod log;
pub mod replica_consensus;

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

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchronicity of the Internet, messages may arrive out of
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
        tbo_queue_message(
            self.curr_seq,
            &mut self.pre_prepares,
            StoredMessage::new(h, m),
        )
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

/// Represents the current phase of the consensus protocol.
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

#[derive(Clone)]
pub struct ConsensusGuard {
    //TODO: Make sure the consensus is locked during view changes and Csts
    consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
    //Consensus atomic bool guard. Set to true when the consensus is locked,
    //Set to false when it is unlocked and the proposer can propose the new
    //Consensus instance
    consensus_guard: Arc<AtomicBool>,
}

impl ConsensusGuard {
    fn new(seq_no: SeqNo, view: ViewInfo) -> Self {
        Self {
            consensus_lock: Arc::new(Mutex::new((seq_no, view))),
            consensus_guard: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn consensus_info(&self) -> &Arc<Mutex<(SeqNo, ViewInfo)>> {
        &self.consensus_lock
    }

    ///Get a reference to the
    pub fn consensus_guard(&self) -> &Arc<AtomicBool> {
        &self.consensus_guard
    }
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
    Decided(Digest, &'a [Digest], Vec<Digest>),
}

/// An abstract consensus trait.
/// Contains the base methods that are required on both followers and replicas
pub trait AbstractConsensus<S: Service> {
    fn sequence_number(&self) -> SeqNo;

    fn install_new_phase(&mut self, phase: &RecoveryState<State<S>, Request<S>>);
}

///Base consensus state machine implementation
pub struct Consensus<S: Service> {
    node_id: NodeId,
    phase: ProtoPhase,
    tbo: TboQueue<Request<S>>,

    //A vector that contains the digest of all requests contained in the batch that is currently being processed
    current: Vec<Digest>,
    //The digest of the entire batch that is currently being processed
    current_digest: Digest,
    //The size of batch that is currently being processed
    current_batch_size: usize,
    //A list of digests of all consensus related messages pertaining to this
    //Consensus instance. Used to keep track of if the persistent log has saved the messages already
    //So the requests can be executed
    current_messages: Vec<Digest>,

    accessory: ConsensusAccessory<S>,
}

///Accessory services for the base consensus instance
/// This is structured like this to reuse as much code as possible so we can reduce fault locations
pub enum ConsensusAccessory<S: Service> {
    Follower,
    Replica(ReplicaConsensus<S>),
}

impl<S: Service + 'static> AbstractConsensus<S> for Consensus<S> {
    fn sequence_number(&self) -> SeqNo {
        self.tbo.curr_seq
    }

    fn install_new_phase(&mut self, recovery_state: &RecoveryState<State<S>, Request<S>>) {
        // get the latest seq no
        let seq_no = {
            let pre_prepares = recovery_state.decision_log().pre_prepares();
            if pre_prepares.is_empty() {
                self.sequence_number()
            } else {
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
}

impl<S: Service + 'static> Consensus<S> {
    pub fn new_replica(
        node_id: NodeId,
        view: ViewInfo,
        next_seq_num: SeqNo,
        batch_size: usize,
        observer_handle: ObserverHandle,
        follower_handle: Option<FollowerHandle<S>>,
    ) -> Self {
        Self {
            node_id,
            phase: ProtoPhase::Init,
            tbo: TboQueue::new(next_seq_num),
            current_digest: Digest::from_bytes(&[0; Digest::LENGTH][..]).unwrap(),
            current: Vec::with_capacity(batch_size),
            current_batch_size: 0,
            current_messages: Vec::new(),
            accessory: ConsensusAccessory::Replica(ReplicaConsensus::new(
                view,
                next_seq_num,
                observer_handle,
                follower_handle,
            )),
        }
    }

    pub fn new_follower(node_id: NodeId, next_seq_num: SeqNo, batch_size: usize) -> Self {
        Self {
            node_id,
            phase: ProtoPhase::Init,
            tbo: TboQueue::new(next_seq_num),
            current_digest: Digest::from_bytes(&[0; Digest::LENGTH][..]).unwrap(),
            current: Vec::with_capacity(batch_size),
            current_batch_size: batch_size,
            current_messages: Vec::new(),
            accessory: ConsensusAccessory::Follower,
        }
    }

    ///Get the kind of consensus state machine this is
    pub fn kind(&self) -> &ConsensusAccessory<S> {
        &self.accessory
    }

    ///Get the consensus guard, only available on replicas
    pub fn consensus_guard(&self) -> Option<&ConsensusGuard> {
        match &self.accessory {
            ConsensusAccessory::Replica(rep) => Some(rep.consensus_guard()),
            _ => None,
        }
    }

    /// Returns true if there is a running consensus instance.
    pub fn is_deciding(&self) -> bool {
        match self.phase {
            ProtoPhase::Init => false,
            _ => true,
        }
    }

    /// Check if we can process new consensus messages.
    /// Checks for messages that have been received
    pub fn poll<T>(
        &mut self,
        log: &Log<S, T>,
    ) -> ConsensusPollStatus<Request<S>> where T: PersistentLogModeTrait {
        match self.phase {
            ProtoPhase::Init if self.tbo.get_queue => {
                extract_msg!(
                    {
                        self.phase = ProtoPhase::PrePreparing;
                    },
                    ConsensusPollStatus::Recv,
                    &mut self.tbo.get_queue,
                    &mut self.tbo.pre_prepares
                )
            }
            ProtoPhase::Init => ConsensusPollStatus::Recv,
            ProtoPhase::PreparingRequests => match &mut self.accessory {
                ConsensusAccessory::Follower => {
                    unreachable!();
                }
                ConsensusAccessory::Replica(rep) => {
                    rep.handle_poll_preparing_requests(self, log)
                }
            },
            ProtoPhase::PrePreparing if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.pre_prepares)
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
                let iterator = self.tbo.pre_prepares.drain(..limit).chain(
                    self.tbo
                        .prepares
                        .drain(..limit)
                        .chain(self.tbo.commits.drain(..limit)),
                );
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

        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_installed_seq_num(self);
            }
        }
    }

    ///Advance from the initial phase
    pub fn advance_init_phase(&mut self) {
        match self.phase {
            ProtoPhase::Init => self.phase = ProtoPhase::PrePreparing,
            _ => return,
        }
    }

    /// Starts a new consensus instance.
    pub fn next_instance(&mut self) {
        self.current.clear();
        self.current_messages.clear();

        let _prev_seq = self.curr_seq.clone();

        self.tbo.next_instance_queue();

        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_next_instance(self);
            }
        }
    }

    /// Create a fake `PRE-PREPARE`. This is useful during the view
    /// change protocol.
    pub fn forge_propose<K>(
        &self,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        synchronizer: &K,
    ) -> SystemMessage<State<S>, Request<S>, Reply<S>>
        where
            K: AbstractSynchronizer<S>,
    {
        SystemMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            synchronizer.view().sequence_number(),
            ConsensusMessageKind::PrePrepare(requests),
        ))
    }

    pub fn finalize_view_change<T>(
        &mut self,
        (header, message): (Header, ConsensusMessage<Request<S>>),
        synchronizer: &Synchronizer<S>,
        timeouts: &Timeouts,
        log: &Log<S, T>,
        node: &Node<S::Data>,
    ) where T: PersistentLogModeTrait {
        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_finalize_view_change(synchronizer);
            }
        }

        //Prepare the algorithm as we are already entering this phase
        self.phase = ProtoPhase::PrePreparing;

        self.process_message(header, message, synchronizer, timeouts, log, node);
    }

    /// Process a message for a particular consensus instance.
    pub fn process_message<'a, T>(
        &'a mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
        synchronizer: &Synchronizer<S>,
        timeouts: &Timeouts,
        log: &Log<S, T>,
        node: &Node<S::Data>,
    ) -> ConsensusStatus<'a> where T: PersistentLogModeTrait {
        // FIXME: make sure a replica doesn't vote twice
        // by keeping track of who voted, and not just
        // the amount of votes received
        match self.phase {
            ProtoPhase::Init => {
                // in the init phase, we can't do anything,
                // queue the message for later
                return match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        ConsensusStatus::Deciding
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        self.queue_prepare(header, message);
                        ConsensusStatus::Deciding
                    }
                    ConsensusMessageKind::Commit(_) => {
                        self.queue_commit(header, message);
                        ConsensusStatus::Deciding
                    }
                }
            }
            ProtoPhase::PrePreparing => {
                // queue message if we're not pre-preparing
                // or in the same seq as the message
                let view = synchronizer.view();

                let pre_prepare_received_time = Utc::now();

                let _request_batch = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                    if message.view() != view.sequence_number() =>
                        {
                            // drop proposed value in a different view (from different leader)
                            debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                            return ConsensusStatus::Deciding;
                        }
                    ConsensusMessageKind::PrePrepare(_) if header.from() != view.leader() => {
                        // Drop proposed value since sender is not leader
                        debug!("{:?} // Dropped pre prepare message because the sender was not the leader {:?} vs {:?} (ours)",
                        self.node_id, header.from(), view.leader());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_)
                    if message.sequence_number() != self.sequence_number() =>
                        {
                            debug!("{:?} // Queued pre prepare message because of seq num {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), self.sequence_number());

                            self.queue_pre_prepare(header, message);
                            return ConsensusStatus::Deciding;
                        }
                    ConsensusMessageKind::PrePrepare(request_batch) => {
                        request_batch
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!(
                            "{:?} // Received prepare message {:?} while in prepreparing ",
                            self.node_id, d
                        );
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!(
                            "{:?} // Received commit message {:?} while in pre preparing",
                            self.node_id, d
                        );
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                };

                //We know that from here on out we will never need to own the message again, so
                //To prevent cloning it, wrap it into this
                let stored_msg = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                let mut digests = request_batch_received(
                    stored_msg.clone(),
                    timeouts,
                    synchronizer,
                    log,
                );

                //Populate the currently active requests
                self.current_digest = header.digest().clone();

                self.current.clear();
                self.current.append(&mut digests);

                self.current_messages.clear();
                self.current_messages.push(header.digest().clone());

                {
                    //Update batch meta
                    let mut meta_guard = log.batch_meta().lock();

                    meta_guard.prepare_sent_time = Utc::now();
                    meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                }

                // add message to the log
                log.insert_consensus(stored_msg.clone());

                match &mut self.accessory {
                    ConsensusAccessory::Follower => {}
                    ConsensusAccessory::Replica(rep) => {
                        //Perform the rest of the necessary operations to handle the received message
                        //If we are a replica
                        rep.handle_preprepare_sucessfull(self, view, stored_msg, node);
                    }
                }

                //Start the count at one since the leader always agrees with his own pre-prepare message
                //So, even if we are not the leader, we count the preprepare message as a prepare message as well
                self.phase = ProtoPhase::Preparing(1);

                ConsensusStatus::Deciding
            }
            ProtoPhase::PreparingRequests => {
                match &mut self.accessory {
                    ConsensusAccessory::Follower => {
                        unreachable!()
                    }
                    ConsensusAccessory::Replica(_) => {
                        // can't do anything while waiting for client requests,
                        // queue the message for later
                        return match message.kind() {
                            ConsensusMessageKind::PrePrepare(_) => {
                                debug!(
                                    "{:?} // Received pre prepare message while in preparing requests",
                                    self.node_id
                                );
                                self.queue_pre_prepare(header, message);
                                ConsensusStatus::Deciding
                            }
                            ConsensusMessageKind::Prepare(_) => {
                                debug!(
                                    "{:?} // Received prepare while in preparing requests",
                                    self.node_id
                                );
                                self.queue_prepare(header, message);
                                ConsensusStatus::Deciding
                            }
                            ConsensusMessageKind::Commit(_) => {
                                debug!(
                                    "{:?} // Received commit message while in preparing requests",
                                    self.node_id
                                );
                                self.queue_commit(header, message);
                                ConsensusStatus::Deciding
                            }
                        }
                    }
                }
            }
            ProtoPhase::Preparing(i) => {
                // queue message if we're not preparing
                // or in the same seq as the message
                let curr_view = synchronizer.view();

                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        debug!(
                            "{:?} // Received pre prepare {:?} message while in preparing",
                            self.node_id,
                            header.digest()
                        );
                        self.queue_pre_prepare(header, message);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d)
                    if message.view() != curr_view.sequence_number() =>
                        {
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
                    ConsensusMessageKind::Prepare(d)
                    if message.sequence_number() != self.sequence_number() =>
                        {
                            debug!("{:?} // Queued prepare message {:?} because of seqnumber {:?} vs {:?} (ours)",
                            self.node_id, d, message.sequence_number(), self.sequence_number());

                            self.queue_prepare(header, message);
                            return ConsensusStatus::Deciding;
                        }
                    ConsensusMessageKind::Prepare(_) => i + 1,
                    ConsensusMessageKind::Commit(d) => {
                        debug!(
                            "{:?} // Received commit message {:?} while in preparing",
                            self.node_id, d
                        );

                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    }
                };

                let stored_msg = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                //Add the message to the messages that must be saved before 
                //We are able to execute the consensus instance
                self.current_messages.push(stored_msg.header().digest().clone());

                // add message to the log
                log.insert_consensus(stored_msg.clone());

                // check if we have gathered enough votes,
                // and transition to a new phase
                self.phase = if i == curr_view.params().quorum() {
                    log.batch_meta().lock().commit_sent_time = Utc::now();

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_preparing_quorum(self, curr_view, stored_msg, log, node);
                        }
                    }

                    //We set at 0 since we broadcast the messages above, meaning we will also receive the message.
                    ProtoPhase::Committing(0)
                } else {
                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_preparing_no_quorum(curr_view, stored_msg, log, node);
                        }
                    }

                    ProtoPhase::Preparing(i)
                };

                ConsensusStatus::Deciding
            }
            ProtoPhase::Committing(i) => {
                let batch_digest;
                let curr_view = synchronizer.view();

                // queue message if we're not committing
                // or in the same seq as the message
                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        debug!(
                            "{:?} // Received pre prepare message {:?} while in committing",
                            self.node_id,
                            header.digest()
                        );
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!(
                            "{:?} // Received prepare message {:?} while in committing",
                            self.node_id, d
                        );
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d)
                    if message.view() != curr_view.sequence_number() =>
                        {
                            // drop msg in a different view
                            debug!("{:?} // Dropped commit message {:?} because of view {:?} vs {:?} (ours)",
                            self.node_id, d, message.view(), curr_view.sequence_number());

                            return ConsensusStatus::Deciding;
                        }
                    ConsensusMessageKind::Commit(d) if d != &self.current_digest => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped commit message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.current_digest);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d)
                    if message.sequence_number() != self.sequence_number() =>
                        {
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

                let stored_message = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                //Add the message to the messages that must be saved before 
                //We are able to execute the consensus instance
                self.current_messages.push(stored_message.header().digest().clone());

                // add message to the log
                log.insert_batched(stored_message.clone());

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

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_committing_quorum(self.sequence_number(), curr_view, stored_message);
                        }
                    }

                    let mut messages = Vec::with_capacity(self.current_messages.len());

                    //clear our local current messages vec and return the messages we need to wait for 
                    //To the necessary place
                    messages.append(&mut self.current_messages);

                    ConsensusStatus::Decided(batch_digest, &self.current[..self.current_batch_size], messages)
                } else {
                    self.phase = ProtoPhase::Committing(i);

                    match &self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(_) => {
                            Self::handle_committing_no_quorum(self, curr_view, stored_message);
                        }
                    }

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
fn request_batch_received<S, T>(
    preprepare: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    timeouts: &Timeouts,
    synchronizer: &Synchronizer<S>,
    log: &Log<S, T>,
) -> Vec<Digest>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
        T: PersistentLogModeTrait
{
    let mut batch_guard = log.batch_meta().lock();

    batch_guard.batch_size = match preprepare.message().kind() {
        ConsensusMessageKind::PrePrepare(req) => {
            req.len()
        }
        _ => { panic!("Wrong message type provided") }
    };
    batch_guard.reception_time = Utc::now();

    //Tell the synchronizer to watch this request batch
    //The synchronizer will also add the batch into the log of requests
    synchronizer.request_batch_received(preprepare, timeouts, log)
}
