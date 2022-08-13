use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, RequestMessage, StoredMessage, SystemMessage,
};
use crate::bft::communication::{Node, NodeId};
use crate::bft::consensus::log::MemLog;
use crate::bft::consensus::TboQueue;
use crate::bft::sync::AbstractSynchronizer;
use chrono::Utc;
use either::{Left, Right};
use log::debug;
use std::ops::{Deref, DerefMut};

use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::ordering::tbo_pop_message;
use crate::bft::ordering::Orderable;
use crate::bft::ordering::SeqNo;
use crate::bft::sync::follower_sync::FollowerSynchronizer;

use super::AbstractConsensus;

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        extract_msg!({}, FollowerPollStatus::Recv, $g, $q)
    };

    ($opt:block, $rsp:expr, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ConsensusMessage<_>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            FollowerPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            $rsp
        }
    };
}

///Represents the phase that this follower is currently on
///This phase is correspondent on the
#[derive(Debug, Copy, Clone)]
enum FollowerPhase {
    Init,
    PrePreparing,
    Preparing(usize),
    Committing(usize),
}

/// Status returned from processing a consensus message.
pub enum FollowerStatus<'a> {
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

/// Represents the status of calling `poll()` on a `Consensus`.
pub enum FollowerPollStatus<O> {
    /// The `Follower` associated with this `Consensus` should
    /// poll its main channel for more messages.
    Recv,
    /// A new consensus message is available to be processed.
    NextMessage(Header, ConsensusMessage<O>),
}

/// Contains the followed state of an active consensus instance.
/// Does not partake in the consensus
pub struct ConsensusFollower<S: Service> {
    node_id: NodeId,
    phase: FollowerPhase,
    tbo: TboQueue<Request<S>>,

    current: Vec<Digest>,
    current_digest: Digest,
    batch_size: usize,
}

impl<S: Service + 'static> AbstractConsensus<S> for ConsensusFollower<S> {
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
}

impl<S: Service> ConsensusFollower<S> {
    pub fn new(node_id: NodeId, view: ViewInfo, next_seq_num: SeqNo, batch_size: usize) -> Self {
        Self {
            node_id,
            phase: FollowerPhase::Init,
            tbo: TboQueue::new(next_seq_num),
            current_digest: Digest::from_bytes(&[0; Digest::LENGTH][..]).unwrap(),
            current: Vec::with_capacity(batch_size),
            batch_size: batch_size,
        }
    }

    /// Returns true if there is a running consensus instance.
    pub fn is_deciding(&self) -> bool {
        match self.phase {
            FollowerPhase::Init => false,
            _ => true,
        }
    }

    /// Check if we can process new consensus messages.
    /// Checks for messages that have been received
    pub fn poll(&mut self) -> FollowerPollStatus<Request<S>> {
        match self.phase {
            FollowerPhase::Init if self.tbo.get_queue => {
                extract_msg!(
                    {
                        self.phase = FollowerPhase::PrePreparing;
                    },
                    FollowerPollStatus::Recv,
                    &mut self.tbo.get_queue,
                    &mut self.tbo.pre_prepares
                )
            }
            FollowerPhase::Init => FollowerPollStatus::Recv,
            FollowerPhase::PrePreparing if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.pre_prepares)
            }
            FollowerPhase::Preparing(_) if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.prepares)
            }
            FollowerPhase::Committing(_) if self.tbo.get_queue => {
                extract_msg!(&mut self.tbo.get_queue, &mut self.tbo.commits)
            }
            _ => FollowerPollStatus::Recv,
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
        self.phase = FollowerPhase::Init;
        // FIXME: do we need to clear the missing requests buffers?
    }

    pub fn advance_init_phase(&mut self) {
        match self.phase {
            FollowerPhase::Init => self.phase = FollowerPhase::PrePreparing,
            _ => return,
        }
    }

    /// Starts a new consensus instance.
    pub fn next_instance(&mut self) {
        let prev_seq = self.curr_seq.clone();

        self.tbo.next_instance_queue();
    }

    /// Process a message for a particular consensus instance.
    pub fn process_message<'a>(
        &'a mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
        synchronizer: &FollowerSynchronizer<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        node: &Node<S::Data>,
    ) -> FollowerStatus<'a> {
        // FIXME: make sure a replica doesn't vote twice
        // by keeping track of who voted, and not just
        // the amount of votes received
        match self.phase {
            FollowerPhase::Init => {
                // in the init phase, we can't do anything,
                // queue the message for later
                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        self.queue_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(_) => {
                        self.queue_commit(header, message);
                        return FollowerStatus::Deciding;
                    }
                }
            }
            FollowerPhase::PrePreparing => {
                // queue message if we're not pre-preparing
                // or in the same seq as the message
                let view = synchronizer.view();

                let pre_prepare_received_time = Utc::now();

                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                        if message.view() != view.sequence_number() =>
                    {
                        // drop proposed value in a different view (from different leader)
                        debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_)
                        if message.sequence_number() != self.sequence_number() =>
                    {
                        debug!("{:?} // Queued pre prepare message because of seq num {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), self.sequence_number());

                        self.queue_pre_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(request_batch) => {
                        let mut digests = request_batch_received::<S>(
                            header.digest().clone(),
                            request_batch.clone(),
                            log,
                        );
                        self.current_digest = header.digest().clone();
                        self.current.clear();
                        self.current.append(&mut digests);
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!(
                            "{:?} // Received prepare message {:?} while in prepreparing ",
                            self.node_id, d
                        );
                        self.queue_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!(
                            "{:?} // Received commit message {:?} while in pre preparing",
                            self.node_id, d
                        );
                        self.queue_commit(header, message);
                        return FollowerStatus::Deciding;
                    }
                }

                let my_id = node.id();

                let seq = self.sequence_number();
                let view_seq = view.sequence_number();

                {
                    //Update batch meta
                    let mut meta_guard = log.batch_meta().lock();

                    meta_guard.prepare_sent_time = Utc::now();
                    meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                }

                // add message to the log
                log.insert(header, SystemMessage::Consensus(message));

                //Start the count at one since the leader always agrees with his own pre-prepare message
                self.phase = FollowerPhase::Preparing(1);

                FollowerStatus::Deciding
            }
            FollowerPhase::Preparing(i) => {
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

                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d)
                        if message.view() != curr_view.sequence_number() =>
                    {
                        // drop msg in a different view

                        debug!("{:?} // Dropped prepare message {:?} because of view {:?} vs {:?} (ours)",
                            self.node_id, d, message.view(), synchronizer.view().sequence_number());

                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) if d != &self.current_digest => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped prepare message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.current_digest);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d)
                        if message.sequence_number() != self.sequence_number() =>
                    {
                        debug!("{:?} // Queued prepare message {:?} because of seqnumber {:?} vs {:?} (ours)",
                            self.node_id, d, message.sequence_number(), self.sequence_number());

                        self.queue_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => i + 1,
                    ConsensusMessageKind::Commit(d) => {
                        debug!(
                            "{:?} // Received commit message {:?} while in preparing",
                            self.node_id, d
                        );

                        self.queue_commit(header, message);
                        return FollowerStatus::Deciding;
                    }
                };

                // add message to the log
                log.insert(header, SystemMessage::Consensus(message));

                // check if we have gathered enough votes,
                // and transition to a new phase
                self.phase = if i == curr_view.params().quorum() {
                    log.batch_meta().lock().commit_sent_time = Utc::now();

                    //We set at 0 since we broadcast the messages above, meaning we will also receive the message.
                    FollowerPhase::Committing(0)
                } else {
                    FollowerPhase::Preparing(i)
                };

                FollowerStatus::Deciding
            }
            FollowerPhase::Committing(i) => {
                let batch_digest;

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
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!(
                            "{:?} // Received prepare message {:?} while in committing",
                            self.node_id, d
                        );
                        self.queue_prepare(header, message);
                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d)
                        if message.view() != synchronizer.view().sequence_number() =>
                    {
                        // drop msg in a different view
                        debug!("{:?} // Dropped commit message {:?} because of view {:?} vs {:?} (ours)",
                            self.node_id, d, message.view(), synchronizer.view().sequence_number());

                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) if d != &self.current_digest => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped commit message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.current_digest);

                        return FollowerStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d)
                        if message.sequence_number() != self.sequence_number() =>
                    {
                        debug!("{:?} // Queued commit message {:?} because of seqnumber {:?} vs {:?} (ours)",
                            self.node_id, d, message.sequence_number(), self.sequence_number());

                        self.queue_commit(header, message);
                        return FollowerStatus::Deciding;
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
                    self.phase = FollowerPhase::Init;

                    log.batch_meta().lock().consensus_decision_time = Utc::now();

                    FollowerStatus::Decided(batch_digest, &self.current[..self.batch_size])
                } else {
                    self.phase = FollowerPhase::Committing(i);
                    FollowerStatus::Deciding
                }
            }
        }
    }

    /// Create a fake `PRE-PREPARE`. This is useful during the view
    /// change protocol.
    pub fn forge_propose(
        &self,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        synchronizer: &FollowerSynchronizer<S>,
    ) -> SystemMessage<State<S>, Request<S>, Reply<S>> {
        SystemMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            synchronizer.view().sequence_number(),
            ConsensusMessageKind::PrePrepare(requests),
        ))
    }

    pub fn finalize_view_change(
        &mut self,
        (header, message): (Header, ConsensusMessage<Request<S>>),
        synchronizer: &FollowerSynchronizer<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        node: &Node<S::Data>,
    ) {
        //Prepare the algorithm as we are already entering this phase
        self.phase = FollowerPhase::PrePreparing;

        self.process_message(header, message, synchronizer, log, node);
    }
}

impl<S> Deref for ConsensusFollower<S>
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

impl<S> DerefMut for ConsensusFollower<S>
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
    log: &MemLog<State<S>, Request<S>, Reply<S>>,
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

    let mut digests = Vec::with_capacity(requests.len());

    for request in &requests {
        digests.push(request.header().unique_digest());
    }

    //Log the insert
    log.insert_batched(batch_digest, requests);

    digests
}
