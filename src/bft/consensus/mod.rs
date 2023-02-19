//! The consensus algorithm used for `febft` and other logic.


use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use std::ops::{Deref, DerefMut};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ::log::debug;
use chrono::Utc;
use either::Either::{Left, Right};
use log::error;

use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, StoredMessage,
};
use crate::bft::consensus::replica_consensus::ReplicaPreparingPollStatus;

use self::replica_consensus::ReplicaConsensus;

use super::communication::message::{RequestMessage, SystemMessage};
use super::communication::{Node, NodeId};
use super::core::server::follower_handling::FollowerHandle;
use super::core::server::observer::ObserverHandle;
use super::executable::Reply;
use super::globals::ReadOnly;
use super::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{ExecutorHandle, Request, Service, State};
use crate::bft::msg_log::decided_log::{BatchExecutionInfo, DecidedLog};
use crate::bft::msg_log::deciding_log::{CompletedBatch, DecidingLog};
use crate::bft::msg_log::decisions::Proof;
use crate::bft::error::*;
use crate::bft::msg_log::Info;

use crate::bft::msg_log::pending_decision::PendingRequestLog;

use crate::bft::ordering::{
    tbo_advance_message_queue, tbo_pop_message, tbo_queue_message, Orderable, SeqNo,
};
use crate::bft::sync::view::ViewInfo;
use crate::bft::timeouts::{Timeout, Timeouts};

pub mod follower_consensus;
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
    PrePreparing(usize),
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
//TODO: Make sure the consensus is locked during view changes and Csts
//This is kind of already done but I'm not 200% sure so I'm leaving this comment
//Situations that have been dealt
pub struct ConsensusGuard {
    consensus_information: Arc<Mutex<(SeqNo, ViewInfo)>>,
    //Consensus atomic bool guard. Set to true when the consensus is locked,
    //Set to false when it is unlocked and the proposer can propose the new
    //Consensus instance
    consensus_guard: Arc<AtomicBool>,
}

impl ConsensusGuard {
    fn new(seq_no: SeqNo, view: ViewInfo) -> Self {
        Self {
            consensus_information: Arc::new(Mutex::new((seq_no, view))),
            consensus_guard: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn consensus_info(&self) -> &Arc<Mutex<(SeqNo, ViewInfo)>> {
        &self.consensus_information
    }

    /// Make it so the proposer is not able to propose any new requests at this time
    pub fn lock_consensus(&self) {
        self.consensus_guard.store(true, Ordering::SeqCst)
    }

    /// Make it so the proposer is able to propose one request. This indicates that the
    /// consensus is ready for a new batch
    pub fn unlock_consensus(&self) {
        self.consensus_guard.store(false, Ordering::SeqCst);
    }

    /// This is for the proposer to check if he can propose a new batch at this time.
    /// The user of the function is then bound to propose a new batch or the
    /// whole system can go unresponsive
    pub fn attempt_to_propose_message(&self) -> std::result::Result<bool, bool> {
        self.consensus_guard.compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
    }
}

/// Status returned from processing a consensus message.
pub enum ConsensusStatus<S> where S: Service {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    /// THe second Vec<Digest> is a vec with digests of the requests contained in the batch
    /// The third is the messages that should be persisted for this batch to be considered persisted
    Decided(CompletedBatch<S>),
}

/// An abstract consensus trait.
/// Contains the base methods that are required on both followers and replicas
pub trait AbstractConsensus<S: Service> {
    fn sequence_number(&self) -> SeqNo;

    fn install_state(&mut self, phase: &RecoveryState<State<S>, Request<S>>);

    /*fn handle_message(&mut self, header: Header, message: ConsensusMessage<Request<S>>,
                      timeouts: &Timeouts,
                      synchronizer: impl AbstractSynchronizer<S>,
                      decided_log: &mut DecidedLog<S>, );*/
}

///Base consensus state machine implementation
pub struct Consensus<S: Service> {
    node_id: NodeId,
    phase: ProtoPhase,
    tbo: TboQueue<Request<S>>,

    // The information about the log that is currently being processed
    deciding_log: DecidingLog<S>,

    // The handle for the executor
    executor_handle: ExecutorHandle<S>,

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

    fn install_state(&mut self, recovery_state: &RecoveryState<State<S>, Request<S>>) {
        // get the latest seq no
        let seq_no = {
            let last_exec = recovery_state.decision_log().last_execution();
            if last_exec.is_none() {
                self.sequence_number()
            } else {
                last_exec.unwrap()
            }
        };

        // skip old messages
        self.install_sequence_number(seq_no);

        // try to fetch msgs from tbo queue
        self.signal();
    }

    /*fn handle_message(&mut self, header: Header, message: ConsensusMessage<Request<S>>,
                      timeouts: &Timeouts, synchronizer: impl AbstractSynchronizer<S>,
                      decided_log: &mut DecidedLog<S>) {
        todo!()
    }*/
}

impl<S: Service + 'static> Consensus<S> {
    pub fn new_replica(
        node_id: NodeId,
        view: ViewInfo,
        next_seq_num: SeqNo,
        executor_handle: ExecutorHandle<S>,
        observer_handle: ObserverHandle,
        follower_handle: Option<FollowerHandle<S>>,
    ) -> Self {
        Self {
            node_id,
            phase: ProtoPhase::Init,
            tbo: TboQueue::new(next_seq_num),
            deciding_log: DecidingLog::new(),
            executor_handle,
            accessory: ConsensusAccessory::Replica(ReplicaConsensus::new(
                view,
                next_seq_num,
                observer_handle,
                follower_handle,
            )),
        }
    }

    pub fn new_follower(node_id: NodeId, next_seq_num: SeqNo,
                        executor_handle: ExecutorHandle<S>, ) -> Self {
        Self {
            node_id,
            phase: ProtoPhase::Init,
            tbo: TboQueue::new(next_seq_num),
            deciding_log: DecidingLog::new(),
            executor_handle,
            accessory: ConsensusAccessory::Follower,
        }
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
    pub fn poll(
        &mut self,
        log: &PendingRequestLog<S>,
    ) -> ConsensusPollStatus<Request<S>> {
        match self.phase {
            ProtoPhase::Init if self.tbo.get_queue => {
                extract_msg!(
                    {
                        self.phase = ProtoPhase::PrePreparing(0);
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
                    match rep.handle_poll_preparing_requests(log) {
                        ReplicaPreparingPollStatus::Recv => {
                            ConsensusPollStatus::Recv
                        }
                        ReplicaPreparingPollStatus::MoveToPreparing => {
                            extract_msg!({
                                self.phase = ProtoPhase::Preparing(1);
                            },
                            ConsensusPollStatus::Recv,
                            &mut self.tbo.get_queue,
                            &mut self.tbo.prepares
                        )
                        }
                    }
                }
            },
            ProtoPhase::PrePreparing(_) if self.tbo.get_queue => {
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
    /// This is done by the CST protocol and we must clear
    /// every request that we have already received via CST from
    /// our message queues
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
                //Remove all messages up to the ones we are currently using
                let iterator =
                    self.tbo.pre_prepares.drain(..limit)
                        .chain(self.tbo.prepares.drain(..limit))
                        .chain(self.tbo.commits.drain(..limit));

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
        self.curr_seq = seq;
        self.tbo.get_queue = true;
        //Move back to the init phase and prepare to process all of the
        //Pending messages so we can quickly catch up
        self.phase = ProtoPhase::Init;
        self.deciding_log.reset();
        // FIXME: do we need to clear the missing requests buffers?

        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_installed_seq_num(seq);
            }
        }
    }

    ///Advance from the initial phase
    pub fn advance_init_phase(&mut self) {
        match self.phase {
            ProtoPhase::Init => self.phase = ProtoPhase::PrePreparing(0),
            _ => return,
        }
    }

    /// Starts a new consensus instance.
    pub fn next_instance(&mut self) {
        self.deciding_log.reset();

        let _prev_seq = self.curr_seq.clone();

        self.tbo.next_instance_queue();

        let current_seq = self.curr_seq;

        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_next_instance(current_seq);
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

    pub fn catch_up_to_quorum(&mut self,
                              seq: SeqNo,
                              proof: Proof<Request<S>>,
                              dec_log: &mut DecidedLog<S>) -> Result<()> {

        // If this is successful, it means that we are all caught up and can now start executing the
        // batch
        let should_execute = dec_log.install_proof(seq, proof)?;

        if let Some(to_execute) = should_execute {
            let (info, update, _) = to_execute.into();

            match info {
                Info::Nil => {
                    self.executor_handle.queue_update(update)
                }
                Info::BeginCheckpoint => {
                    self.executor_handle.queue_update_and_get_appstate(update)
                }
            }.unwrap();
        }

        // Move to the next instance as this one has been finalized
        self.next_instance();

        Ok(())
    }

    pub fn finalize_view_change(
        &mut self,
        (header, message): (Header, ConsensusMessage<Request<S>>),
        synchronizer: &Synchronizer<S>,
        timeouts: &Timeouts,
        log: &mut DecidedLog<S>,
        node: &Node<S::Data>,
    ) {
        match &mut self.accessory {
            ConsensusAccessory::Follower => {}
            ConsensusAccessory::Replica(rep) => {
                rep.handle_finalize_view_change(synchronizer);
            }
        }

        self.deciding_log.reset();

        //Prepare the algorithm as we are already entering this phase

        //TODO: when we finalize a view change, we want to treat the pre prepare request
        // As the only pre prepare, since it already has info provided by everyone in the network.
        // Therefore, this should go straight to the Preparing phase instead of waiting for
        // All the view's leaders.
        self.phase = ProtoPhase::PrePreparing(0);

        self.process_message(header, message, synchronizer, timeouts, log, node);
    }

    /// Process a message for a particular consensus instance.
    pub fn process_message<'a>(
        &'a mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
        synchronizer: &Synchronizer<S>,
        timeouts: &Timeouts,
        log: &mut DecidedLog<S>,
        node: &Node<S::Data>,
    ) -> ConsensusStatus<S> {
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
                };
            }
            ProtoPhase::PrePreparing(i) => {
                // queue message if we're not pre-preparing
                // or in the same seq as the message
                let view = synchronizer.view();

                let pre_prepare_received_time = Utc::now();

                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                    if message.view() != view.sequence_number() =>
                        {
                            // drop proposed value in a different view (from different leader)
                            debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                            return ConsensusStatus::Deciding;
                        }
                    ConsensusMessageKind::PrePrepare(_) if !view.leader_set().contains(&header.from()) => {
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
                        i + 1
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

                if i == 1 {
                    //If this is the first pre prepare message we have received.
                    self.deciding_log.processing_new_round(&view);
                }

                //We know that from here on out we will never need to own the message again, so
                //To prevent cloning it, wrap it into this
                let stored_msg = Arc::new(ReadOnly::new(
                    StoredMessage::new(header, message)));

                // Notify the synchronizer and other things that we have received this batch
                let mut digests = request_batch_received(
                    &stored_msg,
                    timeouts,
                    synchronizer,
                    &self.deciding_log,
                );

                //Store the currently deciding batch in the deciding log.
                //Also update the rest of the fields to reflect this
                let current_digest = self.deciding_log.processing_batch_request(
                    stored_msg.clone(),
                    stored_msg.header().digest().clone(),
                    digests,
                );

                if let Err(err) = &current_digest {
                    //There was an error when analysing the batch of requests, so it won't be counted
                    panic!("Failed to analyse request batch {:?}", err);

                    return ConsensusStatus::Deciding;
                }

                let current_digest = current_digest.unwrap();

                // add message to the log
                log.insert_consensus(stored_msg.clone());

                self.phase = if i == view.leader_set().len() {

                    //We have received all pre prepare requests for this consensus instance
                    //We are now ready to broadcast our prepare message and move to the next phase
                    {
                        //Update batch meta
                        let mut meta_guard = self.deciding_log.batch_meta().lock().unwrap();

                        meta_guard.prepare_sent_time = Utc::now();
                        meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                    }

                    let seq_no = self.sequence_number();
                    let (current_digest, pre_prepare_ordering) =
                        current_digest.expect("Received all messages but still can't calculate batch digest?");

                    // Register that all of the batches have been received
                    // The digest of the batch and the order of the batches
                    log.all_batches_received(current_digest, pre_prepare_ordering);

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            //Perform the rest of the necessary operations to handle the received message
                            //If we are a replica (In particular, bcast the messages)
                            rep.handle_pre_prepare_successful(seq_no,
                                                              current_digest,
                                                              view, stored_msg, node);
                        }
                    }

                    // We no longer start the count at 1 since all leaders must also send the prepare
                    // message with the digest of the entire batch
                    ProtoPhase::Preparing(0)
                } else {
                    ProtoPhase::PrePreparing(i)
                };

                ConsensusStatus::Deciding
            }
            ProtoPhase::PreparingRequests => {
                match &self.accessory {
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
                        };
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
                    ConsensusMessageKind::Prepare(d) if *d != self.deciding_log.current_digest().unwrap() => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped prepare message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.deciding_log.current_digest());
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

                let stored_msg = Arc::new(ReadOnly::new(
                    StoredMessage::new(header, message)));

                //Add the message to the messages that must be saved before 
                //We are able to execute the consensus instance
                self.deciding_log.register_consensus_message(stored_msg.header().digest().clone());

                // add message to the log
                log.insert_consensus(stored_msg.clone());

                // check if we have gathered enough votes,
                // and transition to a new phase
                self.phase = if i == curr_view.params().quorum() {
                    self.deciding_log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();

                    let seq_no = self.sequence_number();
                    let current_digest = self.deciding_log.current_digest().unwrap();

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_preparing_quorum(seq_no,
                                                        current_digest,
                                                        curr_view, stored_msg,
                                                        &self.deciding_log, node);
                        }
                    }

                    //We set at 0 since we broadcast the messages above, meaning we will also receive the message.
                    ProtoPhase::Committing(0)
                } else {
                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_preparing_no_quorum(curr_view, stored_msg, node);
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
                    ConsensusMessageKind::Commit(d) if *d != self.deciding_log.current_digest().unwrap() => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped commit message {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, d, d, self.deciding_log.current_digest());

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
                self.deciding_log.register_consensus_message(stored_message.header().digest().clone());

                if i == 1 {
                    //Log the first received commit message
                    self.deciding_log.batch_meta().lock().unwrap().first_commit_received = Utc::now();
                }

                log.insert_consensus(stored_message.clone());

                // check if we have gathered enough votes,
                // and transition to a new phase
                if i == curr_view.params().quorum() {
                    // we have reached a decision,
                    // notify core protocol
                    self.phase = ProtoPhase::Init;

                    self.deciding_log.batch_meta().lock().unwrap().consensus_decision_time = Utc::now();

                    let seq_no = self.sequence_number();

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_committing_quorum(seq_no, curr_view, stored_message);
                        }
                    }

                    let processed_batch = self.deciding_log.finish_processing_batch().unwrap();

                    ConsensusStatus::Decided(processed_batch)
                } else {
                    self.phase = ProtoPhase::Committing(i);

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            rep.handle_committing_no_quorum(curr_view, stored_message);
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
fn request_batch_received<S>(
    pre_prepare: &StoredMessage<ConsensusMessage<Request<S>>>,
    timeouts: &Timeouts,
    synchronizer: &Synchronizer<S>,
    log: &DecidingLog<S>,
) -> Vec<Digest>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    let mut batch_guard = log.batch_meta().lock().unwrap();

    batch_guard.batch_size = match pre_prepare.message().kind() {
        ConsensusMessageKind::PrePrepare(req) => {
            req.len()
        }
        _ => { panic!("Wrong message type provided") }
    };
    batch_guard.reception_time = Utc::now();

    //Tell the synchronizer to watch this request batch
    synchronizer.request_batch_received(pre_prepare, timeouts)
}
