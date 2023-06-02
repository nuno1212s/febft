use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;
use chrono::Utc;
use log::{debug, info, warn};
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::Node;
use atlas_execution::serialize::SharedData;
use atlas_core::serialize::StateTransferMessage;
use atlas_core::timeouts::Timeouts;
use atlas_metrics::metrics::{metric_duration};
use crate::bft::consensus::accessory::{AccessoryConsensus, ConsensusDecisionAccessory};
use crate::bft::consensus::accessory::replica::ReplicaAccessory;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::metric::{ConsensusMetrics, PRE_PREPARE_ANALYSIS_ID};
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::deciding_log::{CompletedBatch, DecidingLog};
use crate::bft::msg_log::decisions::IncompleteProof;
use crate::bft::PBFT;
use crate::bft::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::sync::view::ViewInfo;

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        extract_msg!(ConsensusPollStatus::Recv, $g, $q)
    };
    ($rsp:expr, $g:expr, $q:expr) => {
        if let Some(stored) = $q.pop_front() {
            let (header, message) = stored.into_inner();

            DecisionPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            $rsp
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// What phase are we in the current decision phase
pub enum DecisionPhase {
    Initialize,
    /// The node is waiting for Pre Prepare messages from the leader.
    PrePreparing(usize),
    /// The node is waiting for Prepare messages from the other nodes.
    Preparing(usize),
    /// The node is waiting for Commit messages from the other nodes.
    Committing(usize),
    /// The node has decided on the execution of a batch of requests.
    Decided,
}

/// Poll result of a given consensus decision
#[derive(Clone)]
pub enum DecisionPollStatus<O> {
    // List this consensus decision as proposeable
    TryPropose,
    // Receive a message from the network
    Recv,
    // We currently have a message to be processed at this time
    NextMessage(Header, ConsensusMessage<O>),
    // This consensus decision is finished and therefore can be finalized
    Decided,
}

#[derive(Debug, Clone)]
pub enum DecisionStatus {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// Transitioned to another next phase of the consensus decision
    Transitioned,
    /// The message has been queued for later execution
    /// As such this consensus decision should be signaled
    Queued,
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    /// THe second Vec<Digest> is a vec with digests of the requests contained in the batch
    /// The third is the messages that should be persisted for this batch to be considered persisted
    Decided,
}

/// A message queue for this particular consensus instance
pub struct MessageQueue<O> {
    get_queue: bool,
    pre_prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
    prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
    commits: VecDeque<StoredMessage<ConsensusMessage<O>>>,
}

/// The information needed to make a decision on a batch of requests.
pub struct ConsensusDecision<D: SharedData + 'static, ST: StateTransferMessage + 'static> {
    node_id: NodeId,
    /// The sequence number of this consensus decision
    seq: SeqNo,
    /// The current phase of this decision
    phase: DecisionPhase,
    /// The queue of messages for this consensus instance
    message_queue: MessageQueue<D::Request>,
    /// The log of messages for this consensus instance
    message_log: DecidingLog<D::Request>,
    /// Accessory to the base consensus state machine
    accessory: ConsensusDecisionAccessory<D, ST>,
    // Metrics about the consensus instance
    consensus_metrics: ConsensusMetrics,
    //TODO: Store things directly into the persistent log as well as delete them when
    // Things go wrong
}


impl<O> MessageQueue<O> {
    fn new() -> Self {
        Self {
            get_queue: false,
            pre_prepares: Default::default(),
            prepares: Default::default(),
            commits: Default::default(),
        }
    }

    pub(super) fn from_messages(pre_prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
                                prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
                                commits: VecDeque<StoredMessage<ConsensusMessage<O>>>) -> Self {
        let get_queue = !pre_prepares.is_empty() || !prepares.is_empty() || !commits.is_empty();

        Self {
            get_queue,
            pre_prepares,
            prepares,
            commits,
        }
    }

    fn signal(&mut self) {
        self.get_queue = true;
    }

    pub fn is_signalled(&self) -> bool { self.get_queue }

    fn queue_pre_prepare(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.pre_prepares.push_back(message);

        self.signal();
    }

    fn queue_prepare(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.prepares.push_back(message);

        self.signal();
    }

    fn queue_commit(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.commits.push_back(message);

        self.signal();
    }
}

impl<D: SharedData + 'static, ST: StateTransferMessage + 'static> ConsensusDecision<D, ST> {
    pub fn init_decision(node_id: NodeId, seq_no: SeqNo, view: &ViewInfo) -> Self {
        Self {
            node_id,
            seq: seq_no,
            phase: DecisionPhase::Initialize,
            message_queue: MessageQueue::new(),
            message_log: DecidingLog::new(node_id, seq_no, view),
            accessory: ConsensusDecisionAccessory::Replica(ReplicaAccessory::new()),
            consensus_metrics: ConsensusMetrics::new(),
        }
    }

    pub fn init_with_msg_log(node_id: NodeId, seq_no: SeqNo, view: &ViewInfo,
                             message_queue: MessageQueue<D::Request>) -> Self {
        Self {
            node_id,
            seq: seq_no,
            phase: DecisionPhase::Initialize,
            message_queue,
            message_log: DecidingLog::new(node_id, seq_no, view),
            accessory: ConsensusDecisionAccessory::Replica(ReplicaAccessory::new()),
            consensus_metrics: ConsensusMetrics::new(),
        }
    }

    pub fn queue(&mut self, header: Header, message: ConsensusMessage<D::Request>) {
        match message.kind() {
            ConsensusMessageKind::PrePrepare(_) => {
                self.message_queue.queue_pre_prepare(StoredMessage::new(header, message));
            }
            ConsensusMessageKind::Prepare(_) => {
                self.message_queue.queue_prepare(StoredMessage::new(header, message));
            }
            ConsensusMessageKind::Commit(_) => {
                self.message_queue.queue_commit(StoredMessage::new(header, message));
            }
        }
    }

    pub fn poll(&mut self) -> DecisionPollStatus<D::Request> {
        return match self.phase {
            DecisionPhase::Initialize => {
                self.phase = DecisionPhase::PrePreparing(0);

                self.consensus_metrics.consensus_started();

                DecisionPollStatus::TryPropose
            }
            DecisionPhase::PrePreparing(_) if self.message_queue.get_queue => {
                extract_msg!(DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.pre_prepares)
            }
            DecisionPhase::Preparing(_) if self.message_queue.get_queue => {
                extract_msg!(DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.prepares)
            }
            DecisionPhase::Committing(_) if self.message_queue.get_queue => {
                extract_msg!(DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.commits)
            }
            DecisionPhase::Decided => DecisionPollStatus::Decided,
            _ => DecisionPollStatus::Recv
        };
    }

    /// Allows us to skip the initialization phase of this consensus instance
    /// This is useful when we don't want the proposer to receive authorization
    /// To propose into this consensus instance.
    pub fn skip_init_phase(&mut self) {
        self.phase = DecisionPhase::PrePreparing(0);
    }

    /// Update the current view of this consensus instance
    pub fn update_current_view(&mut self, view: &ViewInfo) {
        self.message_log.update_current_view(view);
    }

    /// Process a message relating to this consensus instance
    pub fn process_message<NT>(&mut self,
                               header: Header,
                               message: ConsensusMessage<D::Request>,
                               synchronizer: &Synchronizer<D>,
                               timeouts: &Timeouts,
                               log: &mut Log<D>,
                               node: &NT) -> Result<DecisionStatus>
        where NT: Node<PBFT<D, ST>> {
        let view = synchronizer.view();

        return match self.phase {
            DecisionPhase::Initialize => {
                // The initialize phase will only be skipped by polling
                // This consensus instance.
                warn!("{:?} // Queueing message {:?} as we are in the initialize phase", self.node_id, message);

                self.queue(header, message);

                return Ok(DecisionStatus::Queued);
            }
            DecisionPhase::PrePreparing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                    if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::PrePrepare(_)
                    if !view.leader_set().contains(&header.from()) => {
                        // Drop proposed value since sender is not leader
                        debug!("{:?} // Dropped pre prepare message because the sender was not the leader {:?} vs {:?} (ours)",
                        self.node_id, header.from(), view.leader());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::PrePrepare(_)
                    if message.sequence_number() != self.seq => {
                        //Drop proposed value since it is not for this consensus instance
                        warn!("{:?} // Dropped pre prepare message because the sequence number was not the same {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), self.seq);

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!("{:?} // Received {:?} from {:?} while in prepreparing ",
                            self.node_id, message, header.from());

                        self.message_queue.queue_prepare(StoredMessage::new(header, message));

                        return Ok(DecisionStatus::Queued);
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!("{:?} // Received {:?} from {:?} while in pre preparing",
                            self.node_id, message, header.from());

                        self.message_queue.queue_commit(StoredMessage::new(header, message));

                        return Ok(DecisionStatus::Queued);
                    }
                    ConsensusMessageKind::PrePrepare(_) => {
                        // Everything checks out, we can now process the message
                        received + 1
                    }
                };

                if received == 1 {
                    self.consensus_metrics.first_pre_prepare_recvd();
                }

                let pre_prepare_received_time = Utc::now();

                let stored_msg = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                let mut digests = request_batch_received(
                    &stored_msg,
                    timeouts,
                    synchronizer,
                    &mut self.message_log,
                );

                let batch_metadata = self.message_log.process_pre_prepare(stored_msg.clone(),
                                                                          stored_msg.header().digest().clone(),
                                                                          digests)?;
                let mut result = DecisionStatus::Deciding;

                self.phase = if received == view.leader_set().len() {
                    let batch_metadata = batch_metadata.unwrap();

                    info!("{:?} // Completed pre prepare phase with all pre prepares Seq {:?} with pre prepare from {:?}. Batch size {:?}",
                        node.id(), self.sequence_number(), header.from(), self.message_log.current_batch_size());

                    //We have received all pre prepare requests for this consensus instance
                    //We are now ready to broadcast our prepare message and move to the next phase
                    {
                        //Update batch meta
                        let mut meta_guard = self.message_log.batch_meta().lock().unwrap();

                        meta_guard.prepare_sent_time = Utc::now();
                        meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                    }

                    self.consensus_metrics.all_pre_prepares_recvd(self.message_log.current_batch_size());

                    let current_digest = batch_metadata.batch_digest();

                    // Register that all of the batches have been received
                    // The digest of the batch and the order of the batches
                    log.all_batches_received(batch_metadata);

                    self.accessory.handle_pre_prepare_phase_completed(&self.message_log,
                                                                      &view, stored_msg.clone(), node);

                    self.message_queue.signal();

                    // Mark that we have transitioned to the next phase
                    result = DecisionStatus::Transitioned;

                    // We no longer start the count at 1 since all leaders must also send the prepare
                    // message with the digest of the entire batch
                    DecisionPhase::Preparing(0)
                } else {
                    debug!("{:?} // Received pre prepare message {:?} from {:?}. Current received {:?}",
                        self.node_id, stored_msg.message(), stored_msg.header().from(), received);

                    self.accessory.handle_partial_pre_prepare(&self.message_log,
                                                              &view, stored_msg.clone(), node);

                    DecisionPhase::PrePreparing(received)
                };

                Ok(result)
            }
            DecisionPhase::Preparing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        // When we are in the preparing phase, we no longer accept any pre prepare message
                        warn!("{:?} // Dropped pre prepare message because we are in the preparing phase",
                            self.node_id);

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!("{:?} // Received {:?} from {:?} while in preparing phase",
                            self.node_id, message, header.from());

                        self.message_queue.queue_commit(StoredMessage::new(header, message));

                        return Ok(DecisionStatus::Queued);
                    }
                    ConsensusMessageKind::Prepare(_) if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        warn!("{:?} // Dropped prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), view.sequence_number());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Prepare(_) if message.sequence_number() != self.seq => {
                        // drop proposed value in a different view (from different leader)
                        warn!("{:?} // Dropped prepare message because of seq no {:?} vs {:?} (Ours)",
                            self.node_id, message.sequence_number(), self.seq);

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Prepare(d) if *d != self.message_log.current_digest().unwrap() => {
                        // drop msg with different digest from proposed value
                        warn!("{:?} // Dropped prepare message {:?} from {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), header.from(), d, self.message_log.current_digest());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        // Everything checks out, we can now process the message
                        received + 1
                    }
                };

                if received == 1 {
                    self.consensus_metrics.first_prepare_recvd();
                }

                let stored_msg = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                self.message_log.process_message(stored_msg.clone())?;

                let mut result = DecisionStatus::Deciding;

                self.phase = if received == view.params().quorum() {
                    info!("{:?} // Completed prepare phase with all prepares Seq {:?} with prepare from {:?}", node.id(), self.sequence_number(), header.from());

                    self.message_log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();
                    self.consensus_metrics.prepare_quorum_recvd();

                    let seq_no = self.sequence_number();
                    let current_digest = self.message_log.current_digest().unwrap();

                    self.accessory.handle_preparing_quorum(&self.message_log, &view,
                                                           stored_msg.clone(), node);

                    self.message_queue.signal();

                    result = DecisionStatus::Transitioned;

                    DecisionPhase::Committing(0)
                } else {
                    debug!("{:?} // Received prepare message {:?} from {:?}. Current count {}",
                        self.node_id, stored_msg.message().sequence_number(), header.from(), received);

                    self.accessory.handle_preparing_no_quorum(&self.message_log, &view,
                                                              stored_msg.clone(), node);

                    DecisionPhase::Preparing(received)
                };

                Ok(result)
            }
            DecisionPhase::Committing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::Commit(_) if message.sequence_number() != self.seq => {
                        // drop proposed value in a different view (from different leader)
                        warn!("{:?} // Dropped commit message because of seq no {:?} vs {:?} (Ours)",
                            self.node_id, message.sequence_number(), self.seq);

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Commit(_) if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        warn!("{:?} // Dropped commit message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), view.sequence_number());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Commit(d) if *d != self.message_log.current_digest().unwrap() => {
                        // drop msg with different digest from proposed value
                        warn!("{:?} // Dropped commit message {:?} from {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), header.from(), d, self.message_log.current_digest());

                        return Ok(DecisionStatus::Deciding);
                    }
                    ConsensusMessageKind::Commit(_) => {
                        received + 1
                    }
                    _ => {
                        // Any message relating to any other phase other than commit is not accepted

                        return Ok(DecisionStatus::Deciding);
                    }
                };

                if received == 1 {
                    self.consensus_metrics.first_commit_recvd();
                }

                let stored_msg = Arc::new(ReadOnly::new(
                    StoredMessage::new(header, message)));

                self.message_log.process_message(stored_msg.clone())?;

                return if received == view.params().quorum() {
                    info!("{:?} // Completed commit phase with all commits Seq {:?} with commit from {:?}", node.id(), self.sequence_number(),
                    header.from());


                    self.phase = DecisionPhase::Decided;

                    self.message_log.batch_meta().lock().unwrap().consensus_decision_time = Utc::now();

                    self.consensus_metrics.commit_quorum_recvd();

                    self.accessory.handle_committing_quorum(&self.message_log, &view,
                                                            stored_msg.clone(), node);

                    Ok(DecisionStatus::Decided)
                } else {
                    debug!("{:?} // Received commit message {:?} from {:?}. Current count {}",
                        self.node_id, stored_msg.message().sequence_number(), header.from(), received);

                    self.phase = DecisionPhase::Committing(received);

                    self.accessory.handle_committing_no_quorum(&self.message_log, &view,
                                                               stored_msg.clone(), node);

                    Ok(DecisionStatus::Deciding)
                };
            }
            DecisionPhase::Decided => {
                //Drop unneeded messages
                Ok(DecisionStatus::Decided)
            }
        };
    }

    /// Check if this consensus decision can be finalized
    pub fn is_finalizeable(&self) -> bool {
        if let DecisionPhase::Decided = &self.phase {
            true
        } else {
            false
        }
    }

    /// Finalize this consensus decision and return the information about the batch
    pub fn finalize(self) -> Result<CompletedBatch<D::Request>> {
        if let DecisionPhase::Decided = self.phase {
            self.message_log.finish_processing_batch()
                .ok_or(Error::simple_with_msg(ErrorKind::Consensus, "Failed to finalize batch"))
        } else {
            Err(Error::simple_with_msg(ErrorKind::Consensus, "Cannot finalize batch that is not decided"))
        }
    }

    pub fn deciding(&self, f: usize) -> IncompleteProof {
        self.message_log.deciding(f)
    }

    pub fn message_log(&self) -> &DecidingLog<D::Request> {
        &self.message_log
    }

    pub fn phase(&self) -> &DecisionPhase {
        &self.phase
    }
}

impl<D: SharedData + 'static, ST: StateTransferMessage + 'static> Orderable for ConsensusDecision<D, ST> {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

#[inline]
fn request_batch_received<D>(
    pre_prepare: &StoredMessage<ConsensusMessage<D::Request>>,
    timeouts: &Timeouts,
    synchronizer: &Synchronizer<D>,
    log: &DecidingLog<D::Request>,
) -> Vec<Digest>
    where
        D: SharedData + 'static
{
    let start = Instant::now();

    let mut batch_guard = log.batch_meta().lock().unwrap();

    batch_guard.batch_size += match pre_prepare.message().kind() {
        ConsensusMessageKind::PrePrepare(req) => {
            req.len()
        }
        _ => { panic!("Wrong message type provided") }
    };

    batch_guard.reception_time = Utc::now();

    // Notify the synchronizer that a batch has been received
    let digests = synchronizer.request_batch_received(pre_prepare, timeouts);

    metric_duration(PRE_PREPARE_ANALYSIS_ID, start.elapsed());

    digests
}

impl<O> Debug for DecisionPollStatus<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DecisionPollStatus::TryPropose => {
                write!(f, "Try Propose")
            }
            DecisionPollStatus::Recv => {
                write!(f, "Recv")
            }
            DecisionPollStatus::NextMessage(_, msg) => {
                write!(f, "Next Message {:?}", msg)
            }
            DecisionPollStatus::Decided => {
                write!(f, "Decided")
            }
        }
    }
}