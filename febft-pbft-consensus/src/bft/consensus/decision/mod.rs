use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use atlas_common::Err;
use chrono::Utc;
use thiserror::Error;
use tracing::{debug, info, instrument, warn};

use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::serialization_helper::SerMsg;
use atlas_communication::message::Header;
use atlas_core::messages::{ClientRqInfo, SessionBased};
use atlas_core::metric::RQ_BATCH_TRACKING_ID;
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_core::ordering_protocol::ShareableMessage;
use atlas_core::timeouts::timeout::TimeoutModHandle;
use atlas_metrics::metrics::{metric_correlation_id_passed, metric_duration};

use crate::bft::consensus::accessory::replica::ReplicaAccessory;
use crate::bft::consensus::accessory::{AccessoryConsensus, ConsensusDecisionAccessory};
use crate::bft::log::deciding::{CompletedBatch, WorkingDecisionLog};
use crate::bft::log::decisions::{IncompleteProof, ProofMetadata};
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::metric::{
    ConsensusMetrics, BATCH_COMMIT_DONE, BATCH_PREPARE_DONE, BATCH_PRE_PREPARE_DONE,
    PRE_PREPARE_ANALYSIS_ID,
};
use crate::bft::sync::view::ViewInfo;
use crate::bft::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::PBFT;

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        extract_msg!(ConsensusPollStatus::Recv, $g, $q)
    };
    ($rsp:expr, $g:expr, $q:expr) => {
        if let Some(stored) = $q.pop_front() {
            DecisionPollStatus::NextMessage(stored)
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
    NextMessage(ShareableMessage<PBFTMessage<O>>),
    // This consensus decision is finished and therefore can be finalized
    Decided,
}

#[derive(Debug, Clone)]
pub enum DecisionStatus<O> {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    // Returned when a node ignores a message
    MessageIgnored,
    /// The message has been queued for later execution
    /// As such this consensus decision should be signaled
    MessageQueued,
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding(ShareableMessage<PBFTMessage<O>>),
    /// Transitioned to another next phase of the consensus decision
    Transitioned(Option<ProofMetadata>, ShareableMessage<PBFTMessage<O>>),
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    /// THe second Vec<Digest> is a vec with digests of the requests contained in the batch
    /// The third is the messages that should be persisted for this batch to be considered persisted
    Decided(ShareableMessage<PBFTMessage<O>>),
    DecidedIgnored,
}

/// A message queue for this particular consensus instance
pub struct MessageQueue<O> {
    get_queue: bool,
    pre_prepares: VecDeque<ShareableMessage<PBFTMessage<O>>>,
    prepares: VecDeque<ShareableMessage<PBFTMessage<O>>>,
    commits: VecDeque<ShareableMessage<PBFTMessage<O>>>,
}

/// The information needed to make a decision on a batch of requests.
pub struct ConsensusDecision<RQ>
where
    RQ: SerMsg,
{
    node_id: NodeId,
    /// The sequence number of this consensus decision
    seq: SeqNo,
    /// The current phase of this decision
    phase: DecisionPhase,
    /// The queue of messages for this consensus instance
    message_queue: MessageQueue<RQ>,
    /// The working decision log
    working_log: WorkingDecisionLog<RQ>,
    /// Accessory to the base consensus state machine
    accessory: ConsensusDecisionAccessory<RQ>,
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

    pub(super) fn from_messages(
        pre_prepares: VecDeque<ShareableMessage<PBFTMessage<O>>>,
        prepares: VecDeque<ShareableMessage<PBFTMessage<O>>>,
        commits: VecDeque<ShareableMessage<PBFTMessage<O>>>,
    ) -> Self {
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

    pub fn is_signalled(&self) -> bool {
        self.get_queue
    }

    fn queue_pre_prepare(&mut self, message: ShareableMessage<PBFTMessage<O>>) {
        self.pre_prepares.push_back(message);

        self.signal();
    }

    fn queue_prepare(&mut self, message: ShareableMessage<PBFTMessage<O>>) {
        self.prepares.push_back(message);

        self.signal();
    }

    fn queue_commit(&mut self, message: ShareableMessage<PBFTMessage<O>>) {
        self.commits.push_back(message);

        self.signal();
    }
}

impl<RQ> ConsensusDecision<RQ>
where
    RQ: SerMsg + SessionBased + 'static,
{
    pub fn init_decision(node_id: NodeId, seq_no: SeqNo, view: &ViewInfo) -> Self {
        Self {
            node_id,
            seq: seq_no,
            phase: DecisionPhase::Initialize,
            message_queue: MessageQueue::new(),
            working_log: WorkingDecisionLog::new(node_id, seq_no, view),
            accessory: ConsensusDecisionAccessory::Replica(ReplicaAccessory::new()),
            consensus_metrics: ConsensusMetrics::new(),
        }
    }

    pub fn init_with_msg_log(
        node_id: NodeId,
        seq_no: SeqNo,
        view: &ViewInfo,
        message_queue: MessageQueue<RQ>,
    ) -> Self {
        Self {
            node_id,
            seq: seq_no,
            phase: DecisionPhase::Initialize,
            message_queue,
            working_log: WorkingDecisionLog::new(node_id, seq_no, view),
            accessory: ConsensusDecisionAccessory::Replica(ReplicaAccessory::new()),
            consensus_metrics: ConsensusMetrics::new(),
        }
    }

    pub fn queue(&mut self, message: ShareableMessage<PBFTMessage<RQ>>) {
        match message.message().consensus().kind() {
            ConsensusMessageKind::PrePrepare(_) => {
                self.message_queue.queue_pre_prepare(message);
            }
            ConsensusMessageKind::Prepare(_) => {
                self.message_queue.queue_prepare(message);
            }
            ConsensusMessageKind::Commit(_) => {
                self.message_queue.queue_commit(message);
            }
        }
    }

    #[instrument(skip(self), level = "debug")]
    pub fn poll(&mut self) -> DecisionPollStatus<RQ> {
        match self.phase {
            DecisionPhase::Initialize => {
                self.phase = DecisionPhase::PrePreparing(0);

                self.consensus_metrics.consensus_started();

                DecisionPollStatus::TryPropose
            }
            DecisionPhase::PrePreparing(_) if self.message_queue.get_queue => {
                extract_msg!(
                    DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.pre_prepares
                )
            }
            DecisionPhase::Preparing(_) if self.message_queue.get_queue => {
                extract_msg!(
                    DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.prepares
                )
            }
            DecisionPhase::Committing(_) if self.message_queue.get_queue => {
                extract_msg!(
                    DecisionPollStatus::Recv,
                    &mut self.message_queue.get_queue,
                    &mut self.message_queue.commits
                )
            }
            DecisionPhase::Decided => DecisionPollStatus::Decided,
            _ => DecisionPollStatus::Recv,
        }
    }

    /// Allows us to skip the initialization phase of this consensus instance
    /// This is useful when we don't want the proposer to receive authorization
    /// To propose into this consensus instance.
    pub fn skip_init_phase(&mut self) {
        self.phase = DecisionPhase::PrePreparing(0);
    }

    /// Update the current view of this consensus instance
    pub fn update_current_view(&mut self, view: &ViewInfo) {
        self.working_log.update_current_view(view);
    }

    /// Process a message relating to this consensus instance
    #[instrument(skip(self, synchronizer, timeouts, node), level = "debug")]
    pub fn process_message<NT>(
        &mut self,
        s_message: ShareableMessage<PBFTMessage<RQ>>,
        synchronizer: &Synchronizer<RQ>,
        timeouts: &TimeoutModHandle,
        node: &Arc<NT>,
    ) -> Result<DecisionStatus<RQ>>
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        let view = synchronizer.view();
        let header = s_message.header();
        let message = s_message.message().consensus();

        return match self.phase {
            DecisionPhase::Initialize => {
                // The initialize phase will only be skipped by polling
                // This consensus instance.
                warn!(
                    "{:?} // Queueing message {:?} as we are in the initialize phase",
                    self.node_id, message
                );

                self.queue(s_message);

                return Ok(DecisionStatus::MessageQueued);
            }
            DecisionPhase::PrePreparing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                        if message.view() != view.sequence_number() =>
                    {
                        // drop proposed value in a different view (from different leader)
                        debug!(
                            "{:?} // Dropped {:?} because of view {:?} vs {:?} (ours) header {:?}",
                            self.node_id,
                            message,
                            message.view(),
                            synchronizer.view().sequence_number(),
                            header
                        );

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::PrePrepare(_)
                        if !view.leader_set().contains(&header.from()) =>
                    {
                        // Drop proposed value since sender is not leader
                        debug!("{:?} // Dropped {:?} because the sender was not the leader {:?} vs {:?} (ours)",
                        self.node_id,message, header.from(), view.leader());

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::PrePrepare(_)
                        if message.sequence_number() != self.seq =>
                    {
                        //Drop proposed value since it is not for this consensus instance
                        warn!("{:?} // Dropped {:?} because the sequence number was not the same {:?} vs {:?} (ours)",
                            self.node_id, message, message.sequence_number(), self.seq);

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Prepare(_d) => {
                        debug!(
                            "{:?} // Received {:?} from {:?} while in prepreparing ",
                            self.node_id,
                            message,
                            header.from()
                        );

                        self.message_queue.queue_prepare(s_message);

                        return Ok(DecisionStatus::MessageQueued);
                    }
                    ConsensusMessageKind::Commit(_d) => {
                        debug!(
                            "{:?} // Received {:?} from {:?} while in pre preparing",
                            self.node_id,
                            message,
                            header.from()
                        );

                        self.message_queue.queue_commit(s_message);

                        return Ok(DecisionStatus::MessageQueued);
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

                //TODO: Try out cloning each request on this method,
                let digests = request_batch_received(
                    header,
                    message,
                    timeouts,
                    synchronizer,
                    &self.working_log,
                );

                let batch_metadata = self.working_log.process_pre_prepare(
                    s_message.clone(),
                    *header.digest(),
                    digests,
                )?;

                let result;

                self.phase = if received == view.leader_set().len() {
                    let batch_metadata = batch_metadata.unwrap();

                    info!("{:?} // Completed pre prepare phase with all pre prepares Seq {:?} with pre prepare from {:?}. Batch size {:?}",
                        node.id(), self.sequence_number(), header.from(), self.working_log.current_batch_size());

                    //We have received all pre prepare requests for this consensus instance
                    //We are now ready to broadcast our prepare message and move to the next phase
                    {
                        //Update batch meta
                        let mut meta_guard = self.working_log.batch_meta().lock().unwrap();

                        meta_guard.prepare_sent_time = Utc::now();
                        meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                    }

                    self.consensus_metrics
                        .all_pre_prepares_recvd(self.working_log.current_batch_size());

                    let _current_digest = batch_metadata.batch_digest();

                    self.accessory.handle_pre_prepare_phase_completed(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        node,
                    );

                    self.message_queue.signal();

                    metric_correlation_id_passed(
                        RQ_BATCH_TRACKING_ID,
                        self.sequence_number().into_u32().to_string(),
                        BATCH_PRE_PREPARE_DONE.clone(),
                    );

                    // Mark that we have transitioned to the next phase
                    result = DecisionStatus::Transitioned(Some(batch_metadata), s_message);

                    // We no longer start the count at 1 since all leaders must also send the prepare
                    // message with the digest of the entire batch
                    DecisionPhase::Preparing(0)
                } else {
                    debug!("{:?} // Received pre prepare message {:?} from {:?}. Current received {:?}",
                        self.node_id, s_message.message(), s_message.header().from(), received);

                    self.accessory.handle_partial_pre_prepare(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        &**node,
                    );

                    result = DecisionStatus::Deciding(s_message);

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

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Commit(_d) => {
                        debug!(
                            "{:?} // Received {:?} from {:?} while in preparing phase",
                            self.node_id,
                            message,
                            header.from()
                        );

                        self.message_queue.queue_commit(s_message);

                        return Ok(DecisionStatus::MessageQueued);
                    }
                    ConsensusMessageKind::Prepare(_)
                        if message.view() != view.sequence_number() =>
                    {
                        // drop proposed value in a different view (from different leader)
                        warn!(
                            "{:?} // Dropped prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id,
                            message.view(),
                            view.sequence_number()
                        );

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Prepare(_) if message.sequence_number() != self.seq => {
                        // drop proposed value in a different view (from different leader)
                        warn!(
                            "{:?} // Dropped prepare message because of seq no {:?} vs {:?} (Ours)",
                            self.node_id,
                            message.sequence_number(),
                            self.seq
                        );

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Prepare(d)
                        if *d != self.working_log.current_digest().unwrap() =>
                    {
                        // drop msg with different digest from proposed value
                        warn!("{:?} // Dropped prepare message {:?} from {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), header.from(), d, self.working_log.current_digest());

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        // Everything checks out, we can now process the message
                        received + 1
                    }
                };

                if received == 1 {
                    self.consensus_metrics.first_prepare_recvd();
                }

                self.working_log.process_message(s_message.clone())?;

                let result;

                self.phase = if received == view.params().quorum() {
                    info!("{:?} // Completed prepare phase with all prepares Seq {:?} with prepare from {:?}", node.id(), self.sequence_number(), header.from());

                    self.working_log
                        .batch_meta()
                        .lock()
                        .unwrap()
                        .commit_sent_time = Utc::now();
                    self.consensus_metrics.prepare_quorum_recvd();

                    let _seq_no = self.sequence_number();
                    let _current_digest = self.working_log.current_digest().unwrap();

                    self.accessory.handle_preparing_quorum(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        &**node,
                    );

                    self.message_queue.signal();

                    metric_correlation_id_passed(
                        RQ_BATCH_TRACKING_ID,
                        self.sequence_number().into_u32().to_string(),
                        BATCH_PREPARE_DONE.clone(),
                    );

                    result = DecisionStatus::Transitioned(None, s_message);

                    DecisionPhase::Committing(0)
                } else {
                    debug!(
                        "{:?} // Received prepare message {:?} from {:?}. Current count {}",
                        self.node_id,
                        s_message.message().sequence_number(),
                        header.from(),
                        received
                    );

                    self.accessory.handle_preparing_no_quorum(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        &**node,
                    );

                    result = DecisionStatus::Deciding(s_message);

                    DecisionPhase::Preparing(received)
                };

                Ok(result)
            }
            DecisionPhase::Committing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::Commit(_) if message.sequence_number() != self.seq => {
                        // drop proposed value in a different view (from different leader)
                        warn!(
                            "{:?} // Dropped commit message because of seq no {:?} vs {:?} (Ours)",
                            self.node_id,
                            message.sequence_number(),
                            self.seq
                        );

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Commit(_) if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        warn!(
                            "{:?} // Dropped commit message because of view {:?} vs {:?} (ours)",
                            self.node_id,
                            message.view(),
                            view.sequence_number()
                        );

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Commit(d)
                        if *d != self.working_log.current_digest().unwrap() =>
                    {
                        // drop msg with different digest from proposed value
                        warn!("{:?} // Dropped commit message {:?} from {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), header.from(), d, self.working_log.current_digest());

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                    ConsensusMessageKind::Commit(_) => received + 1,
                    _ => {
                        // Any message relating to any other phase other than commit is not accepted

                        return Ok(DecisionStatus::MessageIgnored);
                    }
                };

                if received == 1 {
                    self.consensus_metrics.first_commit_recvd();
                }

                self.working_log.process_message(s_message.clone())?;

                return if received == view.params().quorum() {
                    info!("{:?} // Completed commit phase with all commits Seq {:?} with commit from {:?}", node.id(), self.sequence_number(),
                    header.from());

                    self.phase = DecisionPhase::Decided;

                    self.working_log
                        .batch_meta()
                        .lock()
                        .unwrap()
                        .consensus_decision_time = Utc::now();

                    self.consensus_metrics.commit_quorum_recvd();

                    self.accessory.handle_committing_quorum(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        &**node,
                    );

                    metric_correlation_id_passed(
                        RQ_BATCH_TRACKING_ID,
                        self.sequence_number().into_u32().to_string(),
                        BATCH_COMMIT_DONE.clone(),
                    );

                    Ok(DecisionStatus::Decided(s_message))
                } else {
                    debug!(
                        "{:?} // Received commit message {:?} from {:?}. Current count {}",
                        self.node_id,
                        s_message.message().sequence_number(),
                        header.from(),
                        received
                    );

                    self.phase = DecisionPhase::Committing(received);

                    self.accessory.handle_committing_no_quorum(
                        &self.working_log,
                        &view,
                        header,
                        message,
                        &**node,
                    );

                    Ok(DecisionStatus::Deciding(s_message))
                };
            }
            DecisionPhase::Decided => {
                //Drop unneeded messages
                Ok(DecisionStatus::DecidedIgnored)
            }
        };
    }

    /// Check if this consensus decision can be finalized
    pub fn is_finalizeable(&self) -> bool {
        matches!(self.phase, DecisionPhase::Decided)
    }

    /// Finalize this consensus decision and return the information about the batch
    #[instrument(skip(self), level = "debug")]
    pub fn finalize(self) -> Result<CompletedBatch<RQ>> {
        if let DecisionPhase::Decided = self.phase {
            let seq = self.sequence_number();

            self.working_log
                .finish_processing_batch()
                .ok_or(anyhow::Error::from(DecisionError::FailedToFinalizeBatch(
                    seq,
                )))
        } else {
            Err!(DecisionError::CannotFinalizeUndecidedBatch(
                self.sequence_number()
            ))
        }
    }

    pub fn deciding(&self, f: usize) -> IncompleteProof {
        self.working_log.deciding(f)
    }

    pub fn phase(&self) -> &DecisionPhase {
        &self.phase
    }
}

impl<RQ> Orderable for ConsensusDecision<RQ>
where
    RQ: SerMsg,
{
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

#[inline]
fn request_batch_received<RQ>(
    header: &Header,
    pre_prepare: &ConsensusMessage<RQ>,
    timeouts: &TimeoutModHandle,
    synchronizer: &Synchronizer<RQ>,
    log: &WorkingDecisionLog<RQ>,
) -> Vec<ClientRqInfo>
where
    RQ: SerMsg + SessionBased + 'static,
{
    let start = Instant::now();

    let mut batch_guard = log.batch_meta().lock().unwrap();

    batch_guard.batch_size += match pre_prepare.kind() {
        ConsensusMessageKind::PrePrepare(req) => req.len(),
        _ => {
            panic!("Wrong message type provided")
        }
    };

    batch_guard.reception_time = Utc::now();

    // Notify the synchronizer that a batch has been received
    let digests = synchronizer.request_batch_received(header, pre_prepare, timeouts);

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
            DecisionPollStatus::NextMessage(message) => {
                write!(
                    f,
                    "Next Message {:?}, Message Type {:?}",
                    message.header(),
                    message.message()
                )
            }
            DecisionPollStatus::Decided => {
                write!(f, "Decided")
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum DecisionError {
    #[error("Unable to finalize an undecided batch {0:?}")]
    CannotFinalizeUndecidedBatch(SeqNo),
    #[error("Failed to finalize a batch {0:?}")]
    FailedToFinalizeBatch(SeqNo),
}
