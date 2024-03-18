//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use ::log::{debug, error, info, trace, warn};
use anyhow::anyhow;
use either::Either;

use crate::bft::config::PBFTConfig;
use crate::bft::consensus::{
    Consensus, ConsensusPollStatus, ConsensusStatus, ProposerConsensusGuard,
};
use crate::bft::log::decided::DecisionLog;
use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::log::{initialize_decided_log, Log};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::message::{ConsensusMessageKind, ObserveEventKind, PBFTMessage};
use crate::bft::proposer::Proposer;
use crate::bft::sync::view::ViewInfo;
use crate::bft::sync::{
    AbstractSynchronizer, SyncReconfigurationResult, Synchronizer, SynchronizerPollStatus,
    SynchronizerStatus,
};
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::serialization_helper::SerType;
use atlas_communication::message::StoredMessage;
use atlas_core::messages::SessionBased;
use atlas_core::ordering_protocol::loggable::{
    LoggableOrderProtocol, OrderProtocolPersistenceHelper, PProof,
};
use atlas_core::ordering_protocol::networking::serialize::{NetworkView, OrderingProtocolMessage};
use atlas_core::ordering_protocol::networking::{
    NetworkedOrderProtocolInitializer, OrderProtocolSendNode,
};
use atlas_core::ordering_protocol::reconfigurable_order_protocol::{
    ReconfigurableOrderProtocol, ReconfigurationAttemptResult,
};
use atlas_core::ordering_protocol::{
    Decision, DecisionInfo, DecisionMetadata, DecisionsAhead, JoinInfo, OPExecResult, OPPollResult,
    OrderProtocolTolerance, OrderingProtocol, OrderingProtocolArgs, PermissionedOrderingProtocol,
    ProtocolConsensusDecision, ShareableConsensusMessage, ShareableMessage,
};
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_core::request_pre_processing::RequestPreProcessor;
use atlas_core::serialize::ReconfigurationProtocolMessage;
use atlas_core::timeouts::{RqTimeout, Timeouts};

pub mod config;
pub mod consensus;
pub mod log;
pub mod message;
pub mod metric;
pub mod observer;
pub mod proposer;
pub mod sync;

// The types responsible for this protocol
pub type PBFT<RQ> = PBFTConsensus<RQ>;
// The message type for this consensus protocol
pub type SysMsg<RQ> = <PBFTConsensus<RQ> as OrderingProtocolMessage<RQ>>::ProtocolMessage;

#[derive(Clone, PartialEq, Eq, Debug)]
/// Which phase of the consensus algorithm are we currently executing
pub enum ConsensusPhase {
    // The normal phase of the consensus
    NormalPhase,
    // The synchronizer phase of the consensus
    SyncPhase,
}

/// The result of advancing the sync phase
#[derive(Debug)]
pub enum SyncPhaseRes<O> {
    SyncProtocolNotNeeded,
    RunSyncProtocol,
    SyncProtocolFinished(ConsensusStatus<O>, Option<OPDecision<O>>),
    JoinedQuorum(ConsensusStatus<O>, Option<OPDecision<O>>, NodeId),
    RunCSTProtocol,
}

pub type OPDecision<O> = Decision<ProofMetadata, PBFTMessage<O>, O>;
pub type OPDecisionInfo<O> = DecisionInfo<ProofMetadata, PBFTMessage<O>, O>;

/// a PBFT based ordering protocol
pub struct PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    // What phase of the consensus algorithm are we currently executing
    phase: ConsensusPhase,
    /// The consensus state machine
    consensus: Consensus<RQ>,
    /// The synchronizer state machine
    synchronizer: Arc<Synchronizer<RQ>>,
    /// The request pre processor
    pre_processor: RequestPreProcessor<RQ>,
    // A reference to the timeouts layer
    timeouts: Timeouts,
    //The proposer guard
    consensus_guard: Arc<ProposerConsensusGuard>,
    // Check if unordered requests can be proposed.
    // This can only occur when we are in the normal phase of the state machine
    unordered_rq_guard: Arc<AtomicBool>,
    // The log of the decided consensus messages
    // This is completely owned by the server thread and therefore does not
    // Require any synchronization
    message_log: Log<RQ>,
    // The proposer of this replica
    proposer: Arc<Proposer<RQ, NT>>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
}

impl<RQ, NT> Orderable for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType,
        NT: 'static + OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    fn sequence_number(&self) -> SeqNo {
        self.consensus.sequence_number()
    }
}

impl<RQ, NT> OrderProtocolTolerance for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType,
        NT: 'static + OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    fn get_n_for_f(f: usize) -> usize {
        3 * f + 1
    }

    fn get_quorum_for_n(n: usize) -> usize {
        //n = 2f+1

        2 * Self::get_f_for_n(n) + 1
    }

    fn get_f_for_n(n: usize) -> usize {
        (n - 1) / 3
    }
}

impl<RQ, NT> NetworkedOrderProtocolInitializer<RQ, NT> for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    fn initialize(
        config: Self::Config,
        ordering_protocol_args: OrderingProtocolArgs<RQ, NT>,
    ) -> Result<Self>
        where
            Self: Sized,
    {
        Self::initialize_protocol(config, ordering_protocol_args, None)
    }
}

impl<RQ, NT> OrderingProtocol<RQ> for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    type Serialization = PBFTConsensus<RQ>;
    type Config = PBFTConfig;

    fn handle_off_ctx_message(&mut self, message: ShareableMessage<PBFTMessage<RQ>>) {
        match message.message() {
            PBFTMessage::Consensus(consensus) => {
                debug!(
                    "{:?} // Received off context consensus message {:?}",
                    self.node.id(),
                    consensus
                );
                self.consensus.queue(message);
            }
            PBFTMessage::ViewChange(view_change) => {
                debug!(
                    "{:?} // Received off context view change message {:?}",
                    self.node.id(),
                    view_change
                );
                self.synchronizer.queue(message);

                self.synchronizer.signal();
            }
            _ => {
                todo!()
            }
        }
    }

    fn handle_execution_changed(&mut self, is_executing: bool) -> Result<()> {
        if !is_executing {
            self.consensus_guard.lock_consensus();
        } else {
            match self.phase {
                ConsensusPhase::NormalPhase => {
                    self.consensus_guard.unlock_consensus();
                }
                ConsensusPhase::SyncPhase => {}
            }
        }

        info!(
            "Execution has changed to {} with our current phase being {:?}",
            is_executing, self.phase
        );

        Ok(())
    }

    fn poll(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        trace!("{:?} // Polling {:?}", self.node.id(), self.phase);

        match self.phase {
            ConsensusPhase::NormalPhase => self.poll_normal_phase(),
            ConsensusPhase::SyncPhase => self.poll_sync_phase(),
        }
    }

    fn process_message(
        &mut self,
        message: ShareableMessage<PBFTMessage<RQ>>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        match self.phase {
            ConsensusPhase::NormalPhase => self.update_normal_phase(message),
            ConsensusPhase::SyncPhase => self.update_sync_phase(message),
        }
    }

    fn install_seq_no(&mut self, seq_no: SeqNo) -> Result<()> {
        self.consensus
            .install_sequence_number(seq_no, &self.synchronizer.view());

        Ok(())
    }

    fn handle_timeout(
        &mut self,
        timeout: Vec<RqTimeout>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        if self.consensus.is_catching_up() {
            warn!(
                "{:?} // Ignoring timeouts while catching up",
                self.node.id()
            );

            return Ok(OPExecResult::MessageDropped);
        }

        let status = self
            .synchronizer
            .client_requests_timed_out(self.node.id(), &timeout);

        if let SynchronizerStatus::RequestsTimedOut { forwarded, stopped } = status {
            if !forwarded.is_empty() {
                let requests = self.pre_processor.clone_pending_rqs(forwarded);

                self.synchronizer.forward_requests(requests, &*self.node);
            }

            if !stopped.is_empty() {
                let stopped = self.pre_processor.clone_pending_rqs(stopped);

                self.switch_phase(ConsensusPhase::SyncPhase);

                self.synchronizer.begin_view_change(
                    Some(stopped),
                    &*self.node,
                    &self.timeouts,
                    &self.message_log,
                );
            }
        };

        Ok(OPExecResult::MessageProcessedNoUpdate)
    }
}

impl<RQ, NT> PermissionedOrderingProtocol for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    type PermissionedSerialization = PBFTConsensus<RQ>;

    fn view(&self) -> ViewInfo {
        self.synchronizer.view()
    }

    fn install_view(&mut self, view: ViewInfo) {
        let current_view = self.view();

        match view.sequence_number().index(current_view.sequence_number()) {
            Either::Left(_) | Either::Right(0) => {
                warn!("Attempted to install view that is the same or older than the current view that is in place? New: {:?} vs {:?}", view, current_view);
            }
            Either::Right(_) => {
                self.consensus.install_view(&view);
                if self.synchronizer.received_view_from_state_transfer(view) {
                    info!("Installed the view and synchronizer now requires execution in order to make sure everything is correctly setup.");

                    self.switch_phase(ConsensusPhase::SyncPhase);
                }
            }
        }
    }
}

impl<RQ, NT> PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    fn initialize_protocol(
        config: PBFTConfig,
        args: OrderingProtocolArgs<RQ, NT>,
        _initial_state: Option<DecisionLog<RQ>>,
    ) -> Result<Self> {
        let PBFTConfig {
            timeout_dur,
            proposer_config,
            watermark,
        } = config;

        let OrderingProtocolArgs(node_id, timeouts, pre_processor, batch_input, node, quorum) =
            args;

        let sync = Synchronizer::initialize_with_quorum(
            node_id,
            SeqNo::ZERO,
            quorum.clone(),
            timeout_dur,
        )?;

        let consensus_guard = ProposerConsensusGuard::new(sync.view(), watermark);

        let consensus = Consensus::<RQ>::new_replica(
            node_id,
            &sync.view(),
            SeqNo::ZERO,
            watermark,
            consensus_guard.clone(),
            timeouts.clone(),
        );

        let dec_log = initialize_decided_log::<RQ>(node_id);

        let proposer = Proposer::<RQ, NT>::new(
            node.clone(),
            batch_input,
            sync.clone(),
            timeouts.clone(),
            consensus_guard.clone(),
            proposer_config,
        );

        let replica = Self {
            phase: ConsensusPhase::NormalPhase,
            consensus,
            synchronizer: sync,
            pre_processor,
            timeouts,
            consensus_guard,
            unordered_rq_guard: Arc::new(Default::default()),
            message_log: dec_log,
            proposer,
            node,
        };

        let crr_view = replica.synchronizer.view();

        info!(
            "{:?} // Initialized ordering protocol with view: {:?} and sequence number: {:?}",
            replica.node.id(),
            crr_view.sequence_number(),
            replica.sequence_number()
        );

        info!(
            "{:?} // Leader count: {}, Leader set: {:?}",
            replica.node.id(),
            crr_view.leader_set().len(),
            crr_view.leader_set()
        );

        info!("{:?} // Watermark: {}", replica.node.id(), watermark);

        println!(
            "{:?} // Leader count: {}, Leader set: {:?}, Quorum: {:?}",
            replica.node.id(),
            crr_view.leader_set().len(),
            crr_view.leader_set(),
            quorum
        );
        println!("{:?} // Watermark: {}", replica.node.id(), watermark);

        replica.proposer.clone().start();

        Ok(replica)
    }

    fn poll_sync_phase(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        // retrieve a view change message to be processed
        let poll_result = self.synchronizer.poll();

        debug!(
            "{:?} // Polling sync phase {:?}",
            self.node.id(),
            poll_result
        );

        match poll_result {
            SynchronizerPollStatus::Recv => Ok(OPPollResult::ReceiveMsg),
            SynchronizerPollStatus::NextMessage(message) => Ok(OPPollResult::Exec(message)),
            SynchronizerPollStatus::ResumeViewChange => {
                debug!("{:?} // Resuming view change", self.node.id());

                let sync_status = self.synchronizer.resume_view_change(
                    &mut self.message_log,
                    &self.timeouts,
                    &mut self.consensus,
                    &self.node,
                );

                self.switch_phase(ConsensusPhase::NormalPhase);

                if let Some(sync_status) = sync_status {
                    match sync_status {
                        SynchronizerStatus::NewViewJoinedQuorum(
                            consensus_status,
                            decisions,
                            node,
                        ) => {
                            let _quorum_members = self.synchronizer.view().quorum_members().clone();

                            let decisions = self.handle_sync_result(consensus_status, decisions)?;

                            let joined = JoinInfo::new(
                                node,
                                self.synchronizer.view().quorum_members().clone(),
                            );

                            if decisions.is_empty() {
                                Ok(OPPollResult::QuorumJoined(
                                    DecisionsAhead::ClearAhead,
                                    None,
                                    joined,
                                ))
                            } else {
                                Ok(OPPollResult::QuorumJoined(
                                    DecisionsAhead::ClearAhead,
                                    Some(decisions),
                                    joined,
                                ))
                            }
                        }
                        SynchronizerStatus::NewView(consensus_status, decisions) => {
                            let decisions = self.handle_sync_result(consensus_status, decisions)?;

                            Ok(OPPollResult::ProgressedDecision(
                                DecisionsAhead::ClearAhead,
                                decisions,
                            ))
                        }
                        _ => {
                            warn!("Received sync status that is not handled");

                            Ok(OPPollResult::RePoll)
                        }
                    }
                } else {
                    Ok(OPPollResult::RePoll)
                }
            }
        }
    }

    fn poll_normal_phase(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        // Consume them until we reached a point where no new messages can be processed
        while self.synchronizer.can_process_stops() {
            let sync_protocol = self.poll_sync_phase()?;

            if let OPPollResult::Exec(s_message) = sync_protocol {
                let (_header, message) = (s_message.header(), s_message.message());

                if let PBFTMessage::ViewChange(_view_change) = message {
                    let result = self.adv_sync(s_message);

                    return Ok(match result {
                        SyncPhaseRes::RunSyncProtocol => {
                            self.switch_phase(ConsensusPhase::SyncPhase);

                            OPPollResult::RePoll
                        }
                        SyncPhaseRes::RunCSTProtocol => {
                            // We don't need to switch to the sync phase
                            // As that has already been done by the adv sync method
                            OPPollResult::RunCst
                        }
                        SyncPhaseRes::SyncProtocolNotNeeded => {
                            error!("Polling the sync phase should never return anything other than a run sync protocol or run cst protocol message, SyncProtocolNotNeeded");
                            OPPollResult::RePoll
                        }
                        SyncPhaseRes::JoinedQuorum(_, _, _) => {
                            error!("Polling the sync phase should never return anything other than a run sync protocol or run cst protocol message, JoinedQuorum");
                            OPPollResult::RePoll
                        }
                        SyncPhaseRes::SyncProtocolFinished(_, _) => {
                            error!("Polling the sync phase should never return anything other than a run sync protocol or run cst protocol message, Protocol Finished");
                            OPPollResult::RePoll
                        }
                    });
                } else {
                    // The synchronizer should never return anything other than a view
                    // change message
                    unreachable!(
                        "Synchronizer returned a message other than a view change message"
                    );
                }
            }
        }

        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let polled_message = self.consensus.poll();

        Ok(match polled_message {
            ConsensusPollStatus::Recv => OPPollResult::ReceiveMsg,
            ConsensusPollStatus::NextMessage(message) => OPPollResult::Exec(message),
            ConsensusPollStatus::Decided(decisions) => OPPollResult::ProgressedDecision(
                DecisionsAhead::Ignore,
                self.handle_decided(decisions)?,
            ),
        })
    }

    fn update_sync_phase(
        &mut self,
        message: ShareableMessage<PBFTMessage<RQ>>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        match message.message() {
            PBFTMessage::ViewChange(_view_change) => {
                return Ok(match self.adv_sync(message) {
                    SyncPhaseRes::SyncProtocolNotNeeded => OPExecResult::MessageProcessedNoUpdate,
                    SyncPhaseRes::RunSyncProtocol => OPExecResult::MessageProcessedNoUpdate,
                    SyncPhaseRes::SyncProtocolFinished(status, to_execute) => {
                        OPExecResult::ProgressedDecision(
                            DecisionsAhead::ClearAhead,
                            self.handle_sync_result(status, to_execute)?,
                        )
                    }
                    SyncPhaseRes::JoinedQuorum(status, to_execute, node) => {
                        info!(
                            "Replica {:?} joined the quorum, with a decision to execute? {}",
                            node,
                            to_execute.is_some()
                        );

                        let new_quorum = self.synchronizer.view().quorum_members().clone();

                        let join_info = JoinInfo::new(node, new_quorum);

                        let decision_adv = self.handle_sync_result(status, to_execute)?;

                        if decision_adv.is_empty() {
                            OPExecResult::QuorumJoined(DecisionsAhead::ClearAhead, None, join_info)
                        } else {
                            OPExecResult::QuorumJoined(
                                DecisionsAhead::ClearAhead,
                                Some(decision_adv),
                                join_info,
                            )
                        }
                    }
                    SyncPhaseRes::RunCSTProtocol => OPExecResult::RunCst,
                });
            }
            PBFTMessage::Consensus(_) => {
                self.consensus.queue(message);
            }
            _ => {}
        }

        Ok(OPExecResult::MessageProcessedNoUpdate)
    }

    fn handle_decided(
        &mut self,
        decisions: MaybeVec<Decision<ProofMetadata, PBFTMessage<RQ>, RQ>>,
    ) -> Result<MaybeVec<OPDecision<RQ>>> {
        let finalized_decisions = self.finalize_all_possible()?;

        let decisions = self.merge_decisions(decisions, finalized_decisions)?;

        Ok(decisions)
    }

    fn update_normal_phase(
        &mut self,
        message: ShareableMessage<PBFTMessage<RQ>>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        match message.message() {
            PBFTMessage::Consensus(_) => {
                return self.adv_consensus(message);
            }
            PBFTMessage::ViewChange(_) => {
                let status = self.synchronizer.process_message(
                    message,
                    &self.timeouts,
                    &mut self.message_log,
                    &self.pre_processor,
                    &mut self.consensus,
                    &self.node,
                );

                self.synchronizer.signal();

                match status {
                    SynchronizerStatus::Nil => (),
                    SynchronizerStatus::Running => self.switch_phase(ConsensusPhase::SyncPhase),
                    // should not happen...
                    _ => {
                        unreachable!()
                    }
                }
            }
            _ => {}
        }

        Ok(OPExecResult::MessageProcessedNoUpdate)
    }

    /// Advance the consensus phase with a received message
    fn adv_consensus(
        &mut self,
        message: ShareableMessage<PBFTMessage<RQ>>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<RQ>, RQ>> {
        let _seq = self.consensus.sequence_number();

        // debug!(
        //     "{:?} // Processing consensus message {:?} ",
        //     self.id(),
        //     message
        // );

        let status = self.consensus.process_message(
            message,
            &self.synchronizer,
            &self.timeouts,
            &self.node,
        )?;

        Ok(match status {
            ConsensusStatus::VotedTwice(_) | ConsensusStatus::MessageIgnored => {
                OPExecResult::MessageDropped
            }
            ConsensusStatus::MessageQueued => OPExecResult::MessageQueued,
            ConsensusStatus::Deciding(result) => {
                OPExecResult::ProgressedDecision(DecisionsAhead::Ignore, result)
            }
            ConsensusStatus::Decided(result) => OPExecResult::ProgressedDecision(
                DecisionsAhead::Ignore,
                self.handle_decided(result)?,
            ),
        })
    }

    /// Finalize all possible consensus instances
    fn finalize_all_possible(&mut self) -> Result<Vec<ProtocolConsensusDecision<RQ>>> {
        let view = self.synchronizer.view();

        let mut finalized_decisions = Vec::with_capacity(self.consensus.finalizeable_count());

        while self.consensus.can_finalize() {
            // This will automatically move the consensus machine to the next consensus instance
            let completed_batch = self.consensus.finalize(&view)?.unwrap();

            //Should the execution be scheduled here or will it be scheduled by the persistent log?
            let exec_info = self.message_log.finalize_batch(completed_batch)?;

            finalized_decisions.push(exec_info);
        }

        Ok(finalized_decisions)
    }

    /// Advance the sync phase of the algorithm
    fn adv_sync(&mut self, message: ShareableMessage<PBFTMessage<RQ>>) -> SyncPhaseRes<RQ> {
        let status = self.synchronizer.process_message(
            message,
            &self.timeouts,
            &mut self.message_log,
            &self.pre_processor,
            &mut self.consensus,
            &self.node,
        );

        self.synchronizer.signal();

        match status {
            SynchronizerStatus::Nil => SyncPhaseRes::SyncProtocolNotNeeded,
            SynchronizerStatus::Running => SyncPhaseRes::RunSyncProtocol,
            SynchronizerStatus::NewView(consensus_status, to_execute) => {
                //Our current view has been updated and we have no more state operations
                //to perform. This happens if we are a correct replica and therefore do not need
                //To update our state or if we are a replica that was incorrect and whose state has
                //Already been updated from the Cst protocol
                self.switch_phase(ConsensusPhase::NormalPhase);

                SyncPhaseRes::SyncProtocolFinished(consensus_status, to_execute)
            }
            SynchronizerStatus::NewViewJoinedQuorum(consensus_decision, decision, node) => {
                //We have joined a quorum and we have a new view to execute
                //We need to switch to the normal phase and execute the new view
                self.switch_phase(ConsensusPhase::NormalPhase);

                SyncPhaseRes::JoinedQuorum(consensus_decision, decision, node)
            }
            SynchronizerStatus::RunCst => {
                //This happens when a new view is being introduced and we are not up to date
                //With the rest of the replicas. This might happen because the replica was faulty
                //or any other reason that might cause it to lose some updates from the other replicas

                //After we update the state, we go back to the sync phase (this phase) so we can check if we are missing
                //Anything or to finalize and go back to the normal phase
                info!("Running CST protocol as requested by the synchronizer");

                self.switch_phase(ConsensusPhase::SyncPhase);

                SyncPhaseRes::RunCSTProtocol
            }
            // should not happen...
            _ => {
                unreachable!()
            }
        }
    }

    fn merge_decisions(
        &mut self,
        status: MaybeVec<OPDecision<RQ>>,
        finalized_decisions: Vec<ProtocolConsensusDecision<RQ>>,
    ) -> Result<MaybeVec<OPDecision<RQ>>> {
        let mut map = BTreeMap::new();

        Self::merge_decision_vec(&mut map, status)?;

        for decision in finalized_decisions {
            if let Some(member) = map.get_mut(&decision.sequence_number()) {
                member.append_decision_info(DecisionInfo::DecisionDone(decision));
            } else {
                map.insert(
                    decision.sequence_number(),
                    Decision::completed_decision(decision.sequence_number(), decision),
                );
            }
        }

        let mut decisions = MaybeVec::builder();

        // By turning this btree map into a vec, we maintain ordering on the delivery (Shouldn't
        // really be necessary but always nice to have)
        map.into_iter().for_each(|(_seq, decision)| {
            decisions.push(decision);
        });

        Ok(decisions.build())
    }

    /// Handles the result of a synchronizer result
    fn handle_sync_result(
        &mut self,
        status: ConsensusStatus<RQ>,
        to_exec: Option<OPDecision<RQ>>,
    ) -> Result<MaybeVec<OPDecision<RQ>>> {
        let mut map = BTreeMap::new();

        match status {
            ConsensusStatus::Deciding(decision) => {
                Self::merge_decision_vec(&mut map, decision)?;
            }
            ConsensusStatus::Decided(decision) => {
                Self::merge_decision_vec(&mut map, decision)?;

                let finalized = self.finalize_all_possible()?;

                for decision in finalized {
                    if let Some(member) = map.get_mut(&decision.sequence_number()) {
                        member.append_decision_info(DecisionInfo::DecisionDone(decision));
                    } else {
                        map.insert(
                            decision.sequence_number(),
                            Decision::completed_decision(decision.sequence_number(), decision),
                        );
                    }
                }
            }
            _ => {}
        }

        if let Some(decision) = to_exec {
            map.insert(decision.sequence_number(), decision);
        }

        let mut decisions = MaybeVec::builder();

        // By turning this btree map into a vec, we maintain ordering on the delivery (Shouldn't
        // really be necessary but always nice to have)
        map.into_iter().for_each(|(_seq, decision)| {
            decisions.push(decision);
        });

        Ok(decisions.build())
    }

    /// Merge a decision vector with the already existing btreemap
    fn merge_decision_vec(
        map: &mut BTreeMap<SeqNo, OPDecision<RQ>>,
        decision: MaybeVec<OPDecision<RQ>>,
    ) -> Result<()> {
        for dec in decision.into_iter() {
            if let Some(member) = map.get_mut(&dec.sequence_number()) {
                member.merge_decisions(dec)?;
            } else {
                map.insert(dec.sequence_number(), dec);
            }
        }

        Ok(())
    }
}

impl<RQ, NT> PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    pub(crate) fn switch_phase(&mut self, new_phase: ConsensusPhase) {
        info!(
            "{:?} // Switching from phase {:?} to phase {:?}",
            self.node.id(),
            self.phase,
            new_phase
        );

        let old_phase = self.phase.clone();

        self.phase = new_phase;

        //TODO: Handle locking the consensus to the proposer thread
        //When we change to another phase to prevent the proposer thread from
        //Constantly proposing new batches. This is also "fixed" by the fact that the proposer
        //thread never proposes two batches to the same sequence id (this might have to be changed
        //however if it's possible to have various proposals for the same seq number in case of leader
        //Failure or something like that. I think that's impossible though so lets keep it as is)

        if self.phase != old_phase {
            //If the phase is the same, then we got nothing to do as no states have changed

            match (&old_phase, &self.phase) {
                (ConsensusPhase::NormalPhase, _) => {
                    //We want to stop the proposer from trying to propose any requests while we are performing
                    //Other operations.
                    self.consensus_guard.lock_consensus();
                }
                (ConsensusPhase::SyncPhase, ConsensusPhase::NormalPhase) => {}
                (_, _) => {}
            }

            /*
            Observe event stuff
            @{
             */
            let _to_send = match (&old_phase, &self.phase) {
                (_, ConsensusPhase::SyncPhase) => ObserveEventKind::ViewChangePhase,
                (_, ConsensusPhase::NormalPhase) => {
                    let current_view = self.synchronizer.view();

                    let current_seq = self.consensus.sequence_number();

                    ObserveEventKind::NormalPhase((current_view, current_seq))
                }
            };

            /*self.observer_handle
                .tx()
                .send(MessageType::Event(to_send))
                .expect("Failed to notify observer thread");
            */
            /*
            }@
            */
        }
    }
}

const CF_PRE_PREPARES: &str = "PRE_PREPARES";
const CF_PREPARES: &str = "PREPARES";
const CF_COMMIT: &str = "COMMITS";

impl<RQ, NT> OrderProtocolPersistenceHelper<RQ, PBFTConsensus<RQ>, PBFTConsensus<RQ>>
for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    fn message_types() -> Vec<&'static str> {
        vec![CF_PRE_PREPARES, CF_PREPARES, CF_COMMIT]
    }

    fn get_type_for_message(msg: &PBFTMessage<RQ>) -> Result<&'static str> {
        match msg {
            PBFTMessage::Consensus(consensus) => match consensus.kind() {
                ConsensusMessageKind::PrePrepare(_) => Ok(CF_PRE_PREPARES),
                ConsensusMessageKind::Prepare(_) => Ok(CF_PREPARES),
                ConsensusMessageKind::Commit(_) => Ok(CF_COMMIT),
            },
            PBFTMessage::ViewChange(_view_change) => {
                Err(anyhow!("Failed to get type for view change message."))
            }
            PBFTMessage::ObserverMessage(_) => {
                Err(anyhow!("Failed to get type for view change message."))
            }
        }
    }

    fn init_proof_from(
        metadata: ProofMetadata,
        messages: Vec<StoredMessage<PBFTMessage<RQ>>>,
    ) -> Result<Proof<RQ>> {
        let mut messages_f = Vec::with_capacity(messages.len());

        for message in messages {
            messages_f.push(Arc::new(ReadOnly::new(message)));
        }

        Proof::init_from_messages(metadata, messages_f)
    }

    fn init_proof_from_scm(
        metadata: DecisionMetadata<RQ, PBFTConsensus<RQ>>,
        messages: Vec<ShareableConsensusMessage<RQ, PBFTConsensus<RQ>>>,
    ) -> Result<PProof<RQ, PBFTConsensus<RQ>, PBFTConsensus<RQ>>> {
        Proof::init_from_messages(metadata, messages)
    }

    fn decompose_proof(
        proof: &Proof<RQ>,
    ) -> (&ProofMetadata, Vec<&StoredMessage<PBFTMessage<RQ>>>) {
        let mut messages = Vec::new();

        for message in proof.pre_prepares() {
            messages.push(&***message);
        }

        for message in proof.prepares() {
            messages.push(&***message);
        }

        for message in proof.commits() {
            messages.push(&***message);
        }

        (proof.metadata(), messages)
    }

    fn get_requests_in_proof(
        proof: &PProof<RQ, PBFTConsensus<RQ>, PBFTConsensus<RQ>>,
    ) -> Result<ProtocolConsensusDecision<RQ>> {
        Ok(ProtocolConsensusDecision::from(proof))
    }
}

impl<RQ, NT> LoggableOrderProtocol<RQ> for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    type PersistableTypes = PBFTConsensus<RQ>;
}

impl<RQ, NT, RP> ReconfigurableOrderProtocol<RP> for PBFTOrderProtocol<RQ, NT>
    where
        RQ: SerType + SessionBased + 'static,
        RP: ReconfigurationProtocolMessage + 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
{
    fn attempt_quorum_node_join(
        &mut self,
        joining_node: NodeId,
    ) -> Result<ReconfigurationAttemptResult> {
        let result = self.synchronizer.start_join_quorum(
            joining_node,
            &*self.node,
            &self.timeouts,
            &self.message_log,
        );

        return match result {
            SyncReconfigurationResult::Failed => {
                warn!(
                    "Failed to start quorum view change to integrate node {:?}",
                    joining_node
                );

                Ok(ReconfigurationAttemptResult::Failed)
            }
            SyncReconfigurationResult::OnGoingViewChange => {
                Ok(ReconfigurationAttemptResult::InProgress)
            }
            SyncReconfigurationResult::OnGoingQuorumChange(node_id) if node_id == joining_node => {
                warn!(
                    "Received join request for node {:?} when it was already ongoing",
                    joining_node
                );

                Ok(ReconfigurationAttemptResult::CurrentlyReconfiguring(
                    node_id,
                ))
            }
            SyncReconfigurationResult::OnGoingQuorumChange(node_id) => Ok(
                ReconfigurationAttemptResult::CurrentlyReconfiguring(node_id),
            ),
            SyncReconfigurationResult::AlreadyPartOfQuorum => {
                Ok(ReconfigurationAttemptResult::AlreadyPartOfQuorum)
            }
            SyncReconfigurationResult::InProgress => Ok(ReconfigurationAttemptResult::InProgress),
            SyncReconfigurationResult::Completed => Ok(ReconfigurationAttemptResult::Successful(
                self.synchronizer.view().quorum_members().clone(),
            )),
        };
    }

    fn joining_quorum(&mut self) -> Result<ReconfigurationAttemptResult> {
        let result = self
            .synchronizer
            .attempt_join_quorum(&*self.node, &self.timeouts);

        match &result {
            ReconfigurationAttemptResult::Failed => {
                warn!("Failed to join quorum")
            }
            ReconfigurationAttemptResult::AlreadyPartOfQuorum => {}
            ReconfigurationAttemptResult::InProgress => {
                self.switch_phase(ConsensusPhase::SyncPhase);
            }
            _ => {}
        }

        Ok(result)
    }
}
