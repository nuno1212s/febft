//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ::log::{debug, error, info, trace, warn};
use anyhow::anyhow;
use either::Either;

use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_core::ordering_protocol::{Decision, DecisionInfo, DecisionMetadata, DecisionsAhead, JoinInfo, OPExecResult, OPPollResult, OrderingProtocol, OrderingProtocolArgs, OrderProtocolTolerance, PermissionedOrderingProtocol, ProtocolConsensusDecision};
use atlas_core::ordering_protocol::loggable::{LoggableOrderProtocol, OrderProtocolPersistenceHelper, PProof};
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_core::ordering_protocol::networking::serialize::{NetworkView, OrderingProtocolMessage};
use atlas_core::ordering_protocol::reconfigurable_order_protocol::{ReconfigurableOrderProtocol, ReconfigurationAttemptResult};
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_core::request_pre_processing::RequestPreProcessor;
use atlas_core::serialize::ReconfigurationProtocolMessage;
use atlas_core::smr::smr_decision_log::{ShareableConsensusMessage, ShareableMessage};
use atlas_core::timeouts::{RqTimeout, Timeouts};
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;

use crate::bft::config::PBFTConfig;
use crate::bft::consensus::{Consensus, ConsensusPollStatus, ConsensusStatus, ProposerConsensusGuard};
use crate::bft::log::{initialize_decided_log, Log};
use crate::bft::log::decided::DecisionLog;
use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::message::{ConsensusMessageKind, ObserveEventKind, PBFTMessage};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::proposer::Proposer;
use crate::bft::sync::{AbstractSynchronizer, Synchronizer, SynchronizerPollStatus, SynchronizerStatus, SyncReconfigurationResult};
use crate::bft::sync::view::ViewInfo;

pub mod consensus;
pub mod proposer;
pub mod sync;
pub mod log;
pub mod config;
pub mod message;
pub mod observer;
pub mod metric;

// The types responsible for this protocol
pub type PBFT<D> = PBFTConsensus<D>;
// The message type for this consensus protocol
pub type SysMsg<D> = <PBFTConsensus<D> as OrderingProtocolMessage<D>>::ProtocolMessage;

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
pub struct PBFTOrderProtocol<D, NT, >
    where
        D: ApplicationData + 'static,
        NT: OrderProtocolSendNode<D, PBFT<D>> + 'static, {
    // What phase of the consensus algorithm are we currently executing
    phase: ConsensusPhase,
    /// The consensus state machine
    consensus: Consensus<D>,
    /// The synchronizer state machine
    synchronizer: Arc<Synchronizer<D>>,
    /// The request pre processor
    pre_processor: RequestPreProcessor<D::Request>,
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
    message_log: Log<D>,
    // The proposer of this replica
    proposer: Arc<Proposer<D, NT>>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
    // The handle to the executor, currently not utilized
    executor: ExecutorHandle<D>,
}

impl<D, NT, > Orderable for PBFTOrderProtocol<D, NT>
    where D: 'static + ApplicationData,
          NT: 'static + OrderProtocolSendNode<D, PBFT<D>>, {
    fn sequence_number(&self) -> SeqNo {
        self.consensus.sequence_number()
    }
}

impl<D, NT, > OrderProtocolTolerance for PBFTOrderProtocol<D, NT>
    where D: 'static + ApplicationData,
          NT: 'static + OrderProtocolSendNode<D, PBFT<D>>, {
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

impl<D, NT> OrderingProtocol<D, NT> for PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> + 'static, {
    type Serialization = PBFTConsensus<D>;
    type Config = PBFTConfig;

    fn initialize(config: PBFTConfig, args: OrderingProtocolArgs<D, NT>) -> Result<Self> where
        Self: Sized,
    {
        Self::initialize_protocol(config, args, None)
    }


    fn handle_off_ctx_message(&mut self, message: ShareableMessage<PBFTMessage<D::Request>>) {
        match message.message() {
            PBFTMessage::Consensus(consensus) => {
                debug!("{:?} // Received off context consensus message {:?}", self.node.id(), consensus);
                self.consensus.queue(message);
            }
            PBFTMessage::ViewChange(view_change) => {
                debug!("{:?} // Received off context view change message {:?}", self.node.id(), view_change);
                self.synchronizer.queue(message);

                self.synchronizer.signal();
            }
            _ => { todo!() }
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

        info!("Execution has changed to {} with our current phase being {:?}", is_executing, self.phase);

        Ok(())
    }

    fn poll(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        trace!("{:?} // Polling {:?}", self.node.id(), self.phase);

        match self.phase {
            ConsensusPhase::NormalPhase => {
                self.poll_normal_phase()
            }
            ConsensusPhase::SyncPhase => {
                self.poll_sync_phase()
            }
        }
    }

    fn process_message(&mut self, message: ShareableMessage<PBFTMessage<D::Request>>) -> Result<OPExecResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        match self.phase {
            ConsensusPhase::NormalPhase => {
                self.update_normal_phase(message)
            }
            ConsensusPhase::SyncPhase => {
                self.update_sync_phase(message)
            }
        }
    }

    fn install_seq_no(&mut self, seq_no: SeqNo) -> Result<()> {
        self.consensus.install_sequence_number(seq_no, &self.synchronizer.view());

        Ok(())
    }

    fn handle_timeout(&mut self, timeout: Vec<RqTimeout>) -> Result<OPExecResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        if self.consensus.is_catching_up() {
            warn!("{:?} // Ignoring timeouts while catching up", self.node.id());

            return Ok(OPExecResult::MessageDropped);
        }

        let status = self.synchronizer.client_requests_timed_out(self.node.id(), &timeout);

        match status {
            SynchronizerStatus::RequestsTimedOut { forwarded, stopped } => {
                if forwarded.len() > 0 {
                    let requests = self.pre_processor.clone_pending_rqs(forwarded);

                    self.synchronizer.forward_requests(
                        requests,
                        &*self.node,
                    );
                }

                if stopped.len() > 0 {
                    let stopped = self.pre_processor.clone_pending_rqs(stopped);

                    self.switch_phase(ConsensusPhase::SyncPhase);

                    self.synchronizer.begin_view_change(Some(stopped),
                                                        &*self.node,
                                                        &self.timeouts,
                                                        &self.message_log);
                }
            }
            // nothing to do
            _ => (),
        }

        Ok(OPExecResult::MessageProcessedNoUpdate)
    }
}

impl<D, NT> PermissionedOrderingProtocol for PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> + 'static {
    type PermissionedSerialization = PBFTConsensus<D>;

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

impl<D, NT> PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> + 'static {
    fn initialize_protocol(config: PBFTConfig, args: OrderingProtocolArgs<D, NT>,
                           initial_state: Option<DecisionLog<D::Request>>) -> Result<Self> {
        let PBFTConfig {
            timeout_dur,
            proposer_config, watermark
        } = config;

        let OrderingProtocolArgs(node_id, executor, timeouts,
                                 pre_processor, batch_input,
                                 node, quorum) = args;

        let sync = Synchronizer::initialize_with_quorum(node_id, SeqNo::ZERO, quorum.clone(), timeout_dur)?;

        let consensus_guard = ProposerConsensusGuard::new(sync.view(), watermark);

        let consensus = Consensus::<D>::new_replica(node_id, &sync.view(), executor.clone(),
                                                    SeqNo::ZERO, watermark, consensus_guard.clone(),
                                                    timeouts.clone());

        let dec_log = initialize_decided_log::<D>(node_id);

        let proposer = Proposer::<D, NT>::new(node.clone(), batch_input, sync.clone(), timeouts.clone(),
                                              executor.clone(), consensus_guard.clone(),
                                              proposer_config);

        let replica = Self {
            phase: ConsensusPhase::NormalPhase,
            consensus,
            synchronizer: sync,
            pre_processor,
            timeouts,
            consensus_guard,
            unordered_rq_guard: Arc::new(Default::default()),
            executor,
            message_log: dec_log,
            proposer,
            node,
        };

        let crr_view = replica.synchronizer.view();

        info!("{:?} // Initialized ordering protocol with view: {:?} and sequence number: {:?}",
              replica.node.id(), crr_view.sequence_number(), replica.sequence_number());

        info!("{:?} // Leader count: {}, Leader set: {:?}", replica.node.id(),
        crr_view.leader_set().len(), crr_view.leader_set());

        info!("{:?} // Watermark: {}", replica.node.id(), watermark);

        println!("{:?} // Leader count: {}, Leader set: {:?}, Quorum: {:?}", replica.node.id(),
                 crr_view.leader_set().len(), crr_view.leader_set(), quorum);
        println!("{:?} // Watermark: {}", replica.node.id(), watermark);

        replica.proposer.clone().start();

        Ok(replica)
    }

    fn poll_sync_phase(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {

        // retrieve a view change message to be processed
        let poll_result = self.synchronizer.poll();

        debug!("{:?} // Polling sync phase {:?}", self.node.id(), poll_result);

        match poll_result {
            SynchronizerPollStatus::Recv => Ok(OPPollResult::ReceiveMsg),
            SynchronizerPollStatus::NextMessage(message) => {
                Ok(OPPollResult::Exec(message))
            }
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
                        SynchronizerStatus::NewViewJoinedQuorum(consensus_status, decisions, node) => {
                            let quorum_members = self.synchronizer.view().quorum_members().clone();

                            let decisions = self.handle_sync_result(consensus_status, decisions)?;

                            let joined = JoinInfo::new(node, self.synchronizer.view().quorum_members().clone());

                            if decisions.is_empty() {
                                Ok(OPPollResult::QuorumJoined(DecisionsAhead::ClearAhead, None, joined))
                            } else {
                                Ok(OPPollResult::QuorumJoined(DecisionsAhead::ClearAhead, Some(decisions), joined))
                            }
                        }
                        SynchronizerStatus::NewView(consensus_status, decisions) => {
                            let decisions = self.handle_sync_result(consensus_status, decisions)?;

                            Ok(OPPollResult::ProgressedDecision(DecisionsAhead::ClearAhead, decisions))
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

    fn poll_normal_phase(&mut self) -> Result<OPPollResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        // Consume them until we reached a point where no new messages can be processed
        while self.synchronizer.can_process_stops() {
            let sync_protocol = self.poll_sync_phase()?;

            if let OPPollResult::Exec(s_message) = sync_protocol {
                let (header, message) = (s_message.header(), s_message.message());

                if let PBFTMessage::ViewChange(view_change) = message {
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
                    unreachable!("Synchronizer returned a message other than a view change message");
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
            ConsensusPollStatus::NextMessage(message) => {
                OPPollResult::Exec(message)
            }
            ConsensusPollStatus::Decided(decisions) => {
                OPPollResult::ProgressedDecision(DecisionsAhead::Ignore, self.handle_decided(decisions)?)
            }
        })
    }

    fn update_sync_phase(&mut self, message: ShareableMessage<PBFTMessage<D::Request>>) -> Result<OPExecResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        match message.message() {
            PBFTMessage::ViewChange(view_change) => {
                return Ok(match self.adv_sync(message) {
                    SyncPhaseRes::SyncProtocolNotNeeded => {
                        OPExecResult::MessageProcessedNoUpdate
                    }
                    SyncPhaseRes::RunSyncProtocol => {
                        OPExecResult::MessageProcessedNoUpdate
                    }
                    SyncPhaseRes::SyncProtocolFinished(status, to_execute) => {
                        OPExecResult::ProgressedDecision(DecisionsAhead::ClearAhead, self.handle_sync_result(status, to_execute)?)
                    }
                    SyncPhaseRes::JoinedQuorum(status, to_execute, node) => {
                        info!("Replica {:?} joined the quorum, with a decision to execute? {}", node, to_execute.is_some());

                        let new_quorum = self.synchronizer.view().quorum_members().clone();

                        let join_info = JoinInfo::new(node, new_quorum);

                        let decision_adv = self.handle_sync_result(status, to_execute)?;

                        if decision_adv.is_empty() {
                            OPExecResult::QuorumJoined(DecisionsAhead::ClearAhead, None, join_info)
                        } else {
                            OPExecResult::QuorumJoined(DecisionsAhead::ClearAhead, Some(decision_adv), join_info)
                        }
                    }
                    SyncPhaseRes::RunCSTProtocol => {
                        OPExecResult::RunCst
                    }
                });
            }
            PBFTMessage::Consensus(_) => {
                self.consensus.queue(message);
            }
            _ => {}
        }

        Ok(OPExecResult::MessageProcessedNoUpdate)
    }

    fn handle_decided(&mut self, decisions: MaybeVec<Decision<ProofMetadata, PBFTMessage<D::Request>, D::Request>>) -> Result<MaybeVec<OPDecision<D::Request>>> {
        let finalized_decisions = self.finalize_all_possible()?;

        let decisions = self.merge_decisions(decisions, finalized_decisions)?;

        Ok(decisions)
    }

    fn update_normal_phase(&mut self, message: ShareableMessage<PBFTMessage<D::Request>>) -> Result<OPExecResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
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
                    SynchronizerStatus::Running => {
                        self.switch_phase(ConsensusPhase::SyncPhase)
                    }
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
        message: ShareableMessage<PBFTMessage<D::Request>>,
    ) -> Result<OPExecResult<ProofMetadata, PBFTMessage<D::Request>, D::Request>> {
        let seq = self.consensus.sequence_number();

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

        return Ok(match status {
            ConsensusStatus::VotedTwice(_) | ConsensusStatus::MessageIgnored => {
                OPExecResult::MessageDropped
            }
            ConsensusStatus::MessageQueued => {
                OPExecResult::MessageQueued
            }
            ConsensusStatus::Deciding(result) => {
                OPExecResult::ProgressedDecision(DecisionsAhead::Ignore, result)
            }
            ConsensusStatus::Decided(result) => {
                OPExecResult::ProgressedDecision(DecisionsAhead::Ignore, self.handle_decided(result)?)
            }
        });
    }

    /// Finalize all possible consensus instances
    fn finalize_all_possible(&mut self) -> Result<Vec<ProtocolConsensusDecision<D::Request>>> {
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
    fn adv_sync(&mut self, message: ShareableMessage<PBFTMessage<D::Request>>) -> SyncPhaseRes<D::Request> {
        let status = self.synchronizer.process_message(
            message,
            &self.timeouts,
            &mut self.message_log,
            &self.pre_processor,
            &mut self.consensus,
            &self.node,
        );

        self.synchronizer.signal();

        return match status {
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
        };
    }

    fn merge_decisions(&mut self, status: MaybeVec<OPDecision<D::Request>>, finalized_decisions: Vec<ProtocolConsensusDecision<D::Request>>) -> Result<MaybeVec<OPDecision<D::Request>>> {
        let mut map = BTreeMap::new();

        Self::merge_decision_vec(&mut map, status)?;

        for decision in finalized_decisions {
            if let Some(member) = map.get_mut(&decision.sequence_number()) {
                member.append_decision_info(DecisionInfo::DecisionDone(decision));
            } else {
                map.insert(decision.sequence_number(), Decision::completed_decision(decision.sequence_number(), decision));
            }
        }

        let mut decisions = MaybeVec::builder();

        // By turning this btree map into a vec, we maintain ordering on the delivery (Shouldn't
        // really be necessary but always nice to have)
        map.into_iter().for_each(|(seq, decision)| {
            decisions.push(decision);
        });

        Ok(decisions.build())
    }

    /// Handles the result of a synchronizer result
    fn handle_sync_result(&mut self, status: ConsensusStatus<D::Request>, to_exec: Option<OPDecision<D::Request>>) -> Result<MaybeVec<OPDecision<D::Request>>> {
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
                        map.insert(decision.sequence_number(), Decision::completed_decision(decision.sequence_number(), decision));
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
        map.into_iter().for_each(|(seq, decision)| {
            decisions.push(decision);
        });

        Ok(decisions.build())
    }

    /// Merge a decision vector with the already existing btreemap
    fn merge_decision_vec(map: &mut BTreeMap<SeqNo, OPDecision<<D as ApplicationData>::Request>>, decision: MaybeVec<OPDecision<<D as ApplicationData>::Request>>) -> Result<()> {
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

impl<D, NT> PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> + 'static, {
    pub(crate) fn switch_phase(&mut self, new_phase: ConsensusPhase) {
        info!("{:?} // Switching from phase {:?} to phase {:?}", self.node.id(), self.phase, new_phase);

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
            let to_send = match (&old_phase, &self.phase) {
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

impl<D, NT> OrderProtocolPersistenceHelper<D, PBFTConsensus<D>, PBFTConsensus<D>> for PBFTOrderProtocol<D, NT> where D: 'static + ApplicationData, NT: OrderProtocolSendNode<D, PBFT<D>> {
    fn message_types() -> Vec<&'static str> {
        vec![
            CF_PRE_PREPARES,
            CF_PREPARES,
            CF_COMMIT,
        ]
    }

    fn get_type_for_message(msg: &PBFTMessage<D::Request>) -> Result<&'static str> {
        match msg {
            PBFTMessage::Consensus(consensus) => {
                match consensus.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        Ok(CF_PRE_PREPARES)
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        Ok(CF_PREPARES)
                    }
                    ConsensusMessageKind::Commit(_) => {
                        Ok(CF_COMMIT)
                    }
                }
            }
            PBFTMessage::ViewChange(view_change) => Err(anyhow!("Failed to get type for view change message.")),
            PBFTMessage::ObserverMessage(_) => Err(anyhow!("Failed to get type for view change message."))
        }
    }

    fn init_proof_from(metadata: ProofMetadata, messages: Vec<StoredMessage<PBFTMessage<D::Request>>>) -> Result<Proof<D::Request>> {
        let mut messages_f = Vec::with_capacity(messages.len());

        for message in messages {
            messages_f.push(Arc::new(ReadOnly::new(message)));
        }

        Proof::init_from_messages(metadata, messages_f)
    }

    fn init_proof_from_scm(metadata: DecisionMetadata<D, PBFTConsensus<D>>,
                           messages: Vec<ShareableConsensusMessage<D, PBFTConsensus<D>>>) -> Result<PProof<D, PBFTConsensus<D>, PBFTConsensus<D>>> {
        Proof::init_from_messages(metadata, messages)
    }

    fn decompose_proof(proof: &Proof<D::Request>) -> (&ProofMetadata, Vec<&StoredMessage<PBFTMessage<D::Request>>>) {
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

    fn get_requests_in_proof(proof: &PProof<D, PBFTConsensus<D>, PBFTConsensus<D>>) -> Result<ProtocolConsensusDecision<D::Request>> {
        Ok(ProtocolConsensusDecision::from(proof))
    }
}

impl<D, NT> LoggableOrderProtocol<D, NT> for PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> {
    type PersistableTypes = PBFTConsensus<D>;
}

impl<D, NT, RP> ReconfigurableOrderProtocol<RP> for PBFTOrderProtocol<D, NT>
    where D: ApplicationData + 'static,
          RP: ReconfigurationProtocolMessage + 'static,
          NT: OrderProtocolSendNode<D, PBFT<D>> + 'static {
    fn attempt_quorum_node_join(&mut self, joining_node: NodeId) -> Result<ReconfigurationAttemptResult> {
        let result = self.synchronizer.start_join_quorum(joining_node, &*self.node, &self.timeouts, &self.message_log);

        return match result {
            SyncReconfigurationResult::Failed => {
                warn!("Failed to start quorum view change to integrate node {:?}", joining_node);

                Ok(ReconfigurationAttemptResult::Failed)
            }
            SyncReconfigurationResult::OnGoingViewChange => {
                Ok(ReconfigurationAttemptResult::InProgress)
            }
            SyncReconfigurationResult::OnGoingQuorumChange(node_id) if node_id == joining_node => {
                warn!("Received join request for node {:?} when it was already ongoing", joining_node);

                Ok(ReconfigurationAttemptResult::CurrentlyReconfiguring(node_id))
            }
            SyncReconfigurationResult::OnGoingQuorumChange(node_id) => {
                Ok(ReconfigurationAttemptResult::CurrentlyReconfiguring(node_id))
            }
            SyncReconfigurationResult::AlreadyPartOfQuorum => {
                Ok(ReconfigurationAttemptResult::AlreadyPartOfQuorum)
            }
            SyncReconfigurationResult::InProgress => {
                Ok(ReconfigurationAttemptResult::InProgress)
            }
            SyncReconfigurationResult::Completed => {
                Ok(ReconfigurationAttemptResult::Successful)
            }
        };
    }

    fn joining_quorum(&mut self) -> Result<ReconfigurationAttemptResult> {
        let result = self.synchronizer.attempt_join_quorum(&*self.node, &self.timeouts);

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
