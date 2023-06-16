//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

use std::ops::Drop;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use log::{debug, info, LevelFilter, trace, warn};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Logger, Root},
    Config,
    encode::pattern::PatternEncoder,
};


use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::Node;
use atlas_communication::serialize::Serializable;
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::SharedData;
use atlas_core::messages::ClientRqInfo;
use atlas_core::messages::Protocol;
use atlas_core::ordering_protocol::{OrderingProtocol, OrderingProtocolArgs, OrderProtocolExecResult, OrderProtocolPoll, ProtocolConsensusDecision, ProtocolMessage, SerProof, SerProofMetadata, View};
use atlas_core::persistent_log::{OrderingProtocolLog, PersistableOrderProtocol, StatefulOrderingProtocolLog};
use atlas_core::request_pre_processing::{PreProcessorMessage, RequestPreProcessor};
use atlas_core::serialize::{NetworkView, ServiceMsg, StateTransferMessage};
use atlas_core::state_transfer::{Checkpoint, DecLog, StatefulOrderProtocol};
use atlas_core::timeouts::{RqTimeout, Timeouts};
use atlas_metrics::metrics::metric_duration;
use crate::bft::config::PBFTConfig;
use crate::bft::consensus::{Consensus, ConsensusPollStatus, ConsensusStatus, ProposerConsensusGuard};
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, ObserveEventKind, PBFTMessage, ViewChangeMessage};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::metric::{CONSENSUS_INSTALL_STATE_TIME_ID, MSG_LOG_INSTALL_TIME_ID};
use crate::bft::msg_log::{initialize_decided_log};
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::decisions::{DecisionLog, Proof};
use crate::bft::proposer::Proposer;
use crate::bft::sync::{AbstractSynchronizer, Synchronizer, SynchronizerPollStatus, SynchronizerStatus};

pub mod consensus;
pub mod proposer;
pub mod sync;
pub mod msg_log;
pub mod config;
pub mod message;
pub mod observer;
pub mod metric;

// The types responsible for this protocol
pub type PBFT<D, ST> = ServiceMsg<D, PBFTConsensus<D>, ST>;
// The message type for this consensus protocol
pub type SysMsg<D, ST> = <PBFT<D, ST> as Serializable>::Message;

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
    SyncProtocolFinished(Option<ProtocolConsensusDecision<O>>),
    RunCSTProtocol,
}

/// a PBFT based ordering protocol
pub struct PBFTOrderProtocol<D, ST, NT, PL>
    where
        D: SharedData + 'static,
        ST: StateTransferMessage + 'static,
        NT: Node<PBFT<D, ST>> + 'static,
        PL: Clone {
    // What phase of the consensus algorithm are we currently executing
    phase: ConsensusPhase,

    /// The consensus state machine
    consensus: Consensus<D, ST, PL>,
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
    message_log: Log<D, PL>,
    // The proposer of this replica
    proposer: Arc<Proposer<D, NT>>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,

    executor: ExecutorHandle<D>,
}

impl<D, ST, NT, PL> Orderable for PBFTOrderProtocol<D, ST, NT, PL> where D: 'static + SharedData,
                                                                         NT: 'static + Node<PBFT<D, ST>>,
                                                                         ST: 'static + StateTransferMessage,
                                                                         PL: Clone {
    fn sequence_number(&self) -> SeqNo {
        self.consensus.sequence_number()
    }
}

impl<D, ST, NT, PL> OrderingProtocol<D, NT, PL> for PBFTOrderProtocol<D, ST, NT, PL>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static,
          NT: Node<PBFT<D, ST>> + 'static,
          PL: Clone {
    type Serialization = PBFTConsensus<D>;
    type Config = PBFTConfig<D, ST>;

    fn initialize(config: PBFTConfig<D, ST>, args: OrderingProtocolArgs<D, NT, PL>) -> Result<Self> where
        Self: Sized,
    {
        Self::initialize_protocol(config, args, None)
    }

    fn view(&self) -> View<Self::Serialization> {
        self.synchronizer.view()
    }

    fn handle_off_ctx_message(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::Request>>>)
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let (header, message) = message.into_inner();

        match message.into_inner() {
            PBFTMessage::Consensus(consensus) => {
                debug!("{:?} // Received off context consensus message {:?}", self.node.id(), consensus);
                self.consensus.queue(header, consensus);
            }
            PBFTMessage::ViewChange(view_change) => {
                debug!("{:?} // Received off context view change message {:?}", self.node.id(), view_change);
                self.synchronizer.queue(header, view_change);

                self.synchronizer.signal();
            }
            _ => { todo!() }
        }
    }

    fn poll(&mut self) -> OrderProtocolPoll<PBFTMessage<D::Request>, D::Request>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        match self.phase {
            ConsensusPhase::NormalPhase => {
                self.poll_normal_phase()
            }
            ConsensusPhase::SyncPhase => {
                self.poll_sync_phase()
            }
        }
    }

    fn process_message(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::Request>>>) -> Result<OrderProtocolExecResult<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        match self.phase {
            ConsensusPhase::NormalPhase => {
                self.update_normal_phase(message)
            }
            ConsensusPhase::SyncPhase => {
                self.update_sync_phase(message)
            }
        }
    }

    fn handle_timeout(&mut self, timeout: Vec<RqTimeout>) -> Result<OrderProtocolExecResult<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        if self.consensus.is_catching_up() {
            warn!("{:?} // Ignoring timeouts while catching up", self.node.id());

            return Ok(OrderProtocolExecResult::Success);
        }

        let status = self.synchronizer
            .client_requests_timed_out(&self.synchronizer, self.node.id(), &timeout);

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

        Ok(OrderProtocolExecResult::Success)
    }

    fn sequence_number_with_proof(&self) -> Result<Option<(SeqNo, SerProof<Self::Serialization>)>> {
        Ok(self.message_log.last_proof(self.synchronizer.view().f())
            .map(|p| (p.sequence_number(), p)))
    }

    fn verify_sequence_number(&self, seq_no: SeqNo, proof: &SerProof<Self::Serialization>) -> Result<bool> {
        let proof: &Proof<D::Request> = proof;

        //TODO: Verify the proof

        Ok(true)
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

    fn install_seq_no(&mut self, seq_no: SeqNo) -> Result<()> {
        self.consensus.install_sequence_number(seq_no, &self.synchronizer.view());

        Ok(())
    }
}

impl<D, ST, NT, PL> PBFTOrderProtocol<D, ST, NT, PL> where D: SharedData + 'static,
                                                           ST: StateTransferMessage + 'static,
                                                           NT: Node<PBFT<D, ST>> + 'static,
                                                           PL: Clone {
    fn initialize_protocol(config: PBFTConfig<D, ST>, args: OrderingProtocolArgs<D, NT, PL>,
                           initial_state: Option<DecisionLog<D::Request>>) -> Result<Self> {
        let PBFTConfig {
            node_id,
            follower_handle,
            view, timeout_dur,
            proposer_config, watermark, _phantom_data
        } = config;

        let OrderingProtocolArgs(executor, timeouts,
                                 pre_processor, batch_input, node, persistent_log) = args;

        let sync = Synchronizer::new_replica(view.clone(), timeout_dur);

        let consensus_guard = ProposerConsensusGuard::new(view.clone(), watermark);

        let consensus = Consensus::<D, ST, PL>::new_replica(node_id, &sync.view(), executor.clone(),
                                                            SeqNo::ZERO, watermark, consensus_guard.clone(),
                                                            timeouts.clone(), persistent_log.clone());

        let dec_log = initialize_decided_log::<D, PL>(node_id, persistent_log, initial_state)?;

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

        println!("{:?} // Leader count: {}, Leader set: {:?}", replica.node.id(),
                 crr_view.leader_set().len(), crr_view.leader_set());
        println!("{:?} // Watermark: {}", replica.node.id(), watermark);

        replica.proposer.clone().start();

        Ok(replica)
    }

    fn poll_sync_phase(&mut self) -> OrderProtocolPoll<PBFTMessage<D::Request>, D::Request>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {

        // retrieve a view change message to be processed
        let poll_result = self.synchronizer.poll();

        debug!("{:?} // Polling sync phase {:?}", self.node.id(), poll_result);

        match poll_result {
            SynchronizerPollStatus::Recv => OrderProtocolPoll::ReceiveFromReplicas,
            SynchronizerPollStatus::NextMessage(h, m) => {
                OrderProtocolPoll::Exec(StoredMessage::new(h, Protocol::new(PBFTMessage::ViewChange(m))))
            }
            SynchronizerPollStatus::ResumeViewChange => {
                debug!("{:?} // Resuming view change", self.node.id());

                self.synchronizer.resume_view_change(
                    &mut self.message_log,
                    &self.timeouts,
                    &mut self.consensus,
                    &*self.node,
                );

                self.switch_phase(ConsensusPhase::NormalPhase);

                OrderProtocolPoll::RePoll
            }
        }
    }

    fn poll_normal_phase(&mut self) -> OrderProtocolPoll<PBFTMessage<D::Request>, D::Request>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        // Consume them until we reached a point where no new messages can be processed
        while self.synchronizer.can_process_stops() {
            let sync_protocol = self.poll_sync_phase();

            if let OrderProtocolPoll::Exec(message) = sync_protocol {
                let (header, message) = message.into_inner();

                if let PBFTMessage::ViewChange(view_change) = message.into_inner() {
                    let result = self.adv_sync(header, view_change);

                    match result {
                        SyncPhaseRes::RunSyncProtocol => {
                            self.switch_phase(ConsensusPhase::SyncPhase);

                            return OrderProtocolPoll::RePoll;
                        }
                        SyncPhaseRes::RunCSTProtocol => {
                            // We don't need to switch to the sync phase
                            // As that has already been done by the adv sync method
                            return OrderProtocolPoll::RunCst;
                        }
                        _ => {}
                    }
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

        match polled_message {
            ConsensusPollStatus::Recv => OrderProtocolPoll::ReceiveFromReplicas,
            ConsensusPollStatus::NextMessage(h, m) => {
                OrderProtocolPoll::Exec(StoredMessage::new(h, Protocol::new(PBFTMessage::Consensus(m))))
            }
            ConsensusPollStatus::Decided => {
                return OrderProtocolPoll::Decided(self.finalize_all_possible().expect("Failed to finalize decisions"));
            }
        }
    }

    fn update_sync_phase(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::Request>>>) -> Result<OrderProtocolExecResult<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let (header, protocol) = message.into_inner();

        match protocol.into_inner() {
            PBFTMessage::ViewChange(view_change) => {
                return Ok(match self.adv_sync(header, view_change) {
                    SyncPhaseRes::SyncProtocolNotNeeded => {
                        OrderProtocolExecResult::Success
                    }
                    SyncPhaseRes::RunSyncProtocol => {
                        OrderProtocolExecResult::Success
                    }
                    SyncPhaseRes::SyncProtocolFinished(to_execute) => {
                        match to_execute {
                            None => {
                                OrderProtocolExecResult::Success
                            }
                            Some(to_execute) => {
                                OrderProtocolExecResult::Decided(vec![to_execute])
                            }
                        }
                    }
                    SyncPhaseRes::RunCSTProtocol => {
                        OrderProtocolExecResult::RunCst
                    }
                });
            }
            PBFTMessage::Consensus(message) => {
                self.consensus.queue(header, message);
            }
            _ => {}
        }

        Ok(OrderProtocolExecResult::Success)
    }

    fn update_normal_phase(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::Request>>>) -> Result<OrderProtocolExecResult<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let (header, protocol) = message.into_inner();

        match protocol.into_inner() {
            PBFTMessage::Consensus(message) => {
                return self.adv_consensus(header, message);
            }
            PBFTMessage::ViewChange(view_change) => {
                let status = self.synchronizer.process_message(
                    header,
                    view_change,
                    &self.timeouts,
                    &mut self.message_log,
                    &self.pre_processor,
                    &mut self.consensus,
                    &*self.node,
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

        Ok(OrderProtocolExecResult::Success)
    }

    /// Advance the consensus phase with a received message
    fn adv_consensus(
        &mut self,
        header: Header,
        message: ConsensusMessage<D::Request>,
    ) -> Result<OrderProtocolExecResult<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let seq = self.consensus.sequence_number();

        // debug!(
        //     "{:?} // Processing consensus message {:?} ",
        //     self.id(),
        //     message
        // );

        let status = self.consensus.process_message(
            header,
            message,
            &self.synchronizer,
            &self.timeouts,
            &mut self.message_log,
            &*self.node,
        )?;

        match status {
            // if deciding, nothing to do
            ConsensusStatus::Deciding => {}
            // FIXME: implement this
            ConsensusStatus::VotedTwice(_) => todo!(),
            // reached agreement, execute requests
            //
            // FIXME: execution layer needs to receive the id
            // attributed by the consensus layer to each op,
            // to execute in order
            ConsensusStatus::Decided => {
                return Ok(OrderProtocolExecResult::Decided(self.finalize_all_possible()?));
            }
        }

        //
        // debug!(
        //     "{:?} // Done processing consensus message. Took {:?}",
        //     self.id(),
        //     Instant::now().duration_since(start)
        // );

        Ok(OrderProtocolExecResult::Success)
    }

    /// Finalize all possible consensus instances
    fn finalize_all_possible(&mut self) -> Result<Vec<ProtocolConsensusDecision<D::Request>>> {
        let view = self.synchronizer.view();

        let mut finalized_decisions = Vec::with_capacity(self.consensus.finalizeable_count());

        while self.consensus.can_finalize() {
            // This will automatically move the consensus machine to the next consensus instance
            let completed_batch = self.consensus.finalize(&view)?.unwrap();

            let seq = completed_batch.sequence_number();

            //Should the execution be scheduled here or will it be scheduled by the persistent log?
            let exec_info = self.message_log.finalize_batch(seq, completed_batch)?;

            finalized_decisions.push(exec_info);
        }

        Ok(finalized_decisions)
    }


    /// Advance the sync phase of the algorithm
    fn adv_sync(&mut self, header: Header,
                message: ViewChangeMessage<D::Request>) -> SyncPhaseRes<D::Request>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let status = self.synchronizer.process_message(
            header,
            message,
            &self.timeouts,
            &mut self.message_log,
            &self.pre_processor,
            &mut self.consensus,
            &*self.node,
        );

        self.synchronizer.signal();

        return match status {
            SynchronizerStatus::Nil => SyncPhaseRes::SyncProtocolNotNeeded,
            SynchronizerStatus::Running => SyncPhaseRes::RunSyncProtocol,
            SynchronizerStatus::NewView(to_execute) => {
                //Our current view has been updated and we have no more state operations
                //to perform. This happens if we are a correct replica and therefore do not need
                //To update our state or if we are a replica that was incorrect and whose state has
                //Already been updated from the Cst protocol
                self.switch_phase(ConsensusPhase::NormalPhase);

                SyncPhaseRes::SyncProtocolFinished(to_execute)
            }
            SynchronizerStatus::RunCst => {
                //This happens when a new view is being introduced and we are not up to date
                //With the rest of the replicas. This might happen because the replica was faulty
                //or any other reason that might cause it to lose some updates from the other replicas

                //After we update the state, we go back to the sync phase (this phase) so we can check if we are missing
                //Anything or to finalize and go back to the normal phase
                self.switch_phase(ConsensusPhase::SyncPhase);

                SyncPhaseRes::RunCSTProtocol
            }
            // should not happen...
            _ => {
                unreachable!()
            }
        };
    }
}

impl<D, ST, NT, PL> StatefulOrderProtocol<D, NT, PL> for PBFTOrderProtocol<D, ST, NT, PL>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static,
          NT: Node<PBFT<D, ST>> + 'static,
          PL: Clone {
    type StateSerialization = PBFTConsensus<D>;

    fn initialize_with_initial_state(config: Self::Config,
                                     args: OrderingProtocolArgs<D, NT, PL>,
                                     initial_state: DecisionLog<D::Request>) -> Result<Self> where Self: Sized {
        Self::initialize_protocol(config, args, Some(initial_state))
    }

    fn install_state(&mut self,
                     view_info: View<Self::Serialization>,
                     dec_log: DecLog<Self::StateSerialization>) -> Result<Vec<D::Request>>
        where PL: StatefulOrderingProtocolLog<PBFTConsensus<D>, PBFTConsensus<D>> {
        info!("{:?} // Installing decision log with Seq No {:?} and View {:?}", self.node.id(),
                dec_log.sequence_number(), view_info);

        let last_exec = if let Some(last_exec) = dec_log.last_execution() {
            last_exec
        } else {
            SeqNo::ZERO
        };

        info!("{:?} // Installing decision log with last execution {:?}", self.node.id(),last_exec);

        self.synchronizer.install_view(view_info.clone());

        let start = Instant::now();

        let res = self.consensus.install_state(view_info.clone(), &dec_log)?;

        metric_duration(CONSENSUS_INSTALL_STATE_TIME_ID, start.elapsed());

        let start = Instant::now();

        self.message_log.install_state(view_info, dec_log);

        metric_duration(MSG_LOG_INSTALL_TIME_ID, start.elapsed());

        Ok(res)
    }

    fn snapshot_log(&mut self) -> Result<(View<Self::Serialization>, DecLog<Self::StateSerialization>)> {
        self.message_log.snapshot(self.synchronizer.view())
    }

    fn checkpointed(&mut self, seq_no: SeqNo) -> Result<()> {
        self.message_log.finalize_checkpoint(seq_no)
    }
}

impl<D, ST, NT, PL> PBFTOrderProtocol<D, ST, NT, PL>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static,
          NT: Node<PBFT<D, ST>> + 'static,
          PL: Clone {
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

impl<D, ST, NT, PL> PersistableOrderProtocol<PBFTConsensus<D>, PBFTConsensus<D>> for PBFTOrderProtocol<D, ST, NT, PL>
    where D: SharedData, ST: StateTransferMessage, NT: Node<PBFT<D, ST>>, PL: Clone {

    fn message_types() -> Vec<&'static str> {
        vec![
            CF_PRE_PREPARES,
            CF_PREPARES, CF_COMMIT,
        ]
    }

    fn get_type_for_message(msg: &ProtocolMessage<PBFTConsensus<D>>) -> Result<&'static str> {
        match msg {
            PBFTMessage::Consensus(msg) => {
                match msg.kind() {
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
            _ => {
                Err(Error::simple_with_msg(ErrorKind::MsgLogPersistentSerialization, "Invalid message type"))
            }
        }
    }

    fn init_proof_from(metadata: SerProofMetadata<PBFTConsensus<D>>, messages: Vec<StoredMessage<ProtocolMessage<PBFTConsensus<D>>>>) -> SerProof<PBFTConsensus<D>> {
        let mut pre_prepares = Vec::with_capacity(messages.len() / 2);
        let mut prepares = Vec::with_capacity(messages.len() / 2);
        let mut commits = Vec::with_capacity(messages.len() / 2);

        for message in messages {
            match message.message() {
                PBFTMessage::Consensus(cons) => {
                    match cons.kind() {
                        ConsensusMessageKind::PrePrepare(_) => {
                            pre_prepares.push(Arc::new(ReadOnly::new(message)));
                        }
                        ConsensusMessageKind::Prepare(_) => {
                            prepares.push(Arc::new(ReadOnly::new(message)));
                        }
                        ConsensusMessageKind::Commit(_) => {
                            commits.push(Arc::new(ReadOnly::new(message)));
                        }
                    }
                }
                PBFTMessage::ViewChange(_) => { unreachable!() }
                PBFTMessage::ObserverMessage(_) => { unreachable!() }
            }
        }

        Proof::new(metadata, pre_prepares, prepares, commits)
    }

    fn init_dec_log(proofs: Vec<SerProof<PBFTConsensus<D>>>) -> DecLog<PBFTConsensus<D>> {
        DecisionLog::from_proofs(proofs)
    }

    fn decompose_proof(proof: &SerProof<PBFTConsensus<D>>) -> (&SerProofMetadata<PBFTConsensus<D>>, Vec<&StoredMessage<ProtocolMessage<PBFTConsensus<D>>>>) {
        let mut messages = Vec::new();

        for message in proof.pre_prepares() {
            messages.push(&**message.as_ref());
        }

        for message in proof.prepares() {
            messages.push(&**message.as_ref());
        }

        for message in proof.commits() {
            messages.push(&**message.as_ref());
        }

        (proof.metadata(), messages)
    }

    fn decompose_dec_log(proofs: &DecLog<PBFTConsensus<D>>) -> Vec<&SerProof<PBFTConsensus<D>>> {
        proofs.proofs().iter().collect()
    }
}
