//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

pub mod consensus;
pub mod cst;
pub mod proposer;
pub mod sync;
pub mod msg_log;
pub mod config;
pub mod message;
pub mod observer;
pub mod follower;

use std::ops::Drop;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use log::{debug, LevelFilter};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Logger, Root},
    Config,
    encode::pattern::PatternEncoder,
};
use febft_common::error::*;
use febft_common::{async_runtime, socket, threadpool};
use febft_common::globals::Flag;
use febft_communication::message::{Header, StoredMessage};
use febft_communication::Node;
use febft_communication::serialize::Serializable;
use febft_execution::app::{Request, Service, State};
use febft_execution::serialize::SharedData;
use febft_messages::messages::{Protocol, SystemMessage};
use febft_messages::ordering_protocol::{OrderingProtocolImpl, OrderProtocolExecResult, OrderProtocolPoll};
use febft_messages::serialize::System;
use crate::bft::consensus::{AbstractConsensus, Consensus, ConsensusGuard, ConsensusPollStatus, ConsensusStatus};
use crate::bft::message::{ConsensusMessage, PBFTMessage, ViewChangeMessage};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::Info;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::proposer::Proposer;
use crate::bft::sync::{Synchronizer, SynchronizerPollStatus, SynchronizerStatus};

// The types responsible for this protocol
pub type PBFT<D> = System<D, PBFTConsensus<D>>;
// The message type for this consensus protocol
pub type SysMsg<D> = <PBFT<D> as Serializable>::Message;

static INITIALIZED: Flag = Flag::new();

/// Configure the init process of the library.
pub struct InitConfig {
    /// Number of threads used by the async runtime.
    pub async_threads: usize,
    /// Number of threads used by the thread pool.
    pub threadpool_threads: usize,

    ///Unique ID, used to specify the log file this replica should use
    pub id: Option<String>,
}

/// Handle to the global data.
///
/// When dropped, the data is deinitialized.
#[repr(transparent)]
pub struct InitGuard;

/// Initializes global data.
///
/// Should always be called before other methods, otherwise runtime
/// panics may ensue.
pub unsafe fn init(c: InitConfig) -> Result<Option<InitGuard>> {
    if INITIALIZED.test() {
        return Ok(None);
    }

    let path = match c.id {
        Some(id) => {
            format!("./log/febft_{}.log", id)
        }
        None => {
            String::from("./log/febft.log")
        }
    };

    let appender = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
        .build(path)
        .wrapped(ErrorKind::MsgLog)?;

    let config = Config::builder()
        .appender(Appender::builder().build("appender", Box::new(appender)))
        .logger(
            Logger::builder()
                .appender("appender")
                .additive(false)
                .build("app::appender", LevelFilter::Debug),
        )
        .build(
            Root::builder()
                .appender("appender")
                .build(LevelFilter::Debug),
        )
        .wrapped(ErrorKind::MsgLog)?;

    let _handle = log4rs::init_config(config).wrapped(ErrorKind::MsgLog)?;

    //tracing_subscriber::fmt::init();

    threadpool::init(c.threadpool_threads)?;
    async_runtime::init(c.async_threads)?;

    debug!("Async threads {}, sync threads {}", c.async_threads, c.threadpool_threads);

    socket::init()?;
    INITIALIZED.set();
    Ok(Some(InitGuard))
}

impl Drop for InitGuard {
    fn drop(&mut self) {
        unsafe { drop().unwrap() }
    }
}

unsafe fn drop() -> Result<()> {
    INITIALIZED.unset();
    threadpool::drop()?;
    async_runtime::drop()?;
    socket::drop()?;
    Ok(())
}

/// Which phase of the consensus algorithm are we currently executing
pub enum ConsensusPhase {
    // The normal phase of the consensus
    NormalPhase,
    // The synchronizer phase of the consensus
    SyncPhase,
}

/// The result of advancing the sync phase
pub enum SyncPhaseRes {
    SyncProtocolNotNeeded,
    RunSyncProtocol,
    SyncProtocolFinished,
    RunCSTProtocol,
}

/*
/// a PBFT based ordering protocol
pub struct PBFTOrderProtocol<D: SharedData + 'static, NT: Node<PBFT<D>> + 'static> {
    // What phase of the consensus algorithm are we currently executing
    phase: ConsensusPhase,

    /// The consensus state machine
    consensus: Consensus<D>,
    /// The synchronizer state machine
    synchronizer: Arc<Synchronizer<D>>,

    //The guard for the consensus.
    //Set to true when there is a consensus running, false when it's ready to receive
    //A new pre-prepare message
    consensus_guard: ConsensusGuard,
    // Check if unordered requests can be proposed.
    // This can only occur when we are in the normal phase of the state machine
    unordered_rq_guard: Arc<AtomicBool>,

    // The pending request log. Handles requests received by this replica
    // Or forwarded by others that have not yet made it into a consensus instance
    pending_request_log: Arc<PendingRequestLog<D>>,
    // The log of the decided consensus messages
    // This is completely owned by the server thread and therefore does not
    // Require any synchronization
    decided_log: DecidedLog<D>,
    // The proposer of this replica
    proposer: Arc<Proposer<D, NT>>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
}

impl<D, NT> OrderingProtocolImpl<PBFTConsensus<D>> for PBFTOrderProtocol<D, NT>
    where D: SharedData + 'static,
          NT: Node<PBFT<D>> + 'static {
    fn poll(&mut self) -> OrderProtocolPoll<PBFTMessage<D::State, D::Request>> {
        match self.phase {
            ConsensusPhase::NormalPhase => {
                self.poll_normal_phase()
            }
            ConsensusPhase::SyncPhase => {
                self.poll_sync_phase()
            }
        }
    }

    fn process_message(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::State, D::Request>>>) -> Result<OrderProtocolExecResult> {
        match self.phase {
            ConsensusPhase::NormalPhase => {
                Ok(self.update_normal_phase(message))
            }
            ConsensusPhase::SyncPhase => {
                Ok(self.update_sync_phase(message))
            }
        }
    }
}


impl<D, NT> PBFTOrderProtocol<D, NT> where D: SharedData + 'static,
                                           NT: Node<PBFT<D>> + 'static {

    fn poll_sync_phase(&mut self) -> OrderProtocolPoll<PBFTMessage<D::State, D::Request>> {
        // retrieve a view change message to be processed
        match self.synchronizer.poll() {
            SynchronizerPollStatus::Recv => OrderProtocolPoll::ReceiveFromReplicas,
            SynchronizerPollStatus::NextMessage(h, m) => {
                OrderProtocolPoll::Exec(StoredMessage::new(h, Protocol::new(PBFTMessage::ViewChange(m))))
            }
            SynchronizerPollStatus::ResumeViewChange => {
                self.synchronizer.resume_view_change(
                    &mut self.decided_log,
                    &self.timeouts,
                    &mut self.consensus,
                    &*self.node,
                );

                self.switch_phase(ConsensusPhase::NormalPhase);

                OrderProtocolPoll::RePoll
            }
        }
    }

    fn poll_normal_phase(&mut self) -> OrderProtocolPoll<PBFTMessage<D::State, D::Request>> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        if self.synchronizer.can_process_stops() {
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
                    unreachable!()
                }
            }
        }

        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let polled_message = self.consensus.poll(&self.pending_request_log);

        match polled_message {
            ConsensusPollStatus::Recv => OrderProtocolPoll::ReceiveFromReplicas,
            ConsensusPollStatus::NextMessage(h, m) => {
                OrderProtocolPoll::Exec(StoredMessage::new(h, Protocol::new(PBFTMessage::Consensus(m))))
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                self.consensus.advance_init_phase();

                //Receive the PrePrepare message from the client rq handler thread
                OrderProtocolPoll::ReceiveFromReplicas
            }
        }
    }

    fn update_sync_phase(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::State, D::Request>>>) -> OrderProtocolExecResult {
        let (header, protocol) = message.into_inner();

        match protocol.into_inner() {
            PBFTMessage::ViewChange(view_change) => {
                return match self.adv_sync(header, view_change) {
                    SyncPhaseRes::SyncProtocolNotNeeded => {
                        OrderProtocolExecResult::Success
                    }
                    SyncPhaseRes::RunSyncProtocol => {
                        OrderProtocolExecResult::Success
                    }
                    SyncPhaseRes::SyncProtocolFinished => {
                        OrderProtocolExecResult::Success
                    }
                    SyncPhaseRes::RunCSTProtocol => {
                        OrderProtocolExecResult::RunCst
                    }
                };
            }
            PBFTMessage::Consensus(message) => {
                self.consensus.queue(header, message);
            }
            PBFTMessage::Cst(message) => {
                self.process_off_context_cst_msg(header, message)?;
            }
            _ => {}
        }

        OrderProtocolExecResult::Success
    }

    fn update_normal_phase(&mut self, message: StoredMessage<Protocol<PBFTMessage<D::State, D::Request>>>) -> OrderProtocolExecResult {
        let (header, protocol) = message.into_inner();

        match protocol.into_inner() {
            PBFTMessage::Consensus(message) => {
                self.adv_consensus(header, message)?;
            }
            PBFTMessage::ViewChange(view_change) => {
                let status = self.synchronizer.process_message(
                    header,
                    view_change,
                    &self.timeouts,
                    &mut self.decided_log,
                    &self.pending_request_log,
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
            PBFTMessage::Cst(message) => {
                self.process_off_context_cst_msg(header, message)?;
            }
            _ => {}
        }

        OrderProtocolExecResult::Success
    }

    /// Advance the consensus phase with a received message
    fn adv_consensus(
        &mut self,
        header: Header,
        message: ConsensusMessage<D::Request>,
    ) -> OrderProtocolExecResult {
        let seq = self.consensus.sequence_number();

        // debug!(
        //     "{:?} // Processing consensus message {:?} ",
        //     self.id(),
        //     message
        // );

        // let start = Instant::now();

        let status = self.consensus.process_message(
            header,
            message,
            &self.synchronizer,
            &self.timeouts,
            &mut self.decided_log,
            &*self.node,
        );

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
            ConsensusStatus::Decided(batch_digest) => {
                if let Some(exec_info) =
                    //Should the execution be scheduled here or will it be scheduled by the persistent log?
                    self.decided_log.finalize_batch(seq, batch_digest)? {
                    let (info, batch, completed_batch) = exec_info.into();

                    match info {
                        Info::Nil => self.executor.queue_update(batch),
                        // execute and begin local checkpoint
                        Info::BeginCheckpoint => {
                            self.executor.queue_update_and_get_appstate(batch)
                        }
                    }.unwrap();
                }

                self.consensus.next_instance();
            }
        }

        // we processed a consensus message,
        // signal the consensus layer of this event
        self.consensus.signal();

        //
        // debug!(
        //     "{:?} // Done processing consensus message. Took {:?}",
        //     self.id(),
        //     Instant::now().duration_since(start)
        // );


        // yield execution since `signal()`
        // will probably force a value from the
        // TBO queue in the consensus layer
        // std::hint::spin_loop();
        OrderProtocolExecResult::Success
    }


    /// Advance the sync phase of the algorithm
    fn adv_sync(&mut self, header: Header,
                message: ViewChangeMessage<D::Request>) -> SyncPhaseRes {
        let status = self.synchronizer.process_message(
            header,
            message,
            &self.timeouts,
            &mut self.decided_log,
            &self.pending_request_log,
            &mut self.consensus,
            &*self.node,
        );

        self.synchronizer.signal();

        match status {
            SynchronizerStatus::Nil => SyncPhaseRes::SyncProtocolNotNeeded,
            SynchronizerStatus::Running => (),
            SynchronizerStatus::NewView => {
                //Our current view has been updated and we have no more state operations
                //to perform. This happens if we are a correct replica and therefore do not need
                //To update our state or if we are a replica that was incorrect and whose state has
                //Already been updated from the Cst protocol
                self.switch_phase(ConsensusPhase::NormalPhase);

                SyncPhaseRes::SyncProtocolFinished
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
        }

        SyncPhaseRes::RunSyncProtocol
    }
}*/
