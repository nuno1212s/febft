use log::{debug, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::message::{ConsensusMessage, Header, Message, SystemMessage};
use crate::bft::communication::{Node, NodeConfig, NodeId};
use crate::bft::consensus::{Consensus, ConsensusPollStatus, ConsensusStatus};
use crate::bft::core::server::client_replier::Replier;
use crate::bft::core::server::ReplicaPhase;
use crate::bft::cst::{install_recovery_state, CollabStateTransfer, CstProgress, CstStatus};
use crate::bft::error::*;
use crate::bft::executable::{Executor, ExecutorHandle, FollowerReplier, Request, Service, State};
use crate::bft::msg_log;
use crate::bft::msg_log::{Info};
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::proposer::follower_proposer::FollowerProposer;
use crate::bft::sync::{
    AbstractSynchronizer, Synchronizer, SynchronizerPollStatus, SynchronizerStatus,
};
use crate::bft::sync::view::ViewInfo;
use crate::bft::timeouts::{Timeout, TimeoutKind, Timeouts};

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum FollowerPhase {
    //Normal phase of follower. Up to date on the current state of the quorum
    // and actively listening for new finished quorums
    NormalPhase,
    // Retrieving the current state of the quorum. Might require transferring
    // the state if we are very far behind or only require a log transmission
    RetrievingStatePhase,
    // the replica has entered the
    // synchronization phase of mod-smart
    SyncPhase,
}

///A follower does not participate in the quorum, but he passively listens
/// to the quorum decisions and executes them locally
///
/// A follower cannot perform non unordered requests as it is not a part of the
/// quorum so it can mostly serve reads.
///
/// This does however mean that we can scale horizontally in read processing with eventual
/// consistency, as well as serve as a "backup" to the quorum
///
/// They might also be used to loosen the load on the quorum replicas when we need a
/// State transfer as they can request the last checkpoint from these replicas.
pub struct Follower<S: Service + 'static> {
    //The current phase of the follower
    phase: FollowerPhase,
    phase_stack: Option<FollowerPhase>,

    //The handle to the current state and the executor of the service, so we
    //can keep up and respond to requests
    executor: ExecutorHandle<S>,
    //A consensus instance for the followers
    consensus: Consensus<S>,
    //
    cst: CollabStateTransfer<S>,
    //These timeouts are only used for the CST protocol,
    //As it's the only place where we are expected to send messages to
    //Other replicas
    timeouts: Timeouts,
    //The proposer, which in this case wil
    proposer: Arc<FollowerProposer<S>>,
    //Synchronizer observer
    synchronizer: Arc<Synchronizer<S>>,

    //The log of messages
    decided_log: DecidedLog<S>,

    pending_rq_log: Arc<PendingRequestLog<S>>,

    node: Arc<Node<S::Data>>,
}

pub struct FollowerConfig<S: Service, T: PersistentLogModeTrait> {
    pub service: S,

    pub log_mode: T,

    pub global_batch_size: usize,
    pub batch_timeout: u128,
    pub node: NodeConfig,
}

impl<S: Service + 'static> Follower<S> {
    pub async fn new<T>(cfg: FollowerConfig<S, T>) -> Result<Self> where T: PersistentLogModeTrait {
        let FollowerConfig {
            service,
            log_mode: _,
            global_batch_size,
            batch_timeout,
            node: node_config,
        } = cfg;

        let log_node_id = node_config.id.clone();
        let n = node_config.n;
        let f = node_config.f;

        let db_path = node_config.db_path.clone();

        let (node, _rogue) = Node::bootstrap(node_config).await?;

        let (executor, handle) = Executor::<S, FollowerReplier>::init_handle();

        debug!("Initializing log");
        let persistent_log = msg_log::initialize_persistent_log::<S, String, T>(executor.clone(), db_path)?;

        let mut decided_log = msg_log::initialize_decided_log(persistent_log.clone())?;

        let pending_request_log = Arc::new(msg_log::initialize_pending_request_log()?);

        let seq;

        let view;

        //Read the state from the persistent log
        let state = if let Some(read_state) = decided_log.read_current_state(n, f)? {
            let last_seq = if let Some(seq) = read_state.decision_log().last_execution() {
                seq
            } else {
                read_state.checkpoint().sequence_number()
            };

            seq = last_seq;

            view = read_state.view().clone();

            let executed_requests = read_state.requests.clone();

            let state = read_state.checkpoint().state().clone();

            decided_log.install_state(last_seq, read_state);

            Some((state, executed_requests))
        } else {
            seq = SeqNo::ZERO;

            view = ViewInfo::new(SeqNo::ZERO, n, f)?;

            None
        };

        let reply_handle = Replier::new(node.id(), node.send_node());

        // start executor
        Executor::<S, FollowerReplier>::new(
            reply_handle,
            handle,
            service,
            state,
            node.send_node(),
            None,
        )?;

        let consensus = Consensus::new_follower(node.id(),
                                                seq,
                                                executor.clone());

        const CST_BASE_DUR: Duration = Duration::from_secs(30);

        let cst = CollabStateTransfer::new(CST_BASE_DUR);

        let synchronizer = Synchronizer::new_follower(view);

        let follower_proposer = FollowerProposer::new(
            node.clone(),
            pending_request_log.clone(),
            executor.clone(),
            global_batch_size,
            batch_timeout,
        );

        let timeouts = Timeouts::new::<S>(500, Arc::clone(node.loopback_channel()));

        Ok(Self {
            phase: FollowerPhase::NormalPhase,
            executor,
            cst,
            consensus,
            synchronizer,
            proposer: follower_proposer,
            node,
            timeouts,
            phase_stack: None,
            decided_log,
            pending_rq_log: pending_request_log,
        })
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            match self.phase {
                FollowerPhase::NormalPhase => todo!(),
                FollowerPhase::RetrievingStatePhase => todo!(),
                FollowerPhase::SyncPhase => todo!(),
            }
        }
    }

    fn switch_phase(&mut self, phase: FollowerPhase) {
        self.phase = phase;
    }

    fn update_normal_phase(&mut self) -> Result<()> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        if self.synchronizer.can_process_stops() {
            let running = self.update_sync_phase()?;

            if running {
                self.switch_phase(FollowerPhase::SyncPhase);

                return Ok(());
            }
        }

        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let polled_message = self.consensus.poll(&self.pending_rq_log);

        let _leader = self.synchronizer.view().leader() == self.id();

        let message = match polled_message {
            ConsensusPollStatus::Recv => self.node.receive_from_replicas()?,
            ConsensusPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::Consensus(m))
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                self.consensus.advance_init_phase();

                //Receive the PrePrepare message from the client rq handler thread
                let replicas = self.node.receive_from_replicas()?;

                replicas
            }
        };

        debug!("{:?} // Processing message {:?}", self.id(), message);

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::Request(_) | SystemMessage::ForwardedRequests(_) => {
                        //Followers do not accept ordered requests
                        warn!("Received ordered request while follower, cannot process so ignoring.");
                    }
                    SystemMessage::UnOrderedRequest(_) => {
                        warn!("Unordered requests should be delivered straight to the executor.")
                    }
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.decided_log,
                            &mut self.node,
                        );
                        match status {
                            CstStatus::Nil => (),
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    SystemMessage::ViewChange(message) => {
                        let status = self.synchronizer.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &mut self.decided_log,
                            &self.pending_rq_log,
                            &mut self.consensus,
                            &self.node,
                        );

                        self.synchronizer.signal();

                        match status {
                            SynchronizerStatus::Nil => (),
                            SynchronizerStatus::Running => {
                                self.switch_phase(FollowerPhase::SyncPhase)
                            }
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    SystemMessage::Consensus(message) => {
                        self.adv_consensus(header, message)?;
                    }
                    SystemMessage::FwdConsensus(message) => {
                        let (header, message) = message.into_inner();

                        self.adv_consensus(header, message)?;
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::Ping(_) => {}
                }
            }
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            }
            Message::ExecutionFinishedWithAppstate((seq, appstate)) => {
                self.execution_finished_with_appstate(seq, appstate)?;
            }
        }

        Ok(())
    }

    /// Iterate the synchronous phase
    fn update_sync_phase(&mut self) -> Result<bool> {
        // retrieve a view change message to be processed
        let message = match self.synchronizer.poll() {
            SynchronizerPollStatus::Recv => self.node.receive_from_replicas()?,
            SynchronizerPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::ViewChange(m))
            }
            SynchronizerPollStatus::ResumeViewChange => {
                self.synchronizer.resume_view_change(
                    &mut self.decided_log,
                    &self.timeouts,
                    &mut self.consensus,
                    &self.node,
                );

                self.switch_phase(FollowerPhase::NormalPhase);

                return Ok(false);
            }
        };

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(_) | SystemMessage::Request(_) => {
                        //Followers cannot process ordered requests
                        warn!("Received ordered request while follower, cannot process so ignoring.");
                    }
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    }
                    SystemMessage::FwdConsensus(fwdConsensus) => {
                        let (h, m) = fwdConsensus.into_inner();

                        //TODO: Verify signature of replica

                        self.consensus.queue(h, m);
                    }
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.decided_log,
                            &mut self.node,
                        );
                        match status {
                            CstStatus::Nil => (),
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    SystemMessage::ViewChange(message) => {
                        let status = self.synchronizer.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &mut self.decided_log,
                            &self.pending_rq_log,
                            &mut self.consensus,
                            &mut self.node,
                        );

                        self.synchronizer.signal();

                        match status {
                            SynchronizerStatus::Nil => return Ok(false),
                            SynchronizerStatus::Running => (),
                            SynchronizerStatus::NewView => {
                                //Our current view has been updated and we have no more state operations
                                //to perform. This happens if we are a correct replica and therefore do not need
                                //To update our state or if we are a replica that was incorrect and whose state has
                                //Already been updated from the Cst protocol
                                self.switch_phase(FollowerPhase::NormalPhase);

                                return Ok(false);
                            }
                            SynchronizerStatus::RunCst => {
                                //This happens when a new view is being introduced and we are not up to date
                                //With the rest of the replicas. This might happen because the replica was faulty
                                //or any other reason that might cause it to lose some updates from the other replicas
                                self.switch_phase(FollowerPhase::RetrievingStatePhase);

                                //After we update the state, we go back to the sync phase (this phase) so we can check if we are missing
                                //Anything or to finalize and go back to the normal phase
                                self.phase_stack = Some(FollowerPhase::SyncPhase);
                            }
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => {
                        warn!("Received request while synchronizing, ignoring.")
                    }
                    SystemMessage::Ping(_) => {}
                }
            }
            //////// XXX XXX XXX XXX
            //
            // TODO: check if simply copying the behavior over from the
            // normal phase is correct here
            //
            //
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            }
            Message::ExecutionFinishedWithAppstate((seq, appstate)) => {
                self.execution_finished_with_appstate(seq, appstate)?;
            }
        }

        Ok(true)
    }

    fn update_retrieving_phase(&mut self) -> Result<()> {
        debug!("{:?} // Retrieving state...", self.id());
        let message = self.node.receive_from_replicas().unwrap();

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(_) | SystemMessage::Request(_) => {
                        //Followers cannot execute ordered requests
                    }
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    }
                    SystemMessage::FwdConsensus(fwdConsensus) => {
                        let (h, m) = fwdConsensus.into_inner();

                        //TODO: Check signature

                        self.consensus.queue(h, m);
                    }
                    SystemMessage::ViewChange(message) => {
                        self.synchronizer.queue(header, message);
                    }
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.decided_log,
                            &self.node,
                        );
                        match status {
                            CstStatus::Running => (),
                            CstStatus::State(state) => {
                                install_recovery_state(
                                    state,
                                    &self.synchronizer,
                                    &mut self.decided_log,
                                    &mut self.executor,
                                    &mut self.consensus,
                                )?;

                                let next_phase = self
                                    .phase_stack
                                    .take()
                                    .unwrap_or(FollowerPhase::NormalPhase);

                                self.switch_phase(next_phase);
                            }
                            CstStatus::SeqNo(seq) => {
                                if self.consensus.sequence_number() < seq {
                                    // this step will allow us to ignore any messages
                                    // for older consensus instances we may have had stored;
                                    //
                                    // after we receive the latest recovery state, we
                                    // need to install the then latest sequence no;
                                    // this is done with the function
                                    // `install_recovery_state` from cst
                                    self.consensus.install_sequence_number(seq);

                                    self.cst.request_latest_state(
                                        &self.synchronizer,
                                        &self.timeouts,
                                        &self.node,
                                    );
                                } else {
                                    self.switch_phase(FollowerPhase::NormalPhase);
                                }
                            }
                            CstStatus::RequestLatestCid => {
                                self.cst.request_latest_consensus_seq_no(
                                    &self.synchronizer,
                                    &self.timeouts,
                                    &self.node,
                                );
                            }
                            CstStatus::RequestState => {
                                self.cst.request_latest_state(
                                    &self.synchronizer,
                                    &self.timeouts,
                                    &mut self.node,
                                );
                            }
                            // should not happen...
                            CstStatus::Nil => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    // FIXME: handle rogue reply messages
                    // Should never
                    SystemMessage::Reply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => {
                        warn!("Rogue unordered request message detected")
                    }
                    SystemMessage::UnOrderedReply(_) => {
                        warn!("How can I receive a reply here?")
                    }
                    SystemMessage::Ping(_) => {}
                }
            }
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            }
            Message::ExecutionFinishedWithAppstate(_) => {
                // TODO: verify if ignoring the checkpoint state while
                // receiving state from peer nodes is correct
            }
        }

        Ok(())
    }

    fn adv_consensus(
        &mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
    ) -> Result<()> {
        let seq = self.consensus.sequence_number();

        debug!(
            "{:?} // Processing consensus message {:?} ",
            self.id(),
            message
        );

        let start = Instant::now();

        let status = self.consensus.process_message(
            header,
            message,
            &self.synchronizer,
            &self.timeouts,
            &mut self.decided_log,
            &self.node,
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
            ConsensusStatus::Decided(completed_batch) => {
                if let Some(batch) =
                    self.decided_log
                        .finalize_batch(seq, completed_batch)?
                {
                    let (info, batch, _) = batch.into();

                    //Send the finalized batch to the rq finalizer
                    //So everything can be removed from the correct logs and
                    //Given to the service thread to execute
                    //self.rq_finalizer.queue_finalize(info, meta, rqs);
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

        debug!(
            "{:?} // Done processing consensus message. Took {:?}",
            self.id(),
            Instant::now().duration_since(start)
        );

        // yield execution since `signal()`
        // will probably force a value from the
        // TBO queue in the consensus layer
        // std::hint::spin_loop();
        Ok(())
    }

    ///Receive a state delivered by the execution layer.
    /// Also must receive the sequence number of the last consensus instance executed in that state.
    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: State<S>) -> Result<()> {
        self.decided_log.finalize_checkpoint(seq, appstate)?;

        if self.cst.needs_checkpoint() {
            // status should return CstStatus::Nil,
            // which does not need to be handled
            let _status = self.cst.process_message(
                CstProgress::Nil,
                &self.synchronizer,
                &self.consensus,
                &self.decided_log,
                &mut self.node,
            );
        }

        Ok(())
    }

    fn timeout_received(&mut self, timeouts: Timeout) {
        for timeout_kind in timeouts {
            match timeout_kind {
                TimeoutKind::Cst(cst_seq) => {
                    if self.cst.cst_request_timed_out(cst_seq,
                                                      &self.synchronizer,
                                                      &self.timeouts,
                                                      &self.node) {

                        self.switch_phase(FollowerPhase::RetrievingStatePhase);

                    }
                }
                TimeoutKind::ClientRequestTimeout(_timeout_seq) => {
                    //Followers never time out client requests (They don't even receive ordered requests)
                    unreachable!();
                }
            }
        }
    }
}
