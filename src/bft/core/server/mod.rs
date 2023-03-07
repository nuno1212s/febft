//! Contains the server side core protocol logic of `febft`.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::offset::Utc;
use chrono::DateTime;
use log::{debug, warn};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::message::{ConsensusMessage, CstMessage, ForwardedRequestsMessage, Header, Message, ObserveEventKind, RequestMessage, StoredMessage, SystemMessage, ViewChangeMessage};
use crate::bft::communication::{Node, NodeConfig, NodeId};
use crate::bft::consensus::{Consensus, ConsensusGuard, ConsensusPollStatus, ConsensusStatus};
use crate::bft::core::server::client_replier::Replier;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::cst::{install_recovery_state, CollabStateTransfer, CstProgress, CstStatus};
use crate::bft::error::*;
use crate::bft::executable::{
    Executor, ExecutorHandle, ReplicaReplier, Reply, Request, Service, State,
};
use crate::bft::msg_log;
use crate::bft::msg_log::{Info};
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::proposer::Proposer;
use crate::bft::sync::{
    AbstractSynchronizer, Synchronizer, SynchronizerPollStatus, SynchronizerStatus,
};
use crate::bft::sync::view::ViewInfo;
use crate::bft::timeouts::{Timeout, TimeoutKind, Timeouts};

use super::SystemParams;

pub mod observer;

pub mod client_replier;
pub mod follower_handling;
// pub mod rq_finalizer;

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum ReplicaPhase {
    // the replica is retrieving state from
    // its peer replicas
    RetrievingState,
    // the replica has entered the
    // synchronization phase of mod-smart
    SyncPhase,
    // the replica is on the normal phase
    // of mod-smart
    NormalPhase,
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service + 'static> {
    // What phase of the algorithm is this replica currently in
    phase: ReplicaPhase,
    // this value is primarily used to switch from
    // state transfer back to a view change
    phase_stack: Option<ReplicaPhase>,
    // The timeout layer, handles timing out requests
    timeouts: Timeouts,
    // The handle to the executor
    executor: ExecutorHandle<S>,
    // Consensus state transfer State machine
    cst: CollabStateTransfer<S>,
    // Synchronizer state machine
    synchronizer: Arc<Synchronizer<S>>,
    //Consensus state machine
    consensus: Consensus<S>,
    //The guard for the consensus.
    //Set to true when there is a consensus running, false when it's ready to receive
    //A new pre-prepare message
    consensus_guard: ConsensusGuard,
    // Check if unordered requests can be proposed.
    // This can only occur when we are in the normal phase of the state machine
    unordered_rq_guard: Arc<AtomicBool>,

    // The pending request log. Handles requests received by this replica
    // Or forwarded by others that have not yet made it into a consensus instance
    pending_request_log: Arc<PendingRequestLog<S>>,
    // The log of the decided consensus messages
    // This is completely owned by the server thread and therefore does not
    // Require any synchronization
    decided_log: DecidedLog<S>,
    // The proposer of this replica
    proposer: Arc<Proposer<S>>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<Node<S::Data>>,
    //A handle to the observer worker thread
    observer_handle: ObserverHandle,
}

/// Represents a configuration used to bootstrap a `Replica`.
// TODO: load files from persistent storage
pub struct ReplicaConfig<S: Service, T: PersistentLogModeTrait> {
    /// The application logic.
    pub service: S,

    //TODO: These two values should be loaded from storage
    /// The sequence number for the current view.
    pub view: SeqNo,
    /// Next sequence number attributed to a request by
    /// the consensus layer.
    pub next_consensus_seq: SeqNo,
    ///The targetted global batch size
    pub global_batch_size: usize,
    ///The time limit for creating that batch, in micro seconds
    pub batch_timeout: u128,
    pub max_batch_size: usize,

    ///The logging mode
    pub log_mode: PhantomData<T>,
    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}

impl<S> Replica<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    /// Bootstrap a replica in `febft`.
    pub async fn bootstrap<T>(cfg: ReplicaConfig<S, T>) -> Result<Self> where T: PersistentLogModeTrait {
        let ReplicaConfig {
            next_consensus_seq,
            global_batch_size,
            batch_timeout,
            max_batch_size,
            node: node_config,
            service,
            view: _,
            log_mode: _,
        } = cfg;

        let per_pool_batch_timeout = node_config.batch_timeout_micros;
        let per_pool_batch_sleep = node_config.batch_sleep_micros;
        let per_pool_batch_size = node_config.batch_size;

        let log_node_id = node_config.id.clone();
        let n = node_config.n;
        let f = node_config.f;

        let db_path = node_config.db_path.clone();

        debug!("Bootstrapping replica, starting with networking");

        let (node, rogue) = Node::bootstrap(node_config).await?;

        // connect to peer nodes
        let observer_handle = observer::start_observers(node.send_node());

        //CURRENTLY DISABLED, USING THREADPOOL INSTEAD
        let reply_handle = Replier::new(node.id(), node.send_node());

        let (executor, handle) = Executor::<S, ReplicaReplier>::init_handle();

        debug!("Initializing log");
        let persistent_log = msg_log::initialize_persistent_log::<S, String, T>(executor.clone(), db_path)?;

        let mut decided_log = msg_log::initialize_decided_log(persistent_log.clone())?;

        let pending_request_log = Arc::new(msg_log::initialize_pending_request_log()?);

        let seq;

        let view;

        debug!("Reading state from memory");
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

        debug!("Initializing executor.");

        // start executor
        Executor::<S, ReplicaReplier>::new(
            reply_handle,
            handle,
            service,
            state,
            node.send_node(),
            Some(observer_handle.clone()),
        )?;

        let node_clone = node.clone();

        debug!("Initializing timeouts");
        // start timeouts handler
        let timeouts = Timeouts::new::<S>(500, Arc::clone(node.loopback_channel()));

        // TODO:
        // - client req timeout base dur configure param
        // - cst timeout base dur configure param
        // - ask for latest cid when bootstrapping
        const CST_BASE_DUR: Duration = Duration::from_secs(30);
        const REQ_BASE_DUR: Duration = Duration::from_secs(2 * 60);

        let synchronizer = Synchronizer::new_replica(view.clone(), REQ_BASE_DUR);

        debug!("Initializing consensus");

        let consensus = Consensus::new_replica(
            node.id(),
            view.clone(),
            next_consensus_seq,
            executor.clone(),
            observer_handle.clone(),
            None,
        );

        //We can unwrap since it's guaranteed that this consensus is of a replica type
        let consensus_guard = consensus.consensus_guard().unwrap().clone();

        //Initialize replica data with the initialized variables from above
        let mut replica = Replica {
            phase: ReplicaPhase::NormalPhase,
            phase_stack: None,
            cst: CollabStateTransfer::new(CST_BASE_DUR),
            synchronizer: synchronizer.clone(),
            consensus,
            consensus_guard: consensus_guard.clone(),
            timeouts: timeouts.clone(),
            node,
            proposer: Proposer::new(
                node_clone,
                synchronizer,
                pending_request_log.clone(),
                timeouts,
                executor.clone(),
                consensus_guard,
                global_batch_size,
                batch_timeout,
                max_batch_size,

                observer_handle.clone(),
            ),
            executor,
            observer_handle: observer_handle.clone(),
            unordered_rq_guard: Arc::new(Default::default()),
            pending_request_log,
            decided_log,
        };

        //Start receiving and processing client requests
        replica.proposer.clone().start();

        // handle rogue messages
        for message in rogue {
            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Request(request) => {
                            replica.request_received(header, request);
                        }
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        }
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                        // FIXME: handle rogue cst messages
                        SystemMessage::Cst(_) => warn!("Rogue cst message detected"),
                        // FIXME: handle rogue view change messages
                        SystemMessage::ViewChange(_) => warn!("Rogue view change message detected"),
                        // FIXME: handle rogue forwarded requests messages
                        SystemMessage::ForwardedRequests(_) => {
                            warn!("Rogue forwarded requests message detected")
                        }
                        SystemMessage::ObserverMessage(_) => {
                            warn!("Rogue observer message detected")
                        }
                        SystemMessage::UnOrderedRequest(_) => {
                            warn!("Rogue unordered request message")
                        }
                        SystemMessage::FwdConsensus(_) => {
                            warn!("Rogue fwd consensus message observed.")
                        }
                        SystemMessage::Ping(_) => {}
                    }
                }
                // ignore other messages for now
                _ => (),
            }
        }

        debug!("{:?} // Started replica.", replica.id());

        debug!(
            "{:?} // Per Pool Batch size: {}",
            replica.id(),
            per_pool_batch_size
        );
        debug!(
            "{:?} // Per pool batch sleep: {}",
            replica.id(),
            per_pool_batch_sleep
        );
        debug!(
            "{:?} // Per pool batch timeout: {}",
            replica.id(),
            per_pool_batch_timeout
        );

        debug!(
            "{:?} // Global batch size: {}",
            replica.id(),
            global_batch_size
        );

        debug!(
            "{:?} // Global batch timeout: {}",
            replica.id(),
            batch_timeout
        );

        println!("{:?} // Leader count: {} ({:?})",
                 replica.id(),
                 replica.synchronizer.view().leader_set().len(),
                 replica.synchronizer.view().leader_set());


        Ok(replica)
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// The main loop of a replica.
    pub fn run(&mut self) -> Result<()> {
        // TODO: exit condition?
        loop {
            match self.phase {
                ReplicaPhase::RetrievingState => self.update_retrieving_state()?,
                ReplicaPhase::NormalPhase => self.update_normal_phase()?,
                ReplicaPhase::SyncPhase => self.update_sync_phase().map(|_| ())?,
            }
        }
    }

    pub(crate) fn switch_phase(&mut self, new_phase: ReplicaPhase) {
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
                (ReplicaPhase::NormalPhase, _) => {
                    //We want to stop the proposer from trying to propose any requests while we are performing
                    //Other operations.
                    self.consensus_guard.lock_consensus();
                }
                (ReplicaPhase::SyncPhase, ReplicaPhase::NormalPhase) => {
                    // When changing from the sync phase to the normal phase
                    // The phase starts with a SYNC phase, so we don't want to allow
                    // The proposer to propose anything
                    self.consensus_guard.lock_consensus();
                }
                (_, ReplicaPhase::NormalPhase) => {
                    //Mark the consensus as available, since we are changing to the normal phase
                    //And are therefore ready to receive pre-prepares (if are are the leaders)
                    self.consensus_guard.unlock_consensus();
                }
                (_, _) => {}
            }

            /*
            Observe event stuff
            @{
             */
            let to_send = match (&old_phase, &self.phase) {
                (_, ReplicaPhase::RetrievingState) => ObserveEventKind::CollabStateTransfer,
                (_, ReplicaPhase::SyncPhase) => ObserveEventKind::ViewChangePhase,
                (_, ReplicaPhase::NormalPhase) => {
                    let current_view = self.synchronizer.view();

                    let current_seq = self.consensus.sequence_number();

                    ObserveEventKind::NormalPhase((current_view, current_seq))
                }
            };

            self.observer_handle
                .tx()
                .send(MessageType::Event(to_send))
                .expect("Failed to notify observer thread");

            /*
            }@
            */
        }
    }

    fn update_retrieving_state(&mut self) -> Result<()> {
        debug!("{:?} // Retrieving state...", self.id());
        let message = self.node.receive_from_replicas().unwrap();

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(requests) => {
                        // FIXME: is this the correct behavior? to save forwarded requests
                        // while we are retrieving state...
                        self.forwarded_requests_received(requests);
                    }
                    SystemMessage::Request(request) => {
                        self.request_received(header, request);
                    }
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    }
                    SystemMessage::FwdConsensus(_) => {
                        //Replicas do not receive forwarded consensus messages.
                    }
                    SystemMessage::ViewChange(message) => {
                        self.synchronizer.queue(header, message);
                    }
                    SystemMessage::Cst(message) => {
                        self.adv_cst(header, message)?;
                    }
                    // FIXME: handle rogue reply messages
                    // Should never
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => {
                        warn!("Rogue unordered request message detected")
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

    /// Iterate the synchronizer state machine
    fn update_sync_phase(&mut self) -> Result<bool> {
        debug!("{:?} // Updating Sync phase", self.id());

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

                self.switch_phase(ReplicaPhase::NormalPhase);
                return Ok(false);
            }
        };

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    }
                    SystemMessage::FwdConsensus(_) => {
                        //Live replicas don't accept forwarded consensus messages
                        // These messages are destined for followers
                    }
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    }
                    SystemMessage::Request(request) => {
                        //How did this get here?
                        self.request_received(header, request);
                    }
                    SystemMessage::Cst(message) => {
                        self.process_off_context_cst_msg(header, message)?;
                    }
                    SystemMessage::ViewChange(message) => {
                        return self.adv_sync(header, message);
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => todo!(),
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

    fn update_normal_phase(&mut self) -> Result<()> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        if self.synchronizer.can_process_stops() {
            let running = self.update_sync_phase()?;
            if running {
                self.switch_phase(ReplicaPhase::SyncPhase);

                return Ok(());
            }
        }

        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let polled_message = self.consensus.poll(&self.pending_request_log);

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

        // debug!("{:?} // Processing message {:?}", self.id(), message);

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    }
                    SystemMessage::Request(request) => {
                        self.request_received(header, request);
                    }
                    SystemMessage::Cst(message) => {
                        self.process_off_context_cst_msg(header, message)?;
                    }
                    SystemMessage::ViewChange(message) => {
                        let status = self.synchronizer.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &mut self.decided_log,
                            &self.pending_request_log,
                            &mut self.consensus,
                            &self.node,
                        );

                        self.synchronizer.signal();

                        match status {
                            SynchronizerStatus::Nil => (),
                            SynchronizerStatus::Running => {
                                self.switch_phase(ReplicaPhase::SyncPhase)
                            }
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer);
                            }
                        }
                    }
                    SystemMessage::Consensus(message) => {
                        self.adv_consensus(header, message)?;
                    }
                    SystemMessage::FwdConsensus(_message) => {
                        warn!("Replicas cannot process forwarded consensus messages! They must receive the preprepare messages straight from leaders!");
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => todo!(),
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

    /// Advance the consensus phase with a received message
    fn adv_consensus(
        &mut self,
        header: Header,
        message: ConsensusMessage<Request<S>>,
    ) -> Result<()> {
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
        Ok(())
    }

    /// Advance the sync phase of the algorithm
    fn adv_sync(&mut self, header: Header,
                message: ViewChangeMessage<Request<S>>) -> Result<bool> {
        let status = self.synchronizer.process_message(
            header,
            message,
            &self.timeouts,
            &mut self.decided_log,
            &self.pending_request_log,
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
                self.switch_phase(ReplicaPhase::NormalPhase);

                return Ok(false);
            }
            SynchronizerStatus::RunCst => {
                //This happens when a new view is being introduced and we are not up to date
                //With the rest of the replicas. This might happen because the replica was faulty
                //or any other reason that might cause it to lose some updates from the other replicas
                self.switch_phase(ReplicaPhase::RetrievingState);

                //After we update the state, we go back to the sync phase (this phase) so we can check if we are missing
                //Anything or to finalize and go back to the normal phase
                self.phase_stack = Some(ReplicaPhase::SyncPhase);
            }
            // should not happen...
            _ => {
                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer);
            }
        }

        Ok(true)
    }

    /// Advance the consensus state transfer machine
    fn adv_cst(&mut self, header: Header, message: CstMessage<State<S>, Request<S>>) -> Result<()> {
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

                // If we were in the middle of performing a view change, then continue that
                // View change. If not, then proceed to the normal phase
                let next_phase =
                    self.phase_stack.take().unwrap_or(ReplicaPhase::NormalPhase);

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
                    self.switch_phase(ReplicaPhase::NormalPhase);
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

        Ok(())
    }

    /// Process a CST message that was received while we are executing another phase
    fn process_off_context_cst_msg(&mut self, header: Header, message: CstMessage<State<S>, Request<S>>) -> Result<()> {
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
                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer);
            }
        }

        Ok(())
    }

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

        // {@
        // Observer code
        // @}

        self.observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::CheckpointEnd(
                self.decided_log.decision_log().executing(),
            )))
            .unwrap();

        //
        // @}
        //

        Ok(())
    }

    fn forwarded_requests_received(&self, requests: ForwardedRequestsMessage<Request<S>>) {
        self.synchronizer
            .watch_forwarded_requests(requests, &self.timeouts, &self.pending_request_log);
    }

    fn timeout_received(&mut self, timeouts: Timeout) {
        let mut client_rq_timeouts = Vec::with_capacity(timeouts.len());

        for timeout_kind in timeouts {
            match timeout_kind {
                TimeoutKind::Cst(cst_seq) => {
                    if self.cst.cst_request_timed_out(cst_seq,
                                                      &self.synchronizer,
                                                      &self.timeouts,
                                                      &self.node) {
                        self.switch_phase(ReplicaPhase::RetrievingState);
                    }
                }
                TimeoutKind::ClientRequestTimeout(timeout_seq) => {
                    client_rq_timeouts.push(timeout_seq);
                }
            }
        }

        if client_rq_timeouts.len() > 0 {
            let status = self.synchronizer
                .client_requests_timed_out(&client_rq_timeouts);

            match status {
                SynchronizerStatus::RequestsTimedOut { forwarded, stopped } => {
                    if forwarded.len() > 0 {
                        let requests = self.pending_request_log.clone_pending_requests(&forwarded);

                        self.synchronizer.forward_requests(
                            requests,
                            &mut self.node,
                            &self.pending_request_log,
                        );
                    }

                    if stopped.len() > 0 {
                        let stopped = self.pending_request_log.clone_pending_requests(&stopped);

                        self.synchronizer.begin_view_change(Some(stopped),
                                                            &mut self.node,
                                                            &self.timeouts,
                                                            &self.decided_log);

                        self.phase = ReplicaPhase::SyncPhase;
                    }
                }
                // nothing to do
                _ => (),
            }
        }
    }

    fn requests_received(
        &self,
        t: DateTime<Utc>,
        reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    ) {
        // let mut batch_meta = self.log.batch_meta().lock();
        //
        // batch_meta.reception_time = t;
        // batch_meta.batch_size = reqs.len();
        //
        // drop(batch_meta);

        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, r)
        }
    }

    fn request_received(&self, h: Header, r: RequestMessage<Request<S>>) {
        self.synchronizer
            .watch_request(h.unique_digest(), &self.timeouts);

        self.pending_request_log.insert(h, r);
    }
}
