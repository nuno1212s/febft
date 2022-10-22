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
use crate::bft::communication::message::{
    ConsensusMessage, ForwardedRequestsMessage, Header, Message, ObserveEventKind, RequestMessage,
    StoredMessage, SystemMessage,
};
use crate::bft::communication::{Node, NodeConfig, NodeId};
use crate::bft::consensus::log::persistent::PersistentLogModeTrait;
use crate::bft::consensus::log::{Info, Log};
use crate::bft::consensus::{Consensus, ConsensusGuard, ConsensusPollStatus, ConsensusStatus};
use crate::bft::core::server::client_replier::Replier;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::core::server::rq_finalizer::{RqFinalizer, RqFinalizerHandle};
use crate::bft::cst::{install_recovery_state, CollabStateTransfer, CstProgress, CstStatus};
use crate::bft::error::*;
use crate::bft::executable::{
    Executor, ExecutorHandle, ReplicaReplier, Reply, Request, Service, State,
};
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::proposer::Proposer;
use crate::bft::sync::{
    AbstractSynchronizer, Synchronizer, SynchronizerPollStatus, SynchronizerStatus,
};
use crate::bft::timeouts::{TimeoutKind, Timeouts, TimeoutsHandle};

use super::SystemParams;

pub mod observer;

pub mod client_replier;
pub mod follower_handling;
pub mod rq_finalizer;

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

/// This struct contains information related with an
/// active `febft` view.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ViewInfo {
    seq: SeqNo,
    quorum: Vec<NodeId>,
    params: SystemParams,
}

impl Orderable for ViewInfo {
    /// Returns the sequence number of the current view.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl ViewInfo {
    /// Creates a new instance of `ViewInfo`.
    pub fn new(seq: SeqNo, n: usize, f: usize) -> Result<Self> {
        //TODO: Make the quorum participants modifiable
        let params = SystemParams::new(n, f)?;
        Ok(ViewInfo {
            seq,
            quorum: NodeId::targets_u32(0..n as u32).collect(),
            params,
        })
    }

    /// Returns a copy of this node's `SystemParams`.
    pub fn params(&self) -> &SystemParams {
        &self.params
    }

    /// Returns a new view with the sequence number after
    /// the current view's number.
    pub fn next_view(&self) -> ViewInfo {
        self.peek(self.seq.next())
    }

    /// Returns a new view with the specified sequence number.
    pub fn peek(&self, seq: SeqNo) -> ViewInfo {
        let mut view = self.clone();
        view.seq = seq;
        view
    }

    /// Returns the leader of the current view.
    pub fn leader(&self) -> NodeId {
        self.quorum[usize::from(self.seq) % self.params.n()]
    }

    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum
    }
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service + 'static, T: PersistentLogModeTrait> {
    phase: ReplicaPhase,
    // this value is primarily used to switch from
    // state transfer back to a view change
    phase_stack: Option<ReplicaPhase>,
    timeouts: Arc<TimeoutsHandle<S>>,
    executor: ExecutorHandle<S>,
    synchronizer: Arc<Synchronizer<S>>,
    consensus: Consensus<S>,
    //The guard for the consensus.
    //Set to true when there is a consensus running, false when it's ready to receive
    //A new pre-prepare message
    consensus_guard: ConsensusGuard,
    // Check if unordered requests can be proposed.
    // This can only occur when we are in the normal phase of the state machine
    unordered_rq_guard: Arc<AtomicBool>,
    cst: CollabStateTransfer<S>,
    log: Arc<Log<S, T>>,
    proposer: Arc<Proposer<S, T>>,
    node: Arc<Node<S::Data>>,
    rq_finalizer: RqFinalizerHandle<S>,
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
    ///The logging mode
    pub log_mode: PhantomData<T>,
    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}

impl<S, T> Replica<S, T>
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + Clone + 'static,
    Reply<S>: Send + 'static,
    T: PersistentLogModeTrait + 'static,
{
    /// Bootstrap a replica in `febft`.
    pub async fn bootstrap(cfg: ReplicaConfig<S, T>) -> Result<Self> {
        let ReplicaConfig {
            next_consensus_seq,
            global_batch_size,
            batch_timeout,
            node: node_config,
            service,
            view,
            log_mode,
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
        let log = Log::new(
            log_node_id,
            global_batch_size,
            Some(observer_handle.clone()),
            executor.clone(),
            db_path,
        );

        let seq;

        let view;

        debug!("Reading state from memory");
        //Read the state from the persistent log
        let state = if let Some(read_state) = log.read_current_state(n, f)? {
            let last_seq = if let Some(seq) = read_state.decision_log().last_execution() {
                seq
            } else {
                read_state.checkpoint().sequence_number()
            };

            seq = last_seq;

            view = read_state.view().clone();

            let executed_requests = read_state.requests.clone();

            let state = read_state.checkpoint().state().clone();

            log.install_state(last_seq, read_state);

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
        let timeouts = Timeouts::new(Arc::clone(node.loopback_channel()));

        // TODO:
        // - client req timeout base dur configure param
        // - cst timeout base dur configure param
        // - ask for latest cid when bootstrapping
        const CST_BASE_DUR: Duration = Duration::from_secs(30);
        const REQ_BASE_DUR: Duration = Duration::from_secs(2 * 60);

        let synchronizer = Synchronizer::new_replica(view.clone(), REQ_BASE_DUR);

        let rq_finalizer = RqFinalizer::new(node.id(), log.clone(), executor.clone());

        debug!("Initializing consensus");

        let consensus = Consensus::new_replica(
            node.id(),
            view.clone(),
            next_consensus_seq,
            global_batch_size,
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
            log: log.clone(),
            proposer: Proposer::new(
                node_clone,
                synchronizer,
                log,
                timeouts,
                executor.clone(),
                consensus_guard,
                global_batch_size,
                batch_timeout,
                observer_handle.clone(),
            ),
            executor,
            rq_finalizer,
            observer_handle: observer_handle.clone(),
            unordered_rq_guard: Arc::new(Default::default()),
        };

        //Start receiving and processing client requests
        replica.proposer.clone().start();

        // handle rogue messages
        for message in rogue {
            match message {
                Message::System(header, message) => {
                    match message {
                        request @ SystemMessage::Request(_) => {
                            replica.request_received(header, request);
                        }
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        }
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => {
                            warn!("Rogue reply message detected")
                        }
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

        //Observing code
        if self.phase != old_phase {
            //If the phase is the same, then we got nothing to do as no states have changed

            match (&old_phase, &self.phase) {
                (ReplicaPhase::NormalPhase, _) => {
                    //We want to stop the proposer from trying to propose any requests while we are performing
                    //Other operations.
                    self.consensus_guard
                        .consensus_guard()
                        .store(true, Ordering::SeqCst);
                }
                (_, ReplicaPhase::NormalPhase) => {
                    //Mark the consensus as available, since we are changing to the normal phase
                    //And are therefore ready to receive pre-prepares (if are are the leaders)
                    self.consensus_guard
                        .consensus_guard()
                        .store(false, Ordering::SeqCst);
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
                    request @ SystemMessage::Request(_) => {
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
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.log,
                            &self.node,
                        );
                        match status {
                            CstStatus::Running => (),
                            CstStatus::State(state) => {
                                install_recovery_state(
                                    state,
                                    &self.synchronizer,
                                    &self.log,
                                    &mut self.executor,
                                    &mut self.consensus,
                                )?;

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
                                        &self.log,
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
                                    &self.log,
                                );
                            }
                            CstStatus::RequestState => {
                                self.cst.request_latest_state(
                                    &self.synchronizer,
                                    &self.timeouts,
                                    &mut self.node,
                                    &self.log,
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
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => {
                        warn!("Rogue reply message detected")
                    }
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => {
                        warn!("Rogue unordered request message detected")
                    }
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
                    &self.log,
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
                    }
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    }
                    request @ SystemMessage::Request(_) => {
                        self.request_received(header, request);
                    }
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.log,
                            &mut self.node,
                        );
                        match status {
                            CstStatus::Nil => (),
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer)
                            }
                        }
                    }
                    SystemMessage::ViewChange(message) => {
                        let status = self.synchronizer.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &mut self.log,
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
                                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer)
                            }
                        }
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => {
                        warn!("Rogue reply message detected")
                    }
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => todo!(),
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
        let polled_message = self.consensus.poll(&self.log);

        let leader = self.synchronizer.view().leader() == self.id();

        let message = match polled_message {
            ConsensusPollStatus::Recv => self.node.receive_from_replicas()?,
            ConsensusPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::Consensus(m))
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                debug!("Receiving client requests. {:?}", self.id());
                if let Ok(requests) = self.client_rqs.receiver_channel().recv_sync() {
                    debug!("{:?} // Proposing requests {}", self.id(), requests.len());

                    //Receive the PrePrepare message from the client rq handler thread
                    let replicas = self.node.receive_from_replicas()?;

                    replicas
                }
            }
        };

        debug!("{:?} // Processing message {:?}", self.id(), message);

        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    }
                    request @ SystemMessage::Request(_) => {
                        self.request_received(header, request);
                    }
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.log,
                            &mut self.node,
                        );
                        match status {
                            CstStatus::Nil => (),
                            // should not happen...
                            _ => {
                                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer)
                            }
                        }
                    }
                    SystemMessage::ViewChange(message) => {
                        let status = self.synchronizer.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &self.log,
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
                                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer)
                            }
                        }
                    }
                    SystemMessage::Consensus(message) => {
                        self.adv_consensus(header, message)?;
                    }
                    SystemMessage::FwdConsensus(message) => {
                        warn!("Replicas cannot process forwarded consensus messages! They must receive the preprepare messages straight from leaders!");
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) | SystemMessage::UnOrderedReply(_) => {
                        warn!("Rogue reply message detected")
                    }
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected"),
                    SystemMessage::UnOrderedRequest(_) => todo!(),
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
            &self.log,
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
            ConsensusStatus::Decided(batch_digest, digests, needed_messages) => {
                // for digest in digests.iter() {
                //     self.synchronizer.unwatch_request(digest);
                // }

                let new_meta = BatchMeta::new();
                let meta = std::mem::replace(&mut *self.log.batch_meta().lock(), new_meta);

                if let Some((info, batch, meta)) =
                    self.log
                        .finalize_batch(seq, batch_digest, digests, needed_messages, meta)?
                {
                    //Send the finalized batch to the rq finalizer
                    //So everything can be removed from the correct logs and
                    //Given to the service thread to execute
                    //self.rq_finalizer.queue_finalize(info, meta, rqs);
                    match info {
                        Info::Nil => self.executor.queue_update(meta, batch),
                        // execute and begin local checkpoint
                        Info::BeginCheckpoint => {
                            self.executor.queue_update_and_get_appstate(meta, batch)
                        }
                    }
                    .unwrap();
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

    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: State<S>) -> Result<()> {
        self.log.finalize_checkpoint(seq, appstate)?;
        if self.cst.needs_checkpoint() {
            // status should return CstStatus::Nil,
            // which does not need to be handled
            let _status = self.cst.process_message(
                CstProgress::Nil,
                &self.synchronizer,
                &self.consensus,
                &self.log,
                &mut self.node,
            );
        }

        Ok(())
    }

    fn forwarded_requests_received(&mut self, requests: ForwardedRequestsMessage<Request<S>>) {
        self.synchronizer
            .watch_forwarded_requests(requests, &self.timeouts, &mut self.log);
    }

    fn timeout_received(&mut self, timeout_kind: TimeoutKind) {
        match timeout_kind {
            TimeoutKind::Cst(cst_seq) => {
                let status = self.cst.timed_out(cst_seq);

                match status {
                    CstStatus::RequestLatestCid => {
                        self.cst.request_latest_consensus_seq_no(
                            &self.synchronizer,
                            &self.timeouts,
                            &self.node,
                            &self.log,
                        );

                        self.switch_phase(ReplicaPhase::RetrievingState);
                    }
                    CstStatus::RequestState => {
                        self.cst.request_latest_state(
                            &self.synchronizer,
                            &self.timeouts,
                            &self.node,
                            &self.log,
                        );

                        self.switch_phase(ReplicaPhase::RetrievingState);
                    }
                    // nothing to do
                    _ => (),
                }
            }
            TimeoutKind::ClientRequests(_timeout_seq) => {
                //let status = self.synchronizer
                //    .client_requests_timed_out(timeout_seq, &self.timeouts);

                //match status {
                //    SynchronizerStatus::RequestsTimedOut { forwarded, stopped } => {
                //        if forwarded.len() > 0 {
                //            let requests = self.log.clone_requests(&forwarded);
                //            self.synchronizer.forward_requests(
                //                requests,
                //                &mut self.node,
                //            );
                //        }
                //        if stopped.len() > 0 {
                //            let stopped = self.log.clone_requests(&stopped);
                //            self.synchronizer.begin_view_change(
                //                Some(stopped),
                //                &mut self.node,
                //            );
                //            self.phase = ReplicaPhase::SyncPhase;
                //        }
                //    },
                //    // nothing to do
                //    _ => (),
                //}
            }
        }
    }

    fn requests_received(
        &self,
        t: DateTime<Utc>,
        reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    ) {
        let mut batch_meta = self.log.batch_meta().lock();

        batch_meta.reception_time = t;
        batch_meta.batch_size = reqs.len();

        drop(batch_meta);

        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer
            .watch_request(h.unique_digest(), &self.timeouts);
        self.log.insert(h, r);
    }
}
