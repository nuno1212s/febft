//! Contains the server side core protocol logic of `febft`.

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use chrono::DateTime;
use chrono::offset::Utc;
use log::{debug, warn};
use parking_lot::Mutex;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use crate::bft::async_runtime as rt;
use crate::bft::async_runtime::JoinHandle;
use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::{
    Node,
    NodeConfig,
    NodeId,
};
use crate::bft::communication::message::{ForwardedRequestsMessage, Header, Message, ObserveEventKind, RequestMessage, StoredMessage, SystemMessage};
use crate::bft::consensus::{
    Consensus,
    ConsensusPollStatus,
    ConsensusStatus,
};
use crate::bft::consensus::log::{
    Info,
    Log,
};
use crate::bft::core::server::client_replier::Replier;
use crate::bft::core::server::client_rq_handling::RqProcessor;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::core::server::rq_finalizer::{RqFinalizer, RqFinalizerHandle};
use crate::bft::cst::{
    CollabStateTransfer,
    CstProgress,
    CstStatus,
    install_recovery_state,
};
use crate::bft::error::*;
use crate::bft::executable::{
    Executor,
    ExecutorHandle,
    Reply,
    Request,
    Service,
    State,
};
use crate::bft::ordering::{
    Orderable,
    SeqNo,
};
use crate::bft::sync::{
    Synchronizer,
    SynchronizerPollStatus,
    SynchronizerStatus,
};
use crate::bft::timeouts::{
    TimeoutKind,
    Timeouts,
    TimeoutsHandle,
};

use super::SystemParams;

pub mod observer;

pub mod client_rq_handling;
pub mod rq_finalizer;
pub mod client_replier;

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
#[derive(Copy, Clone)]
pub struct ViewInfo {
    seq: SeqNo,
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
        let params = SystemParams::new(n, f)?;
        Ok(ViewInfo { seq, params })
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
        let mut view = *self;
        view.seq = seq;
        view
    }

    /// Returns the leader of the current view.
    pub fn leader(&self) -> NodeId {
        NodeId::from(usize::from(self.seq) % self.params.n())
    }
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service + 'static> {
    phase: ReplicaPhase,
    // this value is primarily used to switch from
    // state transfer back to a view change
    phase_stack: Option<ReplicaPhase>,
    timeouts: Arc<TimeoutsHandle<S>>,
    executor: ExecutorHandle<S>,
    synchronizer: Arc<Synchronizer<S>>,
    consensus: Consensus<S>,
    cst: CollabStateTransfer<S>,
    log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
    client_rqs: Arc<RqProcessor<S>>,
    node: Arc<Node<S::Data>>,
    rq_finalizer: RqFinalizerHandle<S>,
    //A handle to the observer worker thread
    observer_handle: ObserverHandle,
}

/// Represents a configuration used to bootstrap a `Replica`.
// TODO: load files from persistent storage
pub struct ReplicaConfig<S> {
    /// The application logic.
    pub service: S,
    /// The sequence number for the current view.
    pub view: SeqNo,
    /// Next sequence number attributed to a request by
    /// the consensus layer.
    pub next_consensus_seq: SeqNo,
    ///The targetted global batch size
    pub global_batch_size: usize,
    ///The time limit for creating that batch, in micro seconds
    pub batch_timeout: u128,
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
    pub async fn bootstrap(cfg: ReplicaConfig<S>) -> Result<Self> {
        let ReplicaConfig {
            next_consensus_seq,
            global_batch_size,
            batch_timeout,
            node: node_config,
            service,
            view,
        } = cfg;

        let per_pool_batch_timeout = node_config.batch_timeout_micros;
        let per_pool_batch_sleep = node_config.batch_sleep_micros;
        let per_pool_batch_size = node_config.batch_size;

        // system params
        let n = node_config.n;
        let f = node_config.f;
        let view = ViewInfo::new(view, n, f)?;

        // TODO: get log from persistent storage
        let log = Log::new(node_config.id.clone(), global_batch_size);

        // connect to peer nodes
        let (node, rogue) = Node::bootstrap(node_config).await?;

        let node_clone = node.clone();

        let reply_handle = Replier::new(node.id(), node.send_node(), log.clone());

        // start executor
        let executor = Executor::new(
            reply_handle,
            log.clone(),
            service,
            node.send_node(),
        )?;

        // start timeouts handler
        let timeouts = Timeouts::new(
            Arc::clone(node.loopback_channel()),
        );

        // TODO:
        // - client req timeout base dur configure param
        // - cst timeout base dur configure param
        // - ask for latest cid when bootstrapping
        const CST_BASE_DUR: Duration = Duration::from_secs(30);
        const REQ_BASE_DUR: Duration = Duration::from_secs(2 * 60);

        let synchronizer = Synchronizer::new(REQ_BASE_DUR, view);

        let rq_finalizer = RqFinalizer::new(
            node.id(),
            log.clone(),
            executor.clone());

        let consensus_info = Arc::new(Mutex::new((next_consensus_seq, view)));

        let consensus_guard = Arc::new(AtomicBool::new(false));

        let observers = observer::start_observers(node.send_node());

        //Initialize replica data with the initialized variables from above
        let mut replica = Replica {
            phase: ReplicaPhase::NormalPhase,
            phase_stack: None,
            cst: CollabStateTransfer::new(CST_BASE_DUR),
            synchronizer: synchronizer.clone(),
            consensus: Consensus::new(next_consensus_seq, node.id(), global_batch_size, consensus_info.clone(),
                                      consensus_guard.clone()),
            timeouts: timeouts.clone(),
            executor,
            node,
            log: log.clone(),
            client_rqs: RqProcessor::new(node_clone, synchronizer, log, timeouts, consensus_info.clone(),
                                         consensus_guard.clone(), global_batch_size,
                                         batch_timeout),
            rq_finalizer,
            observer_handle: observers,
        };

        //Start receiving and processing client requests
        replica.client_rqs.clone().start();

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
                        SystemMessage::Reply(_) => warn!("Rogue reply message detected"),
                        // FIXME: handle rogue cst messages
                        SystemMessage::Cst(_) => warn!("Rogue cst message detected"),
                        // FIXME: handle rogue view change messages
                        SystemMessage::ViewChange(_) => warn!("Rogue view change message detected"),
                        // FIXME: handle rogue forwarded requests messages
                        SystemMessage::ForwardedRequests(_) => warn!("Rogue forwarded requests message detected"),
                        SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected")
                    }
                }
                Message::RequestBatch(time, batch) => {
                    replica.requests_received(time, batch);
                }
                // ignore other messages for now
                _ => (),
            }
        }

        println!("{:?} // Started replica.", replica.id());

        println!("{:?} // Per Pool Batch size: {}", replica.id(), per_pool_batch_size);
        println!("{:?} // Per pool batch sleep: {}", replica.id(), per_pool_batch_sleep);
        println!("{:?} // Per pool batch timeout: {}", replica.id(), per_pool_batch_timeout);

        println!("{:?} // Global batch size: {}", replica.id(), global_batch_size);
        println!("{:?} // Global batch timeout: {}", replica.id(), batch_timeout);

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

        //Observing code
        if self.phase != old_phase {
            //If the phase is the same, then we got nothing to do as no states have changed

            let to_send = match (old_phase, self.phase) {
                (_, ReplicaPhase::RetrievingState) => {
                    ObserveEventKind::CollabStateTransfer
                }
                (_, ReplicaPhase::SyncPhase) => {
                    ObserveEventKind::ViewChangePhase
                }
                (_, ReplicaPhase::NormalPhase) => {
                    let current_view = self.synchronizer.view();

                    let view_seq = current_view.sequence_number();
                    let current_seq = self.consensus.sequence_number();

                    let leader = current_view.leader();

                    ObserveEventKind::NormalPhase((view_seq, current_seq, leader))
                }
            };

            self.observer_handle.tx().send(MessageType::Event(to_send)).expect("Failed to notify observer thread");
        }
    }

    fn update_retrieving_state(&mut self) -> Result<()> {
        debug!("{:?} // Retrieving state...", self.id());
        let message = self.node.receive_from_replicas().unwrap();

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            }
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

                                let next_phase = self.phase_stack
                                    .take()
                                    .unwrap_or(ReplicaPhase::NormalPhase);

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
                    SystemMessage::Reply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected")
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
            SynchronizerPollStatus::Recv => {
                self.node.receive_from_replicas()?
            }
            SynchronizerPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::ViewChange(m))
            }
            SynchronizerPollStatus::ResumeViewChange => {
                self.synchronizer.resume_view_change(
                    &self.log,
                    &mut self.consensus,
                    &self.node,
                );

                self.switch_phase(ReplicaPhase::NormalPhase);
                return Ok(false);
            }
        };

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            }
            Message::System(header, message) => {
                match message {
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
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
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
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
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
                        }
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected")
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
            Message::ExecutionFinishedWithAppstate(appstate) => {
                self.execution_finished_with_appstate(appstate)?;
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
            ConsensusPollStatus::Recv => {
                self.node.receive_from_replicas()?
            }
            ConsensusPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::Consensus(m))
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                debug!("{:?} // Polled propose and recv.", self.id());

                self.consensus.advance_init_phase();

                //Receive the PrePrepare message from the client rq handler thread
                let replicas = self.node.receive_from_replicas()?;

                //debug!("{:?} // Received from replicas. Took {:?}", self.id(), Instant::now().duration_since(start));

                replicas
            }
        };

        debug!("{:?} // Processing message {:?}", self.id(), message);

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            }
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
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
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
                            SynchronizerStatus::Running => self.switch_phase(ReplicaPhase::SyncPhase),
                            // should not happen...
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
                        }
                    }
                    SystemMessage::Consensus(message) => {
                        let seq = self.consensus.sequence_number();

                        debug!("{:?} // Processing consensus message {:?} ", self.id(), message);

                        let start = Instant::now();

                        let status = self.consensus.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &self.synchronizer,
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
                            ConsensusStatus::Decided(batch_digest, digests) => {
                                // for digest in digests.iter() {
                                //     self.synchronizer.unwatch_request(digest);
                                // }

                                let new_meta = BatchMeta::new();
                                let meta = std::mem::replace(&mut *self.log.batch_meta().lock(), new_meta);

                                let (info, batch) = self.log.finalize_batch(seq, batch_digest, digests)?;

                                //Send the finalized batch to the rq finalizer
                                //So everything can be removed from the correct logs and
                                //Given to the service thread to execute
                                //self.rq_finalizer.queue_finalize(info, meta, rqs);
                                match info {
                                    Info::Nil => self.executor.queue_update(
                                        meta,
                                        batch,
                                    ),
                                    // execute and begin local checkpoint
                                    Info::BeginCheckpoint => self.executor.queue_update_and_get_appstate(
                                        meta,
                                        batch,
                                    ),
                                }.unwrap();

                                self.consensus.next_instance(&self.synchronizer);
                            }
                        }

                        // we processed a consensus message,
                        // signal the consensus layer of this event
                        self.consensus.signal();

                        debug!("{:?} // Done processing consensus message. Took {:?}", self.id(), Instant::now().duration_since(start));

                        // yield execution since `signal()`
                        // will probably force a value from the
                        // TBO queue in the consensus layer
                        // std::hint::spin_loop();
                    }
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) => warn!("Rogue reply message detected"),
                    SystemMessage::ObserverMessage(_) => warn!("Rogue observer message detected")
                }
            }
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            }
            Message::ExecutionFinishedWithAppstate(appstate) => {
                self.execution_finished_with_appstate(appstate)?;
            }
        }

        Ok(())
    }

    fn execution_finished_with_appstate(
        &mut self,
        appstate: State<S>,
    ) -> Result<()> {
        self.log.finalize_checkpoint(appstate)?;
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

    fn forwarded_requests_received(
        &mut self,
        requests: ForwardedRequestsMessage<Request<S>>,
    ) {
        self.synchronizer.watch_forwarded_requests(
            requests,
            &self.timeouts,
            &mut self.log,
        );
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

    fn requests_received(&self, t: DateTime<Utc>, reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>) {
        let mut batch_meta = self.log.batch_meta().lock();

        batch_meta.reception_time = t;
        batch_meta.batch_size = reqs.len();

        drop(batch_meta);

        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer.watch_request(
            h.unique_digest(),
            &self.timeouts,
        );
        self.log.insert(h, r);
    }
}
