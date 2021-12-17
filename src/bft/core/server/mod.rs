//! Contains the server side core protocol logic of `febft`.

use std::time::Duration;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use chrono::DateTime;
use chrono::offset::Utc;

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::ordering::{
    SeqNo,
    Orderable,
};
use crate::bft::sync::{
    Synchronizer,
    SynchronizerStatus,
    SynchronizerPollStatus,
};
use crate::bft::timeouts::{
    Timeouts,
    TimeoutKind,
    TimeoutsHandle,
};
use crate::bft::cst::{
    install_recovery_state,
    CollabStateTransfer,
    CstProgress,
    CstStatus,
};
use crate::bft::consensus::log::{
    Info,
    Log,
};
use crate::bft::consensus::{
    Consensus,
    ConsensusStatus,
    ConsensusPollStatus,
};
use crate::bft::executable::{
    Reply,
    State,
    Request,
    Service,
    Executor,
    ExecutorHandle,
};
use crate::bft::communication::{
    Node,
    NodeId,
    NodeConfig,
};
use crate::bft::communication::message::{
    ForwardedRequestsMessage,
    RequestMessage,
    SystemMessage,
    StoredMessage,
    Message,
    Header,
};

enum ReplicaPhase {
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
pub struct Replica<S: Service> {
    phase: ReplicaPhase,
    // this value is primarily used to switch from
    // state transfer back to a view change
    phase_stack: Option<ReplicaPhase>,
    timeouts: TimeoutsHandle<S>,
    executor: ExecutorHandle<S>,
    synchronizer: Synchronizer<S>,
    consensus: Consensus<S>,
    cst: CollabStateTransfer<S>,
    log: Log<State<S>, Request<S>, Reply<S>>,
    node: Node<S::Data>,
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
    /// The maximum number of client requests to queue
    /// before executing the consensus algorithm.
    pub batch_size: usize,
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
            node: node_config,
            batch_size,
            service,
            view,
        } = cfg;

        // system params
        let n = node_config.n;
        let f = node_config.f;
        let view = ViewInfo::new(view, n, f)?;

        // connect to peer nodes
        let (node, batcher, rogue) = Node::bootstrap(node_config).await?;

        // start batcher
        if let Some(b) = batcher {
            b.spawn(batch_size);
        }

        // start executor
        let executor = Executor::new(
            node.send_node(),
            service,
        )?;

        // start timeouts handler
        let timeouts = Timeouts::new(
            node.master_channel().clone(),
        );

        // TODO: get log from persistent storage
        let log = Log::new(batch_size);

        // TODO:
        // - client req timeout base dur configure param
        // - cst timeout base dur configure param
        // - ask for latest cid when bootstrapping
        const CST_BASE_DUR: Duration = Duration::from_secs(30);
        const REQ_BASE_DUR: Duration = Duration::from_secs(2 * 60);

        let mut replica = Replica {
            cst: CollabStateTransfer::new(CST_BASE_DUR),
            synchronizer: Synchronizer::new(REQ_BASE_DUR, view),
            consensus: Consensus::new(next_consensus_seq, batch_size),
            phase: ReplicaPhase::NormalPhase,
            phase_stack: None,
            timeouts,
            executor,
            node,
            log,
        };

        // handle rogue messages
        for message in rogue {
            match message {
                Message::System(header, message) => {
                    match message {
                        request @ SystemMessage::Request(_) => {
                            replica.request_received(header, request);
                        },
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
                        // FIXME: handle rogue cst messages
                        SystemMessage::Cst(_) => panic!("Rogue cst message detected"),
                        // FIXME: handle rogue view change messages
                        SystemMessage::ViewChange(_) => panic!("Rogue view change message detected"),
                        // FIXME: handle rogue forwarded requests messages
                        SystemMessage::ForwardedRequests(_) => panic!("Rogue forwarded requests message detected"),
                    }
                },
                Message::RequestBatch(time, batch) => {
                    replica.requests_received(time, batch);
                },
                // ignore other messages for now
                _ => (),
            }
        }

        Ok(replica)
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// The main loop of a replica.
    pub async fn run(&mut self) -> Result<()> {
        // TODO: exit condition?
        loop {
            match self.phase {
                ReplicaPhase::RetrievingState => self.update_retrieving_state().await?,
                ReplicaPhase::NormalPhase => self.update_normal_phase().await?,
                ReplicaPhase::SyncPhase => self.update_sync_phase().await.map(|_| ())?,
            }
        }
    }

    async fn update_retrieving_state(&mut self) -> Result<()> {
        let message = self.node.receive().await?;

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            },
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(requests) => {
                        // FIXME: is this the correct behavior? to save forwarded requests
                        // while we are retrieving state...
                        self.forwarded_requests_received(requests);
                    },
                    request @ SystemMessage::Request(_) => {
                        self.request_received(header, request);
                    },
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    },
                    SystemMessage::ViewChange(message) => {
                        self.synchronizer.queue(header, message);
                    },
                    SystemMessage::Cst(message) => {
                        let status = self.cst.process_message(
                            CstProgress::Message(header, message),
                            &self.synchronizer,
                            &self.consensus,
                            &self.log,
                            &mut self.node,
                        );
                        match status {
                            CstStatus::Running => (),
                            CstStatus::State(state) => {
                                install_recovery_state(
                                    state,
                                    &mut self.synchronizer,
                                    &mut self.log,
                                    &mut self.executor,
                                    &mut self.consensus,
                                )?;
                                self.phase = self.phase_stack
                                    .take()
                                    .unwrap_or(ReplicaPhase::NormalPhase);
                            },
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
                                        &mut self.node,
                                    );
                                } else {
                                    self.phase = ReplicaPhase::NormalPhase;
                                }
                            },
                            CstStatus::RequestLatestCid => {
                                self.cst.request_latest_consensus_seq_no(
                                    &self.synchronizer,
                                    &self.timeouts,
                                    &mut self.node,
                                );
                            },
                            CstStatus::RequestState => {
                                self.cst.request_latest_state(
                                    &self.synchronizer,
                                    &self.timeouts,
                                    &mut self.node,
                                );
                            },
                            // should not happen...
                            CstStatus::Nil => {
                                return Err("Invalid state reached!")
                                    .wrapped(ErrorKind::CoreServer);
                            },
                        }
                    },
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
                }
            },
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            },
            Message::ExecutionFinishedWithAppstate(_) => {
                // TODO: verify if ignoring the checkpoint state while
                // receiving state from peer nodes is correct
            },
            Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
            Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
            Message::DisconnectedTx(id) if id >= self.node.first_client_id() => println!("{:?} disconnected TX", id),
            Message::DisconnectedRx(Some(id)) if id >= self.node.first_client_id() => println!("{:?} disconnected RX", id),
            // TODO: node disconnected on send side
            Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
            // TODO: node disconnected on receive side
            Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
        }

        Ok(())
    }

    async fn update_sync_phase(&mut self) -> Result<bool> {
        // retrieve a view change message to be processed
        let message = match self.synchronizer.poll() {
            SynchronizerPollStatus::Recv => {
                self.node.receive().await?
            },
            SynchronizerPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::ViewChange(m))
            },
            SynchronizerPollStatus::ResumeViewChange => {
                self.synchronizer.resume_view_change(
                    &mut self.log,
                    &mut self.consensus,
                    &mut self.node,
                );
                self.phase = ReplicaPhase::NormalPhase;
                return Ok(false);
            },
        };

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            },
            Message::System(header, message) => {
                match message {
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    },
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    },
                    request @ SystemMessage::Request(_) => {
                        self.request_received(header, request);
                    },
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
                    },
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
                                self.phase = ReplicaPhase::NormalPhase;
                                return Ok(false);
                            },
                            SynchronizerStatus::RunCst => {
                                self.phase = ReplicaPhase::RetrievingState;
                                self.phase_stack = Some(ReplicaPhase::SyncPhase);
                            },
                            // should not happen...
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
                        }
                    },
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
                }
            },
            //////// XXX XXX XXX XXX
            //
            // TODO: check if simply copying the behavior over from the
            // normal phase is correct here
            //
            //
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            },
            Message::ExecutionFinishedWithAppstate(appstate) => {
                self.execution_finished_with_appstate(appstate)?;
            },
            Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
            Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
            Message::DisconnectedTx(id) if id >= self.node.first_client_id() => println!("{:?} disconnected TX", id),
            Message::DisconnectedRx(Some(id)) if id >= self.node.first_client_id() => println!("{:?} disconnected RX", id),
            // TODO: node disconnected on send side
            Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
            // TODO: node disconnected on receive side
            Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
        }

        Ok(true)
    }

    async fn update_normal_phase(&mut self) -> Result<()> {
        // check if we have STOP messages to be processed,
        // and update our phase when we start installing
        // the new view
        if self.synchronizer.can_process_stops() {
            let running = self.update_sync_phase().await?;
            if running {
                self.phase = ReplicaPhase::SyncPhase;
                return Ok(());
            }
        }

        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let message = match self.consensus.poll(&mut self.log) {
            ConsensusPollStatus::Recv => {
                self.node.receive().await?
            },
            ConsensusPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::Consensus(m))
            },
            ConsensusPollStatus::TryProposeAndRecv => {
                if let Some(requests) = self.log.next_batch() {
                    self.consensus.propose(requests, &self.synchronizer, &mut self.node);
                }
                self.node.receive().await?
            },
        };

        match message {
            Message::RequestBatch(time, batch) => {
                self.requests_received(time, batch);
            },
            Message::System(header, message) => {
                match message {
                    SystemMessage::ForwardedRequests(requests) => {
                        self.forwarded_requests_received(requests);
                    },
                    request @ SystemMessage::Request(_) => {
                        self.request_received(header, request);
                    },
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
                    },
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
                            SynchronizerStatus::Nil => (),
                            SynchronizerStatus::Running => self.phase = ReplicaPhase::SyncPhase,
                            // should not happen...
                            _ => return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer),
                        }
                    },
                    SystemMessage::Consensus(message) => {
                        let seq = self.consensus.sequence_number();
                        let status = self.consensus.process_message(
                            header,
                            message,
                            &self.timeouts,
                            &mut self.synchronizer,
                            &mut self.log,
                            &mut self.node,
                        );
                        match status {
                            // if deciding, nothing to do
                            ConsensusStatus::Deciding => rt::yield_now().await,
                            // FIXME: implement this
                            ConsensusStatus::VotedTwice(_) => unimplemented!(),
                            // reached agreement, execute requests
                            //
                            // FIXME: execution layer needs to receive the id
                            // attributed by the consensus layer to each op,
                            // to execute in order
                            ConsensusStatus::Decided(digests) => {
                                for digest in digests.iter() {
                                    self.synchronizer.unwatch_request(digest);
                                }
                                let (info, batch) = self.log.finalize_batch(seq, digests)?;
                                match info {
                                    // normal execution
                                    Info::Nil => self.executor.queue_update(
                                        *self.log.batch_meta(),
                                        batch,
                                    )?,
                                    // execute and begin local checkpoint
                                    Info::BeginCheckpoint => self.executor.queue_update_and_get_appstate(
                                        *self.log.batch_meta(),
                                        batch,
                                    )?,
                                }
                                self.consensus.next_instance();
                            },
                        }

                        // we processed a consensus message,
                        // signal the consensus layer of this event
                        self.consensus.signal();

                        // yield execution since `signal()`
                        // will probably force a value from the
                        // TBO queue in the consensus layer
                        rt::yield_now().await;
                    },
                    // FIXME: handle rogue reply messages
                    SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
                }
            },
            Message::Timeout(timeout_kind) => {
                self.timeout_received(timeout_kind);
            },
            Message::ExecutionFinishedWithAppstate(appstate) => {
                self.execution_finished_with_appstate(appstate)?;
            },
            Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
            Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
            Message::DisconnectedTx(id) if id >= self.node.first_client_id() => println!("{:?} disconnected TX", id),
            Message::DisconnectedRx(Some(id)) if id >= self.node.first_client_id() => println!("{:?} disconnected RX", id),
            // TODO: node disconnected on send side
            Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
            // TODO: node disconnected on receive side
            Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
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
                            &mut self.node,
                        );
                        self.phase = ReplicaPhase::RetrievingState;
                    },
                    CstStatus::RequestState => {
                        self.cst.request_latest_state(
                            &self.synchronizer,
                            &self.timeouts,
                            &mut self.node,
                        );
                        self.phase = ReplicaPhase::RetrievingState;
                    },
                    // nothing to do
                    _ => (),
                }
            },
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
            },
        }
    }

    fn requests_received(&mut self, t: DateTime<Utc>, reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>) {
        self.log.batch_meta().reception_time = t;
        self.log.batch_meta().batch_size = reqs.len();

        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&mut self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer.watch_request(
            h.unique_digest(),
            &self.timeouts,
        );
        self.log.insert(h, r);
    }
}
