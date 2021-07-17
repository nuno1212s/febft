//! Contains the server side core protocol logic of `febft`.

use std::time::Duration;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::ordering::SeqNo;
use crate::bft::async_runtime as rt;
use crate::bft::sync::{
    Synchronizer,
    SynchronizerStatus,
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
use crate::bft::log::{
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
    UpdateBatchReplies,
};
use crate::bft::communication::{
    Node,
    NodeId,
    NodeConfig,
};
use crate::bft::communication::message::{
    SystemMessage,
    ReplyMessage,
    Message,
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

    /// Returns the sequence number of the current view.
    pub fn sequence_number(&self) -> SeqNo {
        self.seq
    }

    /// Returns a new view with the sequence number after
    /// the current view's number.
    pub fn next_view(&self) -> ViewInfo {
        let mut view = *self;
        view.seq = view.seq.next();
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
        let (node, rogue) = Node::bootstrap(node_config).await?;

        // start executor
        let executor = Executor::new(
            node.master_channel(),
            service,
        )?;

        // start timeouts handler
        let timeouts = Timeouts::new(
            node.master_channel(),
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
                            // TODO: start timer for request
                            replica.log.insert(header, request);
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
                    }
                },
                // ignore other messages for now
                _ => (),
            }
        }

        Ok(replica)
    }

    /// The main loop of a replica.
    pub async fn run(&mut self) -> Result<()> {
        // TODO: exit condition?
        loop {
            match self.phase {
                ReplicaPhase::RetrievingState => self.update_retrieving_state().await?,
                ReplicaPhase::NormalPhase => self.update_normal_phase().await?,
                ReplicaPhase::SyncPhase => self.update_sync_phase().await?,
            }
        }
    }

    async fn update_retrieving_state(&mut self) -> Result<()> {
        let message = self.node.receive().await?;

        match message {
            Message::System(header, message) => {
                match message {
                    request @ SystemMessage::Request(_) => {
                        // TODO: start timer for request
                        self.log.insert(header, request);
                    },
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    },
                    SystemMessage::ViewChange(_message) => {
                        // TODO: handle view change messages
                        unimplemented!()
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
                                self.phase = ReplicaPhase::NormalPhase;
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
            Message::ExecutionFinished(batch) | Message::ExecutionFinishedWithAppstate(batch, _) => {
                // TODO: verify if ignoring the checkpoint state while
                // receiving state from peer nodes is correct
                self.execution_finished(batch);
            },
            Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
            Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
            // TODO: node disconnected on send side
            Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
            // TODO: node disconnected on receive side
            Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
        }

        Ok(())
    }

    async fn update_sync_phase(&mut self) -> Result<()> {
        unimplemented!()
    }

    async fn update_normal_phase(&mut self) -> Result<()> {
        // retrieve the next message to be processed.
        //
        // the order of the next consensus message is guaranteed by
        // `TboQueue`, in the consensus module.
        let message = match self.consensus.poll(&self.log) {
            ConsensusPollStatus::Recv => {
                self.node.receive().await?
            },
            ConsensusPollStatus::NextMessage(h, m) => {
                Message::System(h, SystemMessage::Consensus(m))
            },
            ConsensusPollStatus::TryProposeAndRecv => {
                if let Some(digests) = self.log.next_batch() {
                    self.consensus.propose(digests, &self.synchronizer, &mut self.node);
                }
                self.node.receive().await?
            },
        };

        match message {
            Message::System(header, message) => {
                match message {
                    request @ SystemMessage::Request(_) => {
                        // TODO: start timer for request
                        self.log.insert(header, request);
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
                    SystemMessage::ViewChange(_message) => {
                        // TODO: handle view change messages
                        unimplemented!()
                    },
                    SystemMessage::Consensus(message) => {
                        let status = self.consensus.process_message(
                            header,
                            message,
                            &self.synchronizer,
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
                                let (info, batch) = self.log.finalize_batch(digests)?;
                                match info {
                                    // normal execution
                                    Info::Nil => self.executor.queue_update(
                                        batch,
                                    )?,
                                    // execute and begin local checkpoint
                                    Info::BeginCheckpoint => self.executor.queue_update_and_get_appstate(
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
            Message::ExecutionFinished(batch) => {
                self.execution_finished(batch);
            },
            Message::ExecutionFinishedWithAppstate(batch, appstate) => {
                self.execution_finished_with_appstate(batch, appstate)?;
            },
            Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
            Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
            // TODO: node disconnected on send side
            Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
            // TODO: node disconnected on receive side
            Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
        }
        Ok(())
    }

    fn execution_finished(&mut self, batch: UpdateBatchReplies<Reply<S>>) {
        // deliver replies to clients
        for update_reply in batch.into_inner() {
            let (peer_id, digest, payload) = update_reply.into_inner();
            let message = SystemMessage::Reply(ReplyMessage::new(
                digest,
                payload,
            ));
            self.node.send(message, peer_id);
        }
    }

    fn execution_finished_with_appstate(
        &mut self,
        batch: UpdateBatchReplies<Reply<S>>,
        appstate: State<S>,
    ) -> Result<()> {
        self.log.finalize_checkpoint(appstate)?;
        self.execution_finished(batch);
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
            TimeoutKind::ClientRequests(timeout_seq) => {
                let status = self.synchronizer
                    .client_requests_timed_out(timeout_seq);

                match status {
                    SynchronizerStatus::RequestsTimedOut { forwarded, stopped } => {
                        if forwarded.len() > 0 {
                            let _requests = self.log.clone_requests(&forwarded);

                            // TODO: fetch requests from log and forward them
                            // to all of our peer nodes
                            unimplemented!()
                        }
                        if stopped.len() > 0 {
                            let _stopped = self.log.clone_requests(&stopped);

                            // TODO: begin view change
                            unimplemented!()
                        }
                    },
                    // nothing to do
                    _ => (),
                }
            },
        }
    }
}
