//! Contains the server side core protocol logic of `febft`.

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::ordering::SeqNo;
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

enum ReplicaState {
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
    leader: NodeId,
    params: SystemParams,
}

impl ViewInfo {
    /// Creates a new instance of `ViewInfo`.
    pub fn new(leader: NodeId, n: usize, f: usize) -> Result<Self> {
        if leader >= NodeId::from(n) {
            return Err("Invalid NodeId")
                .wrapped(ErrorKind::Core);
        }
        let params = SystemParams::new(n, f)?;
        Ok(ViewInfo { leader, params })
    }

    /// Returns a copy of this node's `SystemParams`.
    pub fn params(&self) -> &SystemParams {
        &self.params
    }

    /// Returns the leader of the current view.
    pub fn leader(&self) -> NodeId {
        self.leader
    }
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service> {
    state: ReplicaState,
    executor: ExecutorHandle<S>,
    view: ViewInfo,
    consensus: Consensus<S>,
    log: Log<State<S>, Request<S>, Reply<S>>,
    node: Node<S::Data>,
}

/// Represents a configuration used to bootstrap a `Replica`.
// TODO: load files from persistent storage
pub struct ReplicaConfig<S> {
    /// The application logic.
    pub service: S,
    /// The leader for the current view.
    pub leader: NodeId,
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
            leader,
        } = cfg;

        // system params
        let n = node_config.n;
        let f = node_config.f;
        let view = ViewInfo::new(leader, n, f)?;

        // connect to peer nodes
        let (node, rogue) = Node::bootstrap(node_config).await?;

        // start executor
        let executor = Executor::new(
            node.master_channel(),
            service,
        )?;

        // TODO: get log from persistent storage
        let log = Log::new(batch_size);

        let mut replica = Replica {
            consensus: Consensus::new(next_consensus_seq, batch_size),
            state: ReplicaState::NormalPhase,
            executor,
            node,
            view,
            log,
        };

        // handle rogue messages
        for message in rogue {
            match message {
                Message::System(header, message) => {
                    match message {
                        request @ SystemMessage::Request(_) => {
                            replica.log.insert(header, request);
                        },
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
                        // FIXME: handle rogue cst messages
                        SystemMessage::Cst(_) => panic!("Rogue cst message detected"),
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
            match self.state {
                ReplicaState::RetrievingState => self.update_retrieving_state().await?,
                ReplicaState::NormalPhase => self.update_normal_phase().await?,
                ReplicaState::SyncPhase => self.update_sync_phase().await?,
            }
        }
    }

    async fn update_retrieving_state(&mut self) -> Result<()> {
        let message = self.node.receive().await?;

        match message {
            // TODO: handle timeouts
            Message::Timeouts(_) => unimplemented!(),
            Message::System(header, message) => {
                match message {
                    SystemMessage::Consensus(message) => {
                        self.consensus.queue(header, message);
                    },
                    // TODO: implement handling the rest of the
                    // system message kinds
                    _ => unimplemented!(),
                }
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
                    self.consensus.propose(digests, self.view, &mut self.node);
                }
                self.node.receive().await?
            },
        };

        match message {
            // TODO: handle timeouts
            Message::Timeouts(_) => unimplemented!(),
            Message::System(header, message) => {
                match message {
                    request @ SystemMessage::Request(_) => {
                        self.log.insert(header, request);
                    },
                    SystemMessage::Cst(_message) => {
                        // TODO: update cst state
                        //self.cst.process_message( ... )
                    },
                    SystemMessage::Consensus(message) => {
                        let status = self.consensus.process_message(
                            header,
                            message,
                            self.view,
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
        /*
        // check if the cst layer needs the checkpoint
        if self.cst.needs_checkpoint() {
            let status = self.cst.process_message(
                CstProgress::Nil,
                &self.view,
                &self.consensus,
                &mut self.log,
                &mut self.node,
            );
        }
        */
        Ok(())
    }
}
