//! Contains the server side core protocol logic of `febft`.

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::history::Log;
use crate::bft::async_runtime as rt;
use crate::bft::consensus::{
    SeqNo,
    Consensus,
    PollStatus,
    ConsensusStatus,
};
use crate::bft::executable::{
    Service,
    Executor,
    ExecutorHandle,
    Request,
    Reply,
    State,
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

/// This struct contains information related with an
/// active `febft` view.
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
    executor: ExecutorHandle<S>,
    view: ViewInfo,
    consensus: Consensus<S>,
    log: Log<Request<S>, Reply<S>>,
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
    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}

impl<S> Replica<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Bootstrap a replica in `febft`.
    pub async fn bootstrap(cfg: ReplicaConfig<S>) -> Result<Self> {
        let ReplicaConfig {
            next_consensus_seq,
            node: node_config,
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
        let log = Log::new();

        let mut replica = Replica {
            consensus: Consensus::new(next_consensus_seq),
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
                            // NOTE: requests aren't susceptible to
                            // garbage collection log operations,
                            // so ignoring the return value is fine
                            replica.log.insert(header, request);
                        },
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => panic!("Rogue reply message detected"),
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
            // retrieve the next message to be processed.
            //
            // the order of the next consensus message is guaranteed by
            // `TBOQueue`, in the consensus module.
            let message = match self.consensus.poll(&self.log) {
                PollStatus::Recv => self.node.receive().await?,
                PollStatus::NextMessage(h, m) => Message::System(h, SystemMessage::Consensus(m)),
                PollStatus::TryProposeAndRecv => {
                    if let Some(digest) = self.log.next_request() {
                        self.consensus.propose(digest, self.view, &mut self.node);
                    }
                    self.node.receive().await?
                },
            };

            match message {
                Message::System(header, message) => {
                    match message {
                        request @ SystemMessage::Request(_) => {
                            // NOTE: check note above on the handling
                            // of rogue messages during bootstrap
                            self.log.insert(header, request);
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
                                // reached agreement, execute request
                                //
                                // FIXME: execution layer needs to receive the id
                                // attributed by the consensus layer to each op,
                                // to execute in order
                                ConsensusStatus::Decided(digest) => {
                                    let (header, request) = match self.log.request_payload(&digest) {
                                        Some((h, r)) => (h, r.into_inner()),
                                        None => unreachable!(),
                                    };
                                    self.executor.queue_update(
                                        header.from(),
                                        digest,
                                        request,
                                    )?;
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
                Message::ExecutionFinished(peer_id, digest, payload) => {
                    // deliver reply to client
                    let message = SystemMessage::Reply(ReplyMessage::new(
                        digest,
                        payload,
                    ));
                    self.node.send(message, peer_id);
                },
                Message::ConnectedTx(id, sock) => self.node.handle_connected_tx(id, sock),
                Message::ConnectedRx(id, sock) => self.node.handle_connected_rx(id, sock),
                Message::DisconnectedTx(id) if id >= self.node.first_client_id() => (),
                Message::DisconnectedRx(Some(id)) if id >= self.node.first_client_id() => (),
                // TODO: node disconnected on send side
                Message::DisconnectedTx(id) => panic!("{:?} disconnected", id),
                // TODO: node disconnected on receive side
                Message::DisconnectedRx(some_id) => panic!("{:?} disconnected", some_id),
            }

            // loop end
        }

        // run end
    }
}
