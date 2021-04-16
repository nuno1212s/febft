//! Contains the server side core protocol logic of `febft`.

use std::collections::VecDeque;

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::crypto::hash::Digest;
use crate::bft::collections::{self, HashMap, HashSet};
use crate::bft::consensus::{
    Consensus,
    PollStatus,
    ConsensusStatus,
};
use crate::bft::history::{
    Logger,
    LoggerHandle,
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
    Header,
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
    log: LoggerHandle<Request<S>, Reply<S>>,
    view: ViewInfo,
    requests: VecDeque<(Header, Request<S>)>,
    deciding: HashMap<Digest, (Header, Request<S>)>,
    decided: HashSet<Digest>,
    consensus: Consensus<S>,
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
    pub next_consensus_seq: i32,
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

        // start logger
        let log = Logger::new(node.master_channel());

        let mut replica = Replica {
            consensus: Consensus::new(next_consensus_seq),
            deciding: collections::hash_map(),
            decided: collections::hash_set(),
            requests: VecDeque::new(),
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
                        SystemMessage::Request(r) => {
                            let op = r.into_inner();
                            replica.requests.push_back((header, op));
                        },
                        SystemMessage::Consensus(message) => {
                            replica.consensus.queue(header, message);
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => panic!("rogue reply message detected"),
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
            let message = match self.consensus.poll() {
                PollStatus::Recv => self.node.receive().await?,
                PollStatus::NextMessage(h, m) => Message::System(h, SystemMessage::Consensus(m)),
                PollStatus::TryProposeAndRecv => {
                    match self.requests.pop_front() {
                        Some((h, r)) if self.decided.remove(h.digest()) => {
                            // FIXME: is this correct? should we store a consensus
                            // id to execute in order..?
                            self.executor.queue(
                                h.from(),
                                h.digest().clone(),
                                r,
                            ).await?;
                            continue;
                        },
                        Some((h, r)) => {
                            let dig = h.digest().clone();
                            self.consensus.propose(dig, self.view, &mut self.node);
                            self.deciding.insert(dig, (h, r));
                        },
                        None => (),
                    }
                    self.node.receive().await?
                },
            };

            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Request(m) => {
                            // queue request header and payload
                            self.requests.push_back((header, m.into_inner()));
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
                                ConsensusStatus::Decided(digest) => {
                                    if let Some((header, request)) = self.deciding.remove(&digest) {
                                        self.executor.queue(
                                            header.from(),
                                            digest,
                                            request,
                                        ).await?;
                                    } else {
                                        // we haven't processed this request yet,
                                        // store it as decided
                                        self.decided.insert(header.digest().clone());
                                    }
                                    self.consensus.next_instance();
                                },
                            }
                            // we processed a consensus message,
                            // signal the consensus layer of this event
                            self.consensus.signal();
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => panic!("rogue reply message detected"),
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
                // TODO: node disconnected on send side
                Message::DisconnectedTx(_id) => unimplemented!(),
                // TODO: node disconnected on receive side
                Message::DisconnectedRx(_some_id) => unimplemented!(),
            }

            // loop end
        }

        // run end
    }
}
