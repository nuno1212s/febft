//! Contains the server side core protocol logic of `febft`.

use std::collections::VecDeque;

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::crypto::signature::Signature;
use crate::bft::collections::{self, HashMap};
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
    deciding: HashMap<Signature, (Header, Request<S>)>,
    consensus: Consensus<S>,
    node: Node<S::Data>,
}

impl<S> Replica<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
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
                PollStatus::ProposeAndRecv => {
                    if let Some((h, r)) = self.requests.pop_front() {
                        // FIXME: is this correct?
                        let sig = h.signature();
                        self.consensus.propose(sig, self.view, &mut self.node);
                        self.deciding.insert(sig, (h, r));
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
                                ConsensusStatus::Deciding => (),
                                // FIXME: implement this
                                ConsensusStatus::VotedTwice(_) => unimplemented!(),
                                // reached agreement, execute request
                                ConsensusStatus::Decided(signature) => {
                                    // FIXME: if remove fails, what do we do?!
                                    if let Some((header, request)) = self.deciding.remove(&signature) {
                                        self.executor.queue(
                                            header.from(),
                                            signature,
                                            request,
                                        ).await?;
                                    }
                                    self.consensus.next_instance();
                                },
                            }
                            // we processed a consensus message,
                            // signal the consensus layer of this event
                            self.consensus.signal();
                        },
                        // FIXME: handle rogue reply messages
                        SystemMessage::Reply(_) => (),
                    }
                },
                Message::ExecutionFinished(peer_id, signature, payload) => {
                    // deliver reply to client
                    let message = SystemMessage::Reply(ReplyMessage::new(
                        signature,
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
