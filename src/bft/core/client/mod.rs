//! Contains the client side core protocol logic of `febft`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use super::SystemParams;

use crate::bft::error::*;
use crate::bft::ordering::SeqNo;
use crate::bft::async_runtime as rt;
use crate::bft::crypto::hash::Digest;
use crate::bft::collections::{self, HashMap};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::message::{
    Message,
    SystemMessage,
    RequestMessage,
};
use crate::bft::communication::{
    Node,
    NodeId,
    SendNode,
    NodeConfig,
};

struct ClientReady<P> {
    reply: Option<oneshot::Sender<P>>,
}

struct ClientData<P> {
    session_counter: AtomicU32,
    ready: Mutex<HashMap<Digest, ClientReady<P>>>,
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData> {
    session_id: SeqNo,
    operation_counter: SeqNo,
    data: Arc<ClientData<D::Reply>>,
    params: SystemParams,
    node: SendNode<D>,
}

impl<D: SharedData> Clone for Client<D> {
    fn clone(&self) -> Self {
        let session_id = self.data
            .session_counter
            .fetch_add(1, Ordering::Relaxed)
            .into();

        Self {
            session_id,
            params: self.params,
            node: self.node.clone(),
            data: Arc::clone(&self.data),
            operation_counter: SeqNo::ZERO,
        }
    }
}

/// Represents a configuration used to bootstrap a `Client`.
pub struct ClientConfig {
    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}

struct ReplicaVotes {
    count: usize,
    digest: Digest,
}

impl<D: SharedData> std::ops::Drop for Client<D> {
    fn drop(&mut self) {
        println!("CLIENT {:?} DROPPED @ {}", self.id(), std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos());
    }
}

impl<D: SharedData> Client<D> {
    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }
}

impl<D> Client<D>
where
    D: SharedData + 'static,
    D::State: Send + Clone + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
    /// Bootstrap a client in `febft`.
    pub async fn bootstrap(cfg: ClientConfig) -> Result<Self> {
        let ClientConfig { node: node_config } = cfg;

        // system params
        let n = node_config.n;
        let f = node_config.f;
        let params = SystemParams::new(n, f)?;

        // connect to peer nodes
        //
        // FIXME: can the client receive rogue reply messages?
        // perhaps when it reconnects to a replica after experiencing
        // network problems? for now ignore rogue messages...
        let (node, _batcher, _rogue) = Node::bootstrap(node_config).await?;

        // create shared data
        let data = Arc::new(ClientData {
            session_counter: AtomicU32::new(0),
            ready: Mutex::new(collections::hash_map()),
        });
        let task_data = Arc::clone(&data);

        // get `SendNode` before giving up ownership on the `Node`
        let send_node = node.send_node();

        // spawn receiving task
        rt::spawn(Self::message_recv_task(
            params,
            task_data,
            node,
        ));

        let session_id = data
            .session_counter
            .fetch_add(1, Ordering::Relaxed)
            .into();

        Ok(Client {
            data,
            params,
            session_id,
            node: send_node,
            operation_counter: SeqNo::ZERO,
        })
    }

    /// Updates the replicated state of the application running
    /// on top of `febft`.
    //
    // TODO: request timeout
    pub async fn update(&mut self, operation: D::Request) -> D::Reply {
        let message = SystemMessage::Request(RequestMessage::new(
            self.session_id,
            self.next_operation_id(),
            operation,
        ));

        // broadcast our request to the node group
        let targets = NodeId::targets(0..self.params.n());
        let digest = self.node.broadcast(message, targets);

        // register channel to receive response in
        let reply = {
            let (tx, rx) = oneshot::channel();

            let mut ready = self.data.ready.lock();
            ready.insert(digest, ClientReady { reply: Some(tx) });

            rx
        };

        // await response
        reply.await.unwrap()
    }

    fn next_operation_id(&mut self) -> SeqNo {
        let id = self.operation_counter;
        self.operation_counter = self.operation_counter.next();
        id
    }

    async fn message_recv_task(
        params: SystemParams,
        data: Arc<ClientData<D::Reply>>,
        mut node: Node<D>,
    ) {
        let mut votes: HashMap<Digest, ReplicaVotes> = collections::hash_map();

        loop {
            let message = node.receive().await.unwrap();
            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Reply(message) => {
                            let (digest, payload) = message.into_inner();
                            let votes = votes
                                .entry(digest)
                                // FIXME: cache every reply's digest, instead of just the first one
                                // we receive, because the first reply may be faulty, while the
                                // remaining ones may be correct, therefore we would not be able to
                                // count at least f+1 identical replies
                                //
                                // NOTE: the `digest()` call in the header returns the digest of
                                // the payload
                                .or_insert_with(|| ReplicaVotes { count: 0, digest: header.digest().clone() });

                            // register new reply received
                            if &votes.digest == header.digest() {
                                votes.count += 1;
                            }

                            // TODO: check if a replica hasn't voted
                            // twice for the same digest

                            // wait for at least f+1 identical replies
                            if votes.count > params.f() {
                                let mut ready = data.ready.lock();
                                let request = ready
                                    .entry(digest)
                                    .or_insert_with(|| ClientReady { reply: None });

                                // wake up pending task
                                if let Some(sender) = request.reply.take() {
                                    sender.send(payload).unwrap();
                                }
                            }
                        },
                        // FIXME: handle rogue messages on clients
                        _ => panic!("rogue message detected"),
                    }
                },
                Message::ConnectedTx(id, sock) => node.handle_connected_tx(id, sock),
                Message::ConnectedRx(id, sock) => node.handle_connected_rx(id, sock),
                // TODO: node disconnected on send side
                Message::DisconnectedTx(_id) => unimplemented!(),
                // TODO: node disconnected on receive side
                Message::DisconnectedRx(_some_id) => unimplemented!(),
                // we don't receive any other type of messages as a client node
                _ => (),
            }
        }
    }
}
