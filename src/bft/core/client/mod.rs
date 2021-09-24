//! Contains the client side core protocol logic of `febft`.

use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use std::time::{Instant, Duration};
use std::task::{Poll, Waker, Context};
use std::sync::atomic::{AtomicU32, Ordering};

use parking_lot::Mutex;

use super::SystemParams;

use crate::bft::error::*;
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

struct ClientData<P> {
    operation_counter: AtomicU32,
    wakers: Mutex<HashMap<Digest, Waker>>,
    ready: Mutex<HashMap<Digest, P>>,
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData> {
    data: Arc<ClientData<D::Reply>>,
    params: SystemParams,
    node: SendNode<D>,
}

impl<D: SharedData> Clone for Client<D> {
    fn clone(&self) -> Self {
        Self {
            params: self.params,
            node: self.node.clone(),
            data: Arc::clone(&self.data),
        }
    }
}

struct ClientRequestFut<'a, P> {
    digest: Digest,
    data: &'a ClientData<P>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = P;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<P> {
        // check if response is ready
        {
            let mut ready = self.data.ready.lock();
            if let Some(payload) = ready.remove(&self.digest) {
                // FIXME: should you remove wakers here?
                return Poll::Ready(payload);
            }
        }
        // clone waker to wake up this task when
        // the response is ready
        {
            let mut wakers = self.data.wakers.lock();
            wakers.insert(self.digest, cx.waker().clone());
        }
        Poll::Pending
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

impl<D> Client<D>
where
    D: SharedData + 'static,
    D::State: Send + Clone + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
    // elapsed time since last garbage collection
    // of the replica vote counts hashmap;
    //
    // NOTE: garbage collection is needed because we only
    // need a quorum of votes, but the remaining nodes
    // may also vote, populating the hashmap
    //
    // TODO: tune this value, e.g. maybe change to 3mins?
    const GC_DUR: Duration = Duration::from_secs(30);

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
        let (node, _rogue) = Node::bootstrap(node_config).await?;

        // create shared data
        let data = Arc::new(ClientData {
            operation_counter: AtomicU32::new(0),
            wakers: Mutex::new(collections::hash_map()),
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

        Ok(Client {
            data,
            params,
            node: send_node,
        })
    }

    /// Updates the replicated state of the application running
    /// on top of `febft`.
    //
    // TODO: request timeout
    pub async fn update(&mut self, operation: D::Request) -> D::Reply {
        let id = self.data.operation_counter.fetch_add(1, Ordering::Relaxed);
        let message = SystemMessage::Request(RequestMessage::new(
            id.into(),
            operation,
        ));

        // broadcast our request to the node group
        let targets = NodeId::targets(0..self.params.n());
        let digest = self.node.broadcast(message, targets);

        // await response
        let data = &*self.data;
        ClientRequestFut { digest, data }.await
    }

    async fn message_recv_task(
        params: SystemParams,
        data: Arc<ClientData<D::Reply>>,
        mut node: Node<D>,
    ) {
        let mut earlier = Instant::now();
        let mut votes: HashMap<Digest, ReplicaVotes> = collections::hash_map();

        while let Ok(message) = node.receive().await {
            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Reply(message) => {
                            let now = Instant::now();

                            // garbage collect old votes
                            //
                            // TODO: switch to `HashMap::drain_filter` when
                            // this API reaches stable Rust
                            if now.duration_since(earlier) > Self::GC_DUR {
                                let mut to_remove = Vec::new();

                                for (dig, v) in votes.iter() {
                                    if v.count > params.f() {
                                        to_remove.push(dig.clone());
                                    }
                                }

                                for dig in to_remove {
                                    votes.remove(&dig);
                                }
                            }
                            earlier = now;

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

                            // reply already delivered to application
                            if votes.count > params.f() {
                                continue;
                            }

                            // register new reply received
                            if &votes.digest == header.digest() {
                                votes.count += 1;
                            }

                            // TODO: check if a replica hasn't voted
                            // twice for the same digest

                            // wait for at least f+1 identical replies
                            if votes.count > params.f() {
                                // register response
                                {
                                    let mut ready = data.ready.lock();
                                    ready.insert(digest, payload);
                                }

                                // try to wake up a waiting task
                                {
                                    let mut wakers = data.wakers.lock();
                                    if let Some(waker) = wakers.remove(&digest) {
                                        waker.wake();
                                    }
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
