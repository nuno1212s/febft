//! Contains the client side core protocol logic of `febft`.

use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use std::task::{Poll, Waker, Context};
use std::sync::atomic::{AtomicU32, Ordering};

use intmap::IntMap;
use parking_lot::Mutex;

use super::SystemParams;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::crypto::hash::Digest;
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
use crate::bft::ordering::{
    SeqNo,
    Orderable,
};

struct ClientData<P> {
    session_counter: AtomicU32,
    wakers: Mutex<IntMap<Waker>>,
    ready: Mutex<IntMap<P>>,
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

struct ClientRequestFut<'a, P> {
    request_key: u64,
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
            if let Some(payload) = ready.remove(self.request_key) {
                // FIXME: should you remove wakers here?
                return Poll::Ready(payload);
            }
        }
        // clone waker to wake up this task when
        // the response is ready
        {
            let mut wakers = self.data.wakers.lock();
            wakers.insert(self.request_key, cx.waker().clone());
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
            wakers: Mutex::new(IntMap::new()),
            ready: Mutex::new(IntMap::new()),
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

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// Updates the replicated state of the application running
    /// on top of `febft`.
    //
    // TODO: request timeout
    pub async fn update(&mut self, operation: D::Request) -> D::Reply {
        let session_id = self.session_id;
        let operation_id = self.next_operation_id();
        let message = SystemMessage::Request(RequestMessage::new(
            session_id,
            operation_id,
            operation,
        ));

        // broadcast our request to the node group
        let targets = NodeId::targets(0..self.params.n());
        self.node.broadcast(message, targets);

        // await response
        let data = &*self.data;
        let request_key = get_request_key(session_id, operation_id);

        ClientRequestFut { request_key, data }.await
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
        // use session id as key
        let mut last_operation_ids: IntMap<SeqNo> = IntMap::new();
        let mut replica_votes: IntMap<ReplicaVotes> = IntMap::new();

        while let Ok(message) = node.receive().await {
            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Reply(message) => {
                            let (session_id, operation_id, payload) = message.into_inner();
                            let last_operation_id = last_operation_ids
                                .get(session_id.into())
                                .copied()
                                .unwrap_or(SeqNo::ZERO);

                            // reply already delivered to application
                            if last_operation_id > operation_id {
                                continue;
                            }

                            let request_key = get_request_key(session_id, operation_id);
                            let votes = IntMapEntry::get(request_key, &mut replica_votes)
                                // FIXME: cache every reply's digest, instead of just the first one
                                // we receive, because the first reply may be faulty, while the
                                // remaining ones may be correct, therefore we would not be able to
                                // count at least f+1 identical replies
                                //
                                // NOTE: the `digest()` call in the header returns the digest of
                                // the payload
                                .or_insert_with(|| {
                                    ReplicaVotes {
                                        count: 0,
                                        digest: header.digest().clone(),
                                    }
                                });

                            // register new reply received
                            if &votes.digest == header.digest() {
                                votes.count += 1;
                            }

                            // TODO: check if a replica hasn't voted
                            // twice for the same digest

                            // wait for at least f+1 identical replies
                            if votes.count > params.f() {
                                // update intmap states
                                replica_votes.remove(request_key);
                                last_operation_ids.insert(session_id.into(), operation_id);

                                // register response
                                {
                                    let mut ready = data.ready.lock();
                                    ready.insert(request_key, payload);
                                }

                                // try to wake up a waiting task
                                {
                                    let mut wakers = data.wakers.lock();
                                    if let Some(waker) = wakers.remove(request_key) {
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

#[inline]
fn get_request_key(session_id: SeqNo, operation_id: SeqNo) -> u64 {
    let sess: u64 = session_id.into();
    let opid: u64 = operation_id.into();
    sess | (opid << 32)
}

struct IntMapEntry<'a, T> {
    key: u64,
    map: &'a mut IntMap<T>,
}

macro_rules! certain {
    ($some:expr) => {
        match $some {
            Some(x) => x,
            None => unreachable!(),
        }
    }
}

impl<'a, T> IntMapEntry<'a, T> {
    fn get(key: u64, map: &'a mut IntMap<T>) -> Self {
        Self { key, map }
    }

    fn or_insert_with<F: FnOnce() -> T>(self, default: F) -> &'a mut T {
        let (key, map) = (self.key, self.map);

        if !map.contains_key(key) {
            let value = (default)();
            map.insert(key, value);
        }

        certain!(map.get_mut(key))
    }
}
