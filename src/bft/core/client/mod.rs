//! Contains the client side core protocol logic of `febft`.

use std::fs::read;
use std::future::Future;
use std::io::Read;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures::StreamExt;
use futures_timer::Delay;
use intmap::IntMap;
use log::error;
use parking_lot::Mutex;

use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::{
    Node,
    NodeConfig,
    NodeId,
    SendNode,
};
use crate::bft::communication::message::{
    Message,
    RequestMessage,
    SystemMessage,
};
use crate::bft::communication::serialize::SharedData;
use crate::bft::crypto::hash::Digest;
use crate::bft::error::*;
use crate::bft::ordering::SeqNo;

use super::SystemParams;

macro_rules! certain {
    ($some:expr) => {
        match $some {
            Some(x) => x,
            None => unreachable!(),
        }
    }
}

struct Ready<P> {
    waker: Option<Waker>,
    payload: Option<P>,
    timed_out: AtomicBool,
}

struct Callback<P> {
    to_call: Box<dyn FnOnce(P) + Send>,
    timed_out: AtomicBool,
}

impl<P> Deref for Callback<P> {
    type Target = Box<dyn FnOnce(P) + Send>;

    fn deref(&self) -> &Self::Target {
        &self.to_call
    }
}

struct ClientData<P> {
    session_counter: AtomicU32,
    ready: Vec<Mutex<IntMap<Ready<P>>>>,
    callback_ready: Vec<Mutex<IntMap<Callback<P>>>>,
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData + 'static> {
    session_id: SeqNo,
    operation_counter: SeqNo,
    data: Arc<ClientData<D::Reply>>,
    params: SystemParams,
    node: SendNode<D>,
    dummy_meta: Arc<Mutex<BatchMeta>>,
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
            //Start at one, since when receiving we check if received_op_id >= last_op_id, which is by default 0
            operation_counter: SeqNo::ZERO.next(),
            dummy_meta: Arc::new(Mutex::new(BatchMeta::new())),
        }
    }
}

struct ClientRequestFut<'a, P> {
    request_key: u64,
    ready: &'a Mutex<IntMap<Ready<P>>>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = P;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<P> {
        self.ready.try_lock()
            .map(|mut ready| {
                let request = IntMapEntry::get(self.request_key, &mut *ready)
                    .or_insert_with(|| Ready { payload: None, waker: None, timed_out: AtomicBool::new(false) });

                if let Some(payload) = request.payload.take() {
                    //Response is ready, take it
                    ready.remove(self.request_key);
                    return Poll::Ready(payload);
                }

                // clone waker to wake up this task when
                // the response is ready
                request.waker = Some(cx.waker().clone());

                Poll::Pending
            })
            .unwrap_or_else(|| {
                //Failed to get the lock, try again
                cx.waker().wake_by_ref();
                Poll::Pending
            })
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
        let (node, _rogue) = Node::bootstrap(node_config).await?;

        // create shared data
        let data = Arc::new(ClientData {
            session_counter: AtomicU32::new(0),
            ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),
            callback_ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),
        });

        let task_data = Arc::clone(&data);

        // get `SendNode` before giving up ownership on the `Node`
        let send_node = node.send_node();

        // spawn receiving task
        std::thread::Builder::new()
            .name(format!("Client {:?} message processing thread", node.id()))
            .spawn(move || {
                Self::message_recv_task(
                    params,
                    task_data,
                    node,
                )
            }).expect("Failed to launch message processing thread");

        let session_id = data
            .session_counter
            .fetch_add(1, Ordering::Relaxed)
            .into();

        Ok(Client {
            data,
            params,
            session_id,
            node: send_node,
            //Start at one, since when receiving we check if received_op_id >= last_op_id, which is by default 0
            operation_counter: SeqNo::ZERO.next(),
            dummy_meta: Arc::new(Mutex::new(BatchMeta::new())),
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
        let request_key = get_request_key(session_id, operation_id);
        let ready = get_ready::<D>(session_id, &*self.data);

        ClientRequestFut { request_key, ready }.await
    }

    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback(&mut self, operation: D::Request, callback: Box<dyn FnOnce(D::Reply) + Send>) {
        let session_id = self.session_id;

        let operation_id = self.next_operation_id();

        let message = SystemMessage::Request(RequestMessage::new(
            session_id,
            operation_id,
            operation,
        ));

        // await response
        let request_key = get_request_key(session_id, operation_id);
        let ready = get_ready_callback::<D>(session_id, &*self.data);

        let callback = Callback {
            to_call: callback,
            timed_out: AtomicBool::new(false),
        };

        //Scope the mutex operations to reduce the lifetime of the guard
        {
            let mut ready_callback_guard = ready.lock();

            ready_callback_guard.insert(request_key, callback);
        }

        //We only send the message after storing the callback to prevent us receiving the result without having
        //The callback registered, therefore losing the response
        let targets = NodeId::targets(0..self.params.n());

        self.node.broadcast_in_thread_digest(message, targets);

        self.start_timeout(session_id, operation_id, self.data.clone());
    }

    fn next_operation_id(&mut self) -> SeqNo {
        let id = self.operation_counter;

        self.operation_counter = self.operation_counter.next();

        id
    }

    fn start_timeout(&self, session_id: SeqNo, rq_id: SeqNo, client_data: Arc<ClientData<D::Reply>>) {
        let node_id = self.node.id();

        let node = self.node.clone();

        crate::bft::async_runtime::spawn(async move {

            //Timeout delay
            Delay::new(Duration::from_secs(5)).await;

            let req_key = get_request_key(session_id, rq_id);

            {
                let bucket = get_ready::<D>(session_id, &*client_data);

                let bucket_guard = bucket.lock();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    request.timed_out.store(true, Ordering::Relaxed);

                    if let Some(sent_rqs) = &node.parent_node().sent_rqs {
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        if bucket.contains_key(&req_key) {
                            error!("{:?} // Request {:?} of session {:?} was SENT and timed OUT! Request key {}", node_id,
                    rq_id, session_id,get_request_key(session_id, rq_id));
                        };
                    } else {
                        error!("{:?} // Request {:?} of session {:?} was NOT SENT and timed OUT! Request key {}", node_id,
                    rq_id, session_id, get_request_key(session_id, rq_id));
                    }
                } else {
                    if let Some(sent_rqs) = &node.parent_node().sent_rqs {
                        //Cleanup
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        bucket.remove(&req_key);
                    }
                }
            }

            {
                let bucket = get_ready_callback::<D>(session_id, &*client_data);

                let bucket_guard = bucket.lock();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    request.timed_out.store(true, Ordering::Relaxed);

                    if let Some(sent_rqs) = &node.parent_node().sent_rqs {
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        if bucket.contains_key(&req_key) {
                            error!("{:?} // Request {:?} of session {:?} was SENT and timed OUT!", node_id,
                    rq_id, session_id);
                        };
                    } else {
                        error!("{:?} // Request {:?} of session {:?} was NOT SENT and timed OUT!", node_id,
                    rq_id, session_id);
                    }
                } else {
                    //Cleanup
                    if let Some(sent_rqs) = &node.parent_node().sent_rqs {
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        bucket.remove(&req_key);
                    }
                }
            }
        });
    }

    ///This task might become a large bottleneck with the scenario of few clients with high concurrent rqs,
    /// As the replicas will make very large batches and respond to all the sent requests in one go.
    /// This leaves this thread with a very large task to do in a very short time and it just can't keep up
    fn message_recv_task(
        params: SystemParams,
        data: Arc<ClientData<D::Reply>>,
        node: Arc<Node<D>>,
    ) {
        // use session id as key
        let mut last_operation_ids: IntMap<SeqNo> = IntMap::new();
        let mut replica_votes: IntMap<ReplicaVotes> = IntMap::new();

        while let Ok(message) = node.receive_from_replicas() {
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
                            if last_operation_id >= operation_id {
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

                                {
                                    let ready_callback = get_ready_callback::<D>(session_id, &*data);

                                    let mut ready_callback_lock = ready_callback.lock();

                                    if ready_callback_lock.contains_key(request_key) {
                                        let callback = ready_callback_lock.remove(request_key).unwrap();

                                        //FIXME: If this callback executes heavy or blocking operations,
                                        //This will block the receiving thread, meaning request processing
                                        //Can be hampered.
                                        //So to fix this, move this to a threadpool or just to another thread.
                                        (callback.to_call)(payload);

                                        if callback.timed_out.load(Ordering::Relaxed) {
                                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                                node.id(), session_id, operation_id);
                                        }

                                        continue;
                                    }
                                }

                                let mut ready = get_ready::<D>(session_id, &*data).lock();
                                let request = IntMapEntry::get(request_key, &mut *ready)
                                    .or_insert_with(|| Ready { payload: None, waker: None, timed_out: AtomicBool::new(false) });

                                // register response
                                request.payload = Some(payload);

                                // try to wake up a waiting task
                                if let Some(waker) = request.waker.take() {
                                    waker.wake();
                                }

                                if request.timed_out.load(Ordering::Relaxed) {
                                    error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                        node.id(), session_id, operation_id);
                                }
                            }
                        }
                        // FIXME: handle rogue messages on clients
                        _ => panic!("rogue message detected"),
                    }
                }
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

#[inline]
fn get_ready<D: SharedData>(session_id: SeqNo, data: &ClientData<D::Reply>) -> &Mutex<IntMap<Ready<D::Reply>>> {
    let session_id: usize = session_id.into();
    let index = session_id % data.ready.len();
    &data.ready[index]
}

#[inline]
fn get_ready_callback<D: SharedData>(session_id: SeqNo, data: &ClientData<D::Reply>) -> &Mutex<IntMap<Callback<D::Reply>>> {
    let session_id: usize = session_id.into();
    let index = session_id % data.callback_ready.len();

    &data.callback_ready[index]
}

struct IntMapEntry<'a, T> {
    key: u64,
    map: &'a mut IntMap<T>,
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
