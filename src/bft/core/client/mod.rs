//! Contains the client side core protocol logic of `febft`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_timer::Delay;
use intmap::IntMap;
use log::error;

use crate::bft::communication::message::{Message, RequestMessage, SystemMessage};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::{Node, NodeConfig, NodeId};
use crate::bft::core::client::observing_client::ObserverClient;
use crate::bft::crypto::hash::Digest;
use crate::bft::error::*;
use crate::bft::ordering::{SeqNo, Orderable};

use self::unordered_client::{FollowerData, UnorderedClientMode};

use super::SystemParams;

pub mod observing_client;
pub mod ordered_client;
pub mod unordered_client;

macro_rules! certain {
    ($some:expr) => {
        match $some {
            Some(x) => x,
            None => unreachable!(),
        }
    };
}

struct SentRequestInfo {
    //The amount of replicas/followers we sent the request to
    target_count: usize,
    //The amount of responses that we need to deliver the received response to the application
    //Delivers the response to the client when we have # of equal received responses == responses_needed
    responses_needed: usize,
}

enum ClientAwaker<P> {
    Callback(Callback<P>),
    Ready(Option<Ready<P>>)
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

pub struct ClientData<D>
where
    D: SharedData + 'static,
{
    //The global session counter, so we don't have two client objects with the same session number
    session_counter: AtomicU32,

    //Follower data
    follower_data: Arc<FollowerData>,

    //Information about the requests that were sent like to how many replicas were
    //they sent, how many responses they need, etc
    request_info: Vec<Mutex<IntMap<SentRequestInfo>>>,

    //The ready items for requests made by this client. This is what is going to be used by the message receive task
    //To call the awaiting tasks
    ready: Vec<Mutex<IntMap<Ready<D::Reply>>>>,
    callback_ready: Vec<Mutex<IntMap<Callback<D::Reply>>>>,

    //ready: Vec<Mutex<IntMap<ClientAwaker<D::Reply>>>>,

    //We only want to have a single observer client for any and all sessions that the user
    //May have, so we keep this reference in here
    observer: Arc<Mutex<Option<ObserverClient>>>,
    observer_ready: Mutex<Option<observing_client::Ready>>,
}

pub trait ClientType<D: SharedData + 'static> {
    ///Initialize request in accordance with the type of clients
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> SystemMessage<D::State, D::Request, D::Reply>;

    ///The return types for the iterator
    type Iter: Iterator<Item = NodeId>;

    ///Initialize the targets for the requests according with the type of request made
    ///
    /// Returns the iterator along with the amount of items contained within it
    fn init_targets(client: &Client<D>) -> (Self::Iter, usize);

    fn needed_responses(client: &Client<D>) -> usize;
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData + 'static> {
    session_id: SeqNo,
    operation_counter: SeqNo,
    data: Arc<ClientData<D>>,
    params: SystemParams,
    node: Arc<Node<D>>,
}

impl<D: SharedData> Clone for Client<D> {
    fn clone(&self) -> Self {
        let session_id = self
            .data
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
        self.ready
            .try_lock()
            .map(|mut ready| {
                let request =
                    IntMapEntry::get(self.request_key, &mut *ready).or_insert_with(|| Ready {
                        payload: None,
                        waker: None,
                        timed_out: AtomicBool::new(false),
                    });

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
            .unwrap_or_else(|_| {
                //Failed to get the lock, try again
                cx.waker().wake_by_ref();
                Poll::Pending
            })
    }
}

/// Represents a configuration used to bootstrap a `Client`.
pub struct ClientConfig {
    unordered_rq_mode: UnorderedClientMode,

    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}

///Keeps track of the replica (or follower, depending on the request mode) votes for a given request
pub struct ReplicaVotes {
    //How many nodes did we contact in total with this request, so we can keep track if they have all responded
    contacted_nodes: usize,
    //How many votes do we need to provide a response?
    needed_votes_count: usize,
    //Which replicas have already voted?
    voted: BTreeSet<NodeId>,
    //The different digests we have received and how many times we have seen them
    digests: BTreeMap<Digest, usize>,
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
        let ClientConfig {
            node: node_config,
            unordered_rq_mode,
        } = cfg;

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
            follower_data: Arc::new(FollowerData::empty(unordered_rq_mode)),

            request_info: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            callback_ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            observer: Arc::new(Mutex::new(None)),
            observer_ready: Mutex::new(None),
        });

        let task_data = Arc::clone(&data);

        let cli_node = node.clone();

        // spawn receiving task
        std::thread::Builder::new()
            .name(format!("Client {:?} message processing thread", node.id()))
            .spawn(move || Self::message_recv_task(params, task_data, node))
            .expect("Failed to launch message processing thread");

        let session_id = data.session_counter.fetch_add(1, Ordering::Relaxed).into();

        Ok(Client {
            data,
            params,
            session_id,
            node: cli_node,
            //Start at one, since when receiving we check if received_op_id >= last_op_id, which is by default 0
            operation_counter: SeqNo::ZERO.next(),
        })
    }

    ///Bootstrap an observer client and get a reference to the observer client
    pub async fn bootstrap_observer(&mut self) -> &Arc<Mutex<Option<ObserverClient>>> {
        {
            let guard = self.data.observer.lock().unwrap();

            if let None = &*guard {
                drop(guard);

                let observer = ObserverClient::bootstrap_client(self).await;

                let mut guard = self.data.observer.lock().unwrap();

                let _ = guard.insert(observer);
            }
        }

        &self.data.observer
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    pub async fn update_<T>(&mut self, operation: D::Request) -> D::Reply
    where
        T: ClientType<D>,
    {
        let session_id = self.session_id;
        let operation_id = self.next_operation_id();
        let request_key = get_request_key(session_id, operation_id);

        let message = T::init_request(session_id, operation_id, operation);

        let request_info = get_request_info(session_id, &*self.data);

        //get the targets that we are supposed to broadcast the message to
        let (targets, target_count) = T::init_targets(&self);

        {
            let sent_info = SentRequestInfo {
                target_count: target_count,
                responses_needed: T::needed_responses(self),
            };

            let mut request_info_guard = request_info.lock().unwrap();

            request_info_guard.insert(request_key, sent_info);
        }

        // broadcast our request to the node group
        self.node.broadcast(message, targets);

        // await response
        let ready = get_ready::<D>(session_id, &*self.data);

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );

        ClientRequestFut { request_key, ready }.await
    }

    /// Updates the replicated state of the application running
    /// on top of `febft`.
    //
    // TODO: request timeout
    pub async fn update(&mut self, operation: D::Request) -> D::Reply {
        let session_id = self.session_id;
        let operation_id = self.next_operation_id();
        let message =
            SystemMessage::Request(RequestMessage::new(session_id, operation_id, operation));

        // broadcast our request to the node group
        let targets = NodeId::targets(0..self.params.n());
        self.node.broadcast(message, targets);

        // await response
        let request_key = get_request_key(session_id, operation_id);
        let ready = get_ready::<D>(session_id, &*self.data);

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );

        ClientRequestFut { request_key, ready }.await
    }

    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback_<T>(
        &mut self,
        operation: D::Request,
        callback: Box<dyn FnOnce(D::Reply) + Send>,
    ) where
        T: ClientType<D>,
    {
        let session_id = self.session_id;

        let operation_id = self.next_operation_id();

        let message = T::init_request(session_id, operation_id, operation);

        // await response
        let request_key = get_request_key(session_id, operation_id);
        let ready = get_ready_callback::<D>(session_id, &*self.data);

        let callback = Callback {
            to_call: callback,
            timed_out: AtomicBool::new(false),
        };

        //Scope the mutex operations to reduce the lifetime of the guard
        {
            let mut ready_callback_guard = ready.lock().unwrap();

            ready_callback_guard.insert(request_key, callback);
        }

        //We only send the message after storing the callback to prevent us receiving the result without having
        //The callback registered, therefore losing the response
        let (targets, target_count) = T::init_targets(&self);

        let request_info = get_request_info(session_id, &*self.data);

        {
            let sent_info = SentRequestInfo {
                target_count: target_count,
                responses_needed: T::needed_responses(self),
            };

            let mut request_info_guard = request_info.lock().unwrap();

            request_info_guard.insert(request_key, sent_info);
        }

        self.node.broadcast(message, targets);

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );
    }

    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback(
        &mut self,
        operation: D::Request,
        callback: Box<dyn FnOnce(D::Reply) + Send>,
    ) {
        let session_id = self.session_id;

        let operation_id = self.next_operation_id();

        let message =
            SystemMessage::Request(RequestMessage::new(session_id, operation_id, operation));

        // await response
        let request_key = get_request_key(session_id, operation_id);
        let ready = get_ready_callback::<D>(session_id, &*self.data);

        let callback = Callback {
            to_call: callback,
            timed_out: AtomicBool::new(false),
        };

        //Scope the mutex operations to reduce the lifetime of the guard
        {
            let mut ready_callback_guard = ready.lock().unwrap();

            ready_callback_guard.insert(request_key, callback);
        }

        //We only send the message after storing the callback to prevent us receiving the result without having
        //The callback registered, therefore losing the response
        let targets = NodeId::targets(0..self.params.n());

        self.node.broadcast(message, targets);

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );
    }

    fn next_operation_id(&mut self) -> SeqNo {
        let id = self.operation_counter;

        self.operation_counter = self.operation_counter.next();

        id
    }

    fn start_timeout(
        node: Arc<Node<D>>,
        session_id: SeqNo,
        rq_id: SeqNo,
        client_data: Arc<ClientData<D>>,
    ) {
        let node = node.clone();
        let node_id = node.id();

        crate::bft::async_runtime::spawn(async move {
            //Timeout delay
            Delay::new(Duration::from_secs(5)).await;

            let req_key = get_request_key(session_id, rq_id);

            {
                let bucket = get_ready::<D>(session_id, &*client_data);

                let bucket_guard = bucket.lock().unwrap();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    request.timed_out.store(true, Ordering::Relaxed);

                    if let Some(sent_rqs) = &node.sent_rqs {
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
                    if let Some(sent_rqs) = &node.sent_rqs {
                        //Cleanup
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        bucket.remove(&req_key);
                    }
                }
            }

            {
                let bucket = get_ready_callback::<D>(session_id, &*client_data);

                let bucket_guard = bucket.lock().unwrap();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    request.timed_out.store(true, Ordering::Relaxed);

                    if let Some(sent_rqs) = &node.sent_rqs {
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        if bucket.contains_key(&req_key) {
                            error!(
                                "{:?} // Request {:?} of session {:?} was SENT and timed OUT!",
                                node_id, rq_id, session_id
                            );
                        };
                    } else {
                        error!(
                            "{:?} // Request {:?} of session {:?} was NOT SENT and timed OUT!",
                            node_id, rq_id, session_id
                        );
                    }
                } else {
                    //Cleanup
                    if let Some(sent_rqs) = &node.sent_rqs {
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
    fn message_recv_task(params: SystemParams, data: Arc<ClientData<D>>, node: Arc<Node<D>>) {
        // use session id as key
        let mut last_operation_ids: IntMap<SeqNo> = IntMap::new();
        let mut replica_votes: IntMap<ReplicaVotes> = IntMap::new();

        while let Ok(message) = node.receive_from_replicas() {
            match message {
                Message::System(header, message) => {

                    match &message {
                        SystemMessage::Reply(msg_info) | SystemMessage::UnOrderedReply(msg_info) => {
                            
                            let session_id = msg_info.session_id();
                            let operation_id = msg_info.sequence_number();

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
                                .or_insert_with(|| {
                                    let request_info = get_request_info(session_id, &*data);

                                    let mut request_info_guard = request_info.lock().unwrap();

                                    let rq_info = request_info_guard.remove(request_key);

                                    if let Some(rq_info) = rq_info {
                                        //If we have information about the request in question,
                                        //Utilize it
                                        ReplicaVotes {
                                            contacted_nodes: rq_info.target_count,
                                            needed_votes_count: rq_info.responses_needed,
                                            voted: Default::default(),
                                            digests: Default::default(),
                                        }
                                    } else {
                                        ReplicaVotes {
                                            needed_votes_count: params.f() + 1,
                                            voted: Default::default(),
                                            digests: Default::default(),
                                            contacted_nodes: params.n(),
                                        }
                                    }
                                });

                                //Check if replicas try to vote twice on the same consensus instance
                            if votes.voted.contains(&header.from()) {
                                error!(
                                    "Replica {:?} voted twice for the same request, ignoring!",
                                    header.from()
                                );
                                continue;
                            }

                            votes.voted.insert(header.from());

                            //Get how many equal responses we have received and see if we can deliver the state to the client
                            let count = if votes.digests.contains_key(header.digest()) {
                                //Increment the amount of votes that reply has
                                *(votes.digests.get_mut(header.digest()).unwrap()) += 1;

                                votes.digests.get(header.digest()).unwrap().clone()
                            } else {
                                //Register the newly received reply
                                votes.digests.insert(header.digest().clone(), 1);

                                1
                            };

                            // wait for the amount of votes that we require identical replies
                            // In a BFT system, this is by default f+1
                            if count >= votes.needed_votes_count {
                                replica_votes.remove(request_key);

                                last_operation_ids.remove(session_id.into());
                                last_operation_ids.insert(session_id.into(), operation_id);

                                let (_, _, payload) = match message {
                                    SystemMessage::Reply(msg) | SystemMessage::UnOrderedReply(msg) => {
                                        msg.into_inner()
                                    },
                                    _ => {
                                        unreachable!()
                                    }
                                };

                                //Check the callbacks to see if there is something we can call
                                //If there is
                                {
                                    let ready_callback =
                                        get_ready_callback::<D>(session_id, &*data);

                                    let mut ready_callback_lock = ready_callback.lock().unwrap();

                                    if ready_callback_lock.contains_key(request_key) {
                                        let callback =
                                            ready_callback_lock.remove(request_key).unwrap();

                                        //FIXME: If this callback executes heavy or blocking operations,
                                        //This will block the receiving thread, meaning request processing
                                        //Can be hampered.
                                        //So to fix this, move this to a threadpool or just to another thread.
                                        (callback.to_call)(payload);

                                        if callback.timed_out.load(Ordering::Relaxed) {
                                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                                node.id(), session_id, operation_id);
                                        }

                                        //The response has been successfully delivered to the callback, do not process this
                                        //Message any further
                                        continue;
                                    }
                                }

                                //Check the async await
                                {
                                    let mut ready =
                                        get_ready::<D>(session_id, &*data).lock().unwrap();

                                    let request = IntMapEntry::get(request_key, &mut *ready)
                                        .or_insert_with(|| Ready {
                                            payload: None,
                                            waker: None,
                                            timed_out: AtomicBool::new(false),
                                        });

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

                            continue
                        },
                        _ => {}
                    }

                    match message {
                        SystemMessage::ObserverMessage(message) => {
                            let msg =
                                Message::System(header, SystemMessage::ObserverMessage(message));

                            //Pass this message off to the observing module
                            ObserverClient::handle_observed_message(&data, msg);
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
fn get_correct_vec_for<T>(session_id: SeqNo, vec: &Vec<Mutex<T>>) -> &Mutex<T> {
    let session_id: usize = session_id.into();
    let index = session_id % vec.len();

    &vec[index]
}

#[inline]
fn get_ready<D: SharedData>(
    session_id: SeqNo,
    data: &ClientData<D>,
) -> &Mutex<IntMap<Ready<D::Reply>>> {
    get_correct_vec_for(session_id, &data.ready)
}

#[inline]
fn get_ready_callback<D: SharedData>(
    session_id: SeqNo,
    data: &ClientData<D>,
) -> &Mutex<IntMap<Callback<D::Reply>>> {
    get_correct_vec_for(session_id, &data.callback_ready)
}

#[inline]
fn get_request_info<D: SharedData>(
    session_id: SeqNo,
    data: &ClientData<D>,
) -> &Mutex<IntMap<SentRequestInfo>> {
    get_correct_vec_for(session_id, &data.request_info)
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
