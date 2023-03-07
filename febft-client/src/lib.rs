//! Contains the client side core protocol logic of `febft`.
#![feature(type_alias_impl_trait)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{Read, Write};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use febft_common::crypto::hash::Digest;

use intmap::IntMap;
use log::{error};
use febft_common::async_runtime;

use febft_common::error::*;
use febft_common::globals::SystemParams;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::benchmarks::ClientPerf;
use febft_communication::{Node, NodeConfig, NodeId};
use febft_communication::message::{NetworkMessage, NetworkMessageContent};
use febft_communication::serialize::Serializable;
use febft_execution::executable::{Reply, Request, Service};
use febft_execution::serialize::SharedData;
use febft_messages::messages::{ReplyMessage, SystemMessage};
use febft_messages::serialization::ProtocolData;

#[cfg(feature = "observer_client")]
use crate::observing_client::{ObserverClient, ObserverClientData};

use self::unordered_client::{FollowerData, UnorderedClientMode};


#[cfg(feature = "observer_client")]
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

///Represents the possible ways a client can be notified of replies that were delivered to him
enum ClientAwaker<P> {
    //Callbacks have to be set from the start, since they are passed along with the request when the client issues it
    //So we always contain the callback struct
    Callback(Callback<P>),
    //Requests performed asynchronously however are a bit different. There can be 2 possibilities:
    // Client makes request, performs await (which populates the ready option) and then the responses are received and delivered to the client
    // Client makes requests and does not immediately perform await, the responses are received and the ready is populated with the responses
    // When the client awaits, it will see that there is already a Ready populated so it instantly delivers the payload to the client.
    Async(Option<Ready<P>>),
}

struct Ready<P> {
    waker: Option<Waker>,
    payload: Option<Result<P>>,
    timed_out: AtomicBool,
}

struct Callback<P> {
    to_call: Box<dyn FnOnce(Result<P>) + Send>,
    timed_out: AtomicBool,
}

impl<P> Deref for Callback<P> {
    type Target = Box<dyn FnOnce(Result<P>) + Send>;

    fn deref(&self) -> &Self::Target {
        &self.to_call
    }
}

pub struct NoProtocol;

impl Serializable for NoProtocol {
    type Message = ();

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()> {
        Ok(())
    }

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message> {
        Ok(())
    }
}

impl ProtocolData for NoProtocol {
    type Message = ();

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()> {
        Ok(())
    }

    fn deserialize<R: Read>(r: R) -> Result<Self::Message> {
        Ok(())
    }
}

pub struct ClientData<D>
    where
        D: SharedData + 'static,
{
    //The global session counter, so we don't have two client objects with the same session number
    session_counter: AtomicU32,

    //Follower data
    follower_data: FollowerData,

    //Information about the requests that were sent like to how many replicas were
    //they sent, how many responses they need, etc
    request_info: Vec<Mutex<IntMap<SentRequestInfo>>>,

    //The ready items for requests made by this client. This is what is going to be used by the message receive task
    //To call the awaiting tasks
    ready: Vec<Mutex<IntMap<ClientAwaker<D::Reply>>>>,

    //We only want to have a single observer client for any and all sessions that the user
    //May have, so we keep this reference in here
    #[cfg(feature = "observer_client")]
    observer: ObserverClientData,

    stats: Option<Arc<ClientPerf>>,
}


pub trait ClientType<D: SharedData + 'static> {
    ///Initialize request in accordance with the type of clients
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> SystemMessage<D, NoProtocol>;

    ///The return types for the iterator
    type Iter: Iterator<Item=NodeId>;

    ///Initialize the targets for the requests according with the type of request made
    ///
    /// Returns the iterator along with the amount of items contained within it
    fn init_targets(client: &Client<D>) -> (Self::Iter, usize);

    ///How many responses does that client need to get the
    fn needed_responses(client: &Client<D>) -> usize;
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData + 'static> {
    session_id: SeqNo,
    operation_counter: SeqNo,
    data: Arc<ClientData<D>>,
    params: SystemParams,
    node: Arc<Node<SystemMessage<D, NoProtocol>>>,

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
    ready: &'a Mutex<IntMap<ClientAwaker<P>>>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = Result<P>;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ready
            .try_lock()
            .map(|mut ready| {
                let request = ready
                    .get_mut(self.request_key)
                    .expect("Request must be present here, how is this possible?");

                let request = match request {
                    ClientAwaker::Async(awaker) => {
                        if let None = awaker {
                            //If there is still no Ready inserted, insert now so it can later be fetched
                            *awaker = Some(Ready {
                                waker: None,
                                payload: None,
                                timed_out: AtomicBool::new(false),
                            });
                        }

                        //If there was already an inserted Ready struct, then the response is probably already ready to be received

                        awaker.as_mut().unwrap()
                    }
                    ClientAwaker::Callback(_) => unreachable!(),
                };

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
    pub unordered_rq_mode: UnorderedClientMode,

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
        D: SharedData + 'static
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

        let stats = {
            None
            //Some(Arc::new(ClientPerf::new()))
        };

        // create shared data
        let data = Arc::new(ClientData {
            session_counter: AtomicU32::new(0),
            follower_data: FollowerData::empty(unordered_rq_mode),

            request_info: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            #[cfg(feature = "observer_client")]
            observer: ObserverClientData::new(),
            stats,
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

    #[inline]
    pub fn id(&self) -> NodeId {
        self.node.id()
    }

    /// Updates the replicated state of the application running
    /// on top of `febft`.
    //
    pub async fn update<T>(&mut self, operation: D::Request) -> Result<D::Reply>
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
        self.node.broadcast(NetworkMessageContent::System(message), targets);

        // await response
        let ready = get_ready::<D>(session_id, &*self.data);

        {
            let mut ready_stored = ready.lock().unwrap();

            ready_stored.insert(request_key, ClientAwaker::Async(None));
        }

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
    pub fn update_callback<T>(
        &mut self,
        operation: D::Request,
        callback: Box<dyn FnOnce(Result<D::Reply>) + Send>,
    ) where
        T: ClientType<D>,
    {
        let session_id = self.session_id;

        let operation_id = self.next_operation_id();

        //start_measurement!(init_rq);

        let message = T::init_request(session_id, operation_id, operation);

        //measure_time_rq_init!(&self.data.stats, init_rq);


        //start_measurement!(target_init);

        // await response
        let request_key = get_request_key(session_id, operation_id);

        //We only send the message after storing the callback to prevent us receiving the result without having
        //The callback registered, therefore losing the response
        let (targets, target_count) = T::init_targets(&self);

        //measure_target_init_time!(&self.data.stats, target_init);


        //start_measurement!(rq_info_init);

        let request_info = get_request_info(session_id, &*self.data);

        {
            let sent_info = SentRequestInfo {
                target_count,
                responses_needed: T::needed_responses(self),
            };

            let mut request_info_guard = request_info.lock().unwrap();

            request_info_guard.insert(request_key, sent_info);
        }

        //measure_sent_rq_info!(&self.data.stats, rq_info_init);


        //start_measurement!(rq_ready_init);

        let ready = get_ready::<D>(session_id, &*self.data);

        let callback = Callback {
            to_call: callback,
            timed_out: AtomicBool::new(false),
        };

        //Scope the mutex operations to reduce the lifetime of the guard
        {
            let mut ready_callback_guard = ready.lock().unwrap();

            ready_callback_guard.insert(request_key, ClientAwaker::Callback(callback));
        }

        //measure_ready_rq_time!(&self.data.stats, rq_ready_init);

        self.node.broadcast(NetworkMessageContent::System(message), targets);

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

    ///Start performing a timeout for a given request.
    /// TODO: Repeat the request/do something else to fix this
    fn start_timeout(
        node: Arc<Node<SystemMessage<D, NoProtocol>>>,
        session_id: SeqNo,
        rq_id: SeqNo,
        client_data: Arc<ClientData<D>>,
    ) {
        let node = node.clone();
        let node_id = node.id();

        async_runtime::spawn(async move {
            //Timeout delay
            //Delay::new(Duration::from_secs(5)).await;

            let req_key = get_request_key(session_id, rq_id);

            {
                let bucket = get_ready::<D>(session_id, &*client_data);

                let bucket_guard = bucket.lock().unwrap();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    match request {
                        ClientAwaker::Callback(request) => {
                            request.timed_out.store(true, Ordering::SeqCst)
                        }
                        ClientAwaker::Async(Some(ready)) => {
                            ready.timed_out.store(true, Ordering::SeqCst)
                        }
                        ClientAwaker::Async(None) => {
                            //TODO: This has to be handled (should populate the ready with an empty, but timed out ready)
                        }
                    }

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
        });
    }

    /// Create the default replica vote struct
    fn create_replica_votes(
        request_info: &Mutex<IntMap<SentRequestInfo>>,
        request_key: u64,
        params: &SystemParams,
    ) -> ReplicaVotes {
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
            //If there is no stored information, take the safe road and require f + 1 votes
            ReplicaVotes {
                contacted_nodes: params.n(),
                needed_votes_count: params.f() + 1,
                voted: Default::default(),
                digests: Default::default(),
            }
        }
    }

    ///Deliver the reponse to the client
    fn deliver_response(
        node_id: NodeId,
        request_key: u64,
        ready: &Mutex<IntMap<ClientAwaker<D::Reply>>>,
        message: ReplyMessage<D::Reply>,
    ) {
        let mut ready_lock = ready.lock().unwrap();

        let request = ready_lock.get_mut(request_key);

        let (session_id, operation_id, payload) = message.into_inner();

        if let Some(request) = request {
            match request {
                ClientAwaker::Callback(_) => {
                    //This is impossible to fail since we are in the Some method of the request
                    let request = ready_lock.remove(request_key).unwrap();

                    let request = match request {
                        ClientAwaker::Callback(request) => request,
                        _ => unreachable!(),
                    };

                    if request.timed_out.load(Ordering::Relaxed) {
                        error!(
                            "{:?} // Received response to timed out request {:?} on session {:?}",
                            node_id, session_id, operation_id
                        );
                    }

                    (request.to_call)(Ok(payload));
                }
                ClientAwaker::Async(opt_ready) => {
                    if let Some(request) = opt_ready.as_mut() {
                        // register response
                        request.payload = Some(Ok(payload));

                        if request.timed_out.load(Ordering::Relaxed) {
                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                        node_id, session_id, operation_id);
                        }

                        // try to wake up the waiting task
                        if let Some(waker) = request.waker.take() {
                            waker.wake();
                        }
                    } else {
                        let request = Ready {
                            waker: None,
                            payload: Some(Ok(payload)),
                            timed_out: AtomicBool::new(false),
                        };

                        //populate the data with the received payload, even though the
                        //
                        *opt_ready = Some(request);
                    }
                }
            }
        } else {
            error!("Failed to get awaker for request {:?}", request_key)
        }
    }

    ///Deliver an error response
    fn deliver_error(
        node_id: NodeId,
        request_key: u64,
        ready: &Mutex<IntMap<ClientAwaker<D::Reply>>>,
        (session_id, operation_id): (SeqNo, SeqNo),
    ) {
        let mut ready_lock = ready.lock().unwrap();

        let request = ready_lock.get_mut(request_key);

        let err_msg = Err("Could not get f+1 equal responses, failed to execute the request")
            .wrapped(ErrorKind::CoreClient);

        if let Some(request) = request {
            match request {
                ClientAwaker::Callback(_) => {
                    //This is impossible to fail since we are in the Some method of the request
                    let request = ready_lock.remove(request_key).unwrap();

                    let request = match request {
                        ClientAwaker::Callback(request) => request,
                        _ => unreachable!(),
                    };

                    if request.timed_out.load(Ordering::Relaxed) {
                        error!(
                            "{:?} // Received response to timed out request {:?} on session {:?}",
                            node_id, session_id, operation_id
                        );
                    }

                    (request.to_call)(err_msg);
                }
                ClientAwaker::Async(opt_ready) => {
                    if let Some(request) = opt_ready.as_mut() {
                        // register response
                        request.payload = Some(err_msg);

                        if request.timed_out.load(Ordering::Relaxed) {
                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                    node_id, session_id, operation_id);
                        }

                        // try to wake up the waiting task
                        if let Some(waker) = request.waker.take() {
                            waker.wake();
                        }
                    } else {
                        let request = Ready {
                            waker: None,
                            payload: Some(err_msg),
                            timed_out: AtomicBool::new(false),
                        };

                        //populate the data with the received payload
                        *opt_ready = Some(request);
                    }
                }
            }
        } else {
            error!("Failed to get awaker for request {:?}", request_key)
        }
    }

    ///This task might become a large bottleneck with the scenario of few clients with high concurrent rqs,
    /// As the replicas will make very large batches and respond to all the sent requests in one go.
    /// This leaves this thread with a very large task to do in a very short time and it just can't keep up
    fn message_recv_task(params: SystemParams, data: Arc<ClientData<D>>, node: Arc<Node<SystemMessage<D, NoProtocol>>>) {
        // use session id as key
        let mut last_operation_ids: IntMap<SeqNo> = IntMap::new();
        let mut replica_votes: IntMap<ReplicaVotes> = IntMap::new();

        while let Ok(message) = node.receive_from_replicas() {
            let (header, message) = message.into_inner();

            let sys_msg = match message {
                NetworkMessageContent::System(sys) => { sys }
                NetworkMessageContent::Ping(_) => { continue; }
            };

            match &sys_msg {
                SystemMessage::OrderedReply(msg_info)
                | SystemMessage::UnorderedReply(msg_info) => {
                    let session_id = msg_info.session_id();
                    let operation_id = msg_info.sequence_number();

                    //start_measurement!(start_time);

                    //Check if we have already executed the operation
                    let last_operation_id = last_operation_ids
                        .get(session_id.into())
                        .copied()
                        .unwrap_or(SeqNo::ZERO);

                    // reply already delivered to application
                    if last_operation_id >= operation_id {
                        continue;
                    }

                    let request_key = get_request_key(session_id, operation_id);

                    //Get the votes for the instance
                    let votes = IntMapEntry::get(request_key, &mut replica_votes)
                        .or_insert_with(|| {
                            let request_info = get_request_info(session_id, &*data);

                            Self::create_replica_votes(request_info, request_key, &params)
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
                        //Register the newly received reply (has not been seen yet)
                        votes.digests.insert(header.digest().clone(), 1);

                        1
                    };

                    //measure_response_rcv_time!(&data.stats, start_time);

                    // wait for the amount of votes that we require identical replies
                    // In a BFT system, this is by default f+1
                    if count >= votes.needed_votes_count {
                        //start_measurement!(start_time);

                        replica_votes.remove(request_key);

                        last_operation_ids.remove(session_id.into());
                        last_operation_ids.insert(session_id.into(), operation_id);

                        //Get the wakers for this request and deliver the payload

                        let ready = get_ready::<D>(session_id, &*data);

                        Self::deliver_response(
                            node.id(),
                            request_key,
                            ready,
                            match sys_msg {
                                SystemMessage::OrderedReply(message) | SystemMessage::UnorderedReply(message) => message,
                                _ => unreachable!(),
                            },
                        );

                        // measure_response_deliver_time!(&data.stats, start_time);
                    } else {
                        //If we do not have f+1 replies yet, check if it's still possible to get those
                        //Replies by taking a look at the target count and currently received replies count

                        let mut total_count: usize = 0;

                        for (_, count) in votes.digests.iter() {
                            total_count += count;
                        }

                        if total_count >= votes.contacted_nodes {
                            //We already got all of our responses, so it's impossible to get f+1 equal reponses
                            //What we will do now is call the awakers with an Err result
                            replica_votes.remove(request_key);

                            //Clean up the memory corresponding to this request
                            last_operation_ids.remove(session_id.into());
                            last_operation_ids.insert(session_id.into(), operation_id);

                            //Get the wakers for this request and deliver the payload
                            let ready = get_ready::<D>(session_id, &*data);

                            Self::deliver_error(
                                node.id(),
                                request_key,
                                ready,
                                (session_id, operation_id),
                            );
                        }
                    }

                    continue;
                }
                _ => {}
            }

            #[cfg(feature = "observer_client")]
            {
                match &sys_msg {
                    SystemMessage::Protocol(protocol_msg) => {
                        todo!();
                        //Pass this message off to the observing module
                        ObserverClient::handle_observed_message(&data, NetworkMessage::new(header, NetworkMessageContent::System(sys_msg)));
                    }
                    // FIXME: handle rogue messages on clients
                    _ => panic!("rogue message detected"),
                }
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
) -> &Mutex<IntMap<ClientAwaker<D::Reply>>> {
    get_correct_vec_for(session_id, &data.ready)
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
