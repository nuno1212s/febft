use std::cell::RefCell;
use std::fmt::{Debug, format};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, SendError};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{RecvTimeoutError, TryRecvError};
use dsrust::channels::async_ch::ReceiverMultFut;
use dsrust::channels::queue_channel::{bounded_lf_queue, make_mult_recv_from, make_mult_recv_partial_from, Receiver, ReceiverMult, ReceiverPartialMult, RecvMultError, Sender};
use dsrust::queues::lf_array_queue::LFBQueue;
use dsrust::queues::mqueue::MQueue;
use dsrust::queues::queues::{BQueue, PartiallyDumpable, Queue, SizableQueue};
use dsrust::utils::backoff::BackoffN;
use futures::select;
use futures_timer::Delay;
use intmap::IntMap;
use log::{debug, info};
use parking_lot::{Mutex, RawMutex, RwLock};
use parking_lot::lock_api::MutexGuard;

use crate::bft::async_runtime as rt;
use crate::bft::communication::{NODE_CHAN_BOUND, NodeConfig, NodeId};
use crate::bft::communication::channel::{ChannelRx, ChannelTx, new_bounded};
use crate::bft::communication::message::Message;
use crate::bft::error::*;
use crate::bft::threadpool;

///Handles the communication between two peers (replica - replica, replica - client)
/// Only handles reception of requests, not transmission

type QueueType<T> = LFBQueue<Vec<T>>;

type ReplicaQueueType<T> = LFBQueue<T>;

type ClientQueueType<T> = MQueue<T>;

type ClientSender<T> = crossbeam_channel::Sender<T>;
type ClientReceiver<T> = crossbeam_channel::Receiver<T>;

fn channel_init<T>(capacity: usize) -> (Sender<Vec<T>, QueueType<T>>, Receiver<Vec<T>, QueueType<T>>) {
    dsrust::channels::queue_channel::bounded_lf_queue(capacity)
}

fn replica_channel_init<T>(capacity: usize) -> (Sender<T, ReplicaQueueType<T>>, Receiver<T, ReplicaQueueType<T>>) {
    let (tx, rx) = bounded_lf_queue(capacity);

    (tx, rx)
}

fn client_channel_init<T>(capacity: usize) -> (Sender<T, ClientQueueType<T>>, ReceiverPartialMult<T, ClientQueueType<T>>) {
    let (tx, rx) = dsrust::channels::queue_channel::bounded_mutex_backoff_queue(capacity);

    (tx, make_mult_recv_partial_from(rx))
}

pub struct NodePeers<T: Send + 'static> {
    batch_size: usize,
    first_cli: NodeId,
    own_id: NodeId,
    peer_loopback: Arc<ConnectedPeer<T>>,
    replica_handling: Arc<ReplicaHandling<T>>,
    client_handling: Option<Arc<ConnectedPeersGroup<T>>>,
    client_tx: Option<ClientSender<Vec<T>>>,
    client_rx: Option<ClientReceiver<Vec<T>>>,
}

const DEFAULT_CLIENT_QUEUE: usize = 1024;
const DEFAULT_REPLICA_QUEUE: usize = 1024;

///We make this class Sync and send since the clients are going to be handled by a single class
///And the replicas are going to be handled by another class.
/// There is no possibility of 2 threads accessing the client_rx or replica_rx concurrently
unsafe impl<T> Sync for NodePeers<T> where T: Send {}

unsafe impl<T> Send for NodePeers<T> where T: Send {}

impl<T> NodePeers<T> where T: Send {
    pub fn new(id: NodeId, first_cli: NodeId, batch_size: usize) -> NodePeers<T> {
        //We only want to setup client handling if we are a replica
        let client_handling;

        let client_channel;

        if id < first_cli {
            let (client_tx, client_rx) = crossbeam_channel::bounded(NODE_CHAN_BOUND);

            client_handling = Some(ConnectedPeersGroup::new(DEFAULT_CLIENT_QUEUE,
                                                            batch_size,
                                                            client_tx.clone(),
                                                            id));
            client_channel = Some((client_tx, client_rx));
        } else {
            client_handling = None;
            client_channel = None;
        };

        //TODO: Batch size is not correct, should be the value found in env
        //Both replicas and clients have to interact with replicas, so we always need this pool
        //We have a much larger queue because we don't want small slowdowns slowing down the connections
        //And also because there are few replicas, while there can be a very large amount of clients
        let replica_handling = ReplicaHandling::new(NODE_CHAN_BOUND);

        let loopback_address = replica_handling.init_client(id);

        let (cl_tx, cl_rx) = if let Some((cl_tx, cl_rx)) = client_channel {
            (Some(cl_tx), Some(cl_rx))
        } else {
            (None, None)
        };

        let peers = NodePeers {
            batch_size,
            first_cli,
            own_id: id,
            peer_loopback: loopback_address,
            replica_handling,
            client_handling,
            client_tx: cl_tx,
            client_rx: cl_rx,
        };

        peers
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn init_peer_conn(&self, peer: NodeId) -> Arc<ConnectedPeer<T>> {
        //debug!("Initializing peer connection for peer {:?} on peer {:?}", peer, self.own_id);

        return if peer >= self.first_cli {
            self.client_handling.as_ref().expect("Tried to init client request from client itself?")
                .init_client(peer)
        } else {
            self.replica_handling.init_client(peer)
        };
    }

    pub fn resolve_peer_conn(&self, peer: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        if peer == self.own_id {
            return Some(self.peer_loopback.clone());
        }

        return if peer < self.first_cli {
            self.replica_handling.resolve_connection(peer)
        } else {
            self.client_handling.as_ref().expect("Tried to resolve client conn in the client")
                .get_client_conn(peer)
        };
    }

    pub fn peer_loopback(&self) -> &Arc<ConnectedPeer<T>> {
        &self.peer_loopback
    }

    pub fn rqs_len_from_clients(&self) -> usize {
        return match &self.client_rx {
            None => { 0 }
            Some(rx) => {
                rx.len()
            }
        };
    }

    pub fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<T>> {
        return match &self.client_rx {
            None => {
                Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to receive from clients as there are no clients connected"))
            }
            Some(rx) => {
                match timeout {
                    None => {
                        match rx.recv() {
                            Ok(vec) => {
                                Ok(vec)
                            }
                            Err(_) => {
                                Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to receive"))
                            }
                        }
                    }
                    Some(timeout) => {
                        match rx.recv_timeout(timeout) {
                            Ok(vec) => {
                                Ok(vec)
                            }
                            Err(err) => {
                                match err {
                                    RecvTimeoutError::Timeout => {
                                        Ok(vec![])
                                    }
                                    RecvTimeoutError::Disconnected => {
                                        Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to receive"))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    pub fn try_receive_from_clients(&self) -> Result<Option<Vec<T>>> {
        return match &self.client_rx {
            None => {
                Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to receive from clients as there are no clients connected"))
            }
            Some(rx) => {
                match rx.try_recv() {
                    Ok(msgs) => {
                        Ok(Some(msgs))
                    }
                    Err(err) => {
                        match err {
                            TryRecvError::Empty => {
                                Ok(None)
                            }
                            TryRecvError::Disconnected => {
                                Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to receive from clients as there are no clients connected"))
                            }
                        }
                    }
                }
            }
        };
    }

    pub fn receive_from_replicas(&self) -> Result<T> {
        Ok(self.replica_handling.receive_from_replicas())
    }

    pub fn client_count(&self) -> Option<usize> {
        return match &self.client_handling {
            None => { None }
            Some(client) => {
                Some(client.connected_clients.load(Ordering::Relaxed))
            }
        };
    }

    pub fn replica_count(&self) -> usize {
        return self.replica_handling.connected_client_count.load(Ordering::Relaxed);
    }
}

///Represents a connected peer
///Can either be a pooled peer with an individual queue and a thread that will collect all requests
///Or an unpooled connection that puts the messages straight into the channel where the consumer
///Will collect.
pub enum ConnectedPeer<T> where T: Send {
    PoolConnection {
        client_id: NodeId,
        sender: Mutex<Option<Sender<T, ClientQueueType<T>>>>,
        receiver: ReceiverPartialMult<T, ClientQueueType<T>>,
    },
    UnpooledConnection {
        client_id: NodeId,
        sender: Mutex<Option<ChannelTx<T>>>,
    },
}

///Handling replicas is different from handling clients
///We want to handle the requests differently as in communication between replicas
///Latency is extremely important and we have to minimize it to the least amount possible
/// So in this implementation, we will just use a single channel with dumping capabilities (Able to remove capacity items in a couple CAS operations,
/// making it much more efficient than just removing 1 at a time and also minimizing concurrency)
/// for all messages
///
/// FIXME: See if having a multiple channel approach with something like a select is
/// worth the overhead of having to pool multiple channels. We may also get problems with fairness.
/// Probably not worth it
pub struct ReplicaHandling<T> where T: Send {
    capacity: usize,
    channel_tx_replica: ChannelTx<T>,
    channel_rx_replica: ChannelRx<T>,
    connected_clients: RwLock<IntMap<Arc<ConnectedPeer<T>>>>,
    connected_client_count: AtomicUsize,
}

impl<T> ReplicaHandling<T> where T: Send {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = new_bounded(capacity);

        Arc::new(
            Self {
                capacity,
                channel_rx_replica: receiver,
                channel_tx_replica: sender,
                connected_clients: RwLock::new(IntMap::new()),
                connected_client_count: AtomicUsize::new(0),
            }
        )
    }

    pub fn init_client(&self, peer_id: NodeId) -> Arc<ConnectedPeer<T>> {
        let peer = Arc::new(ConnectedPeer::UnpooledConnection {
            client_id: peer_id,
            sender: Mutex::new(Some(self.channel_tx_replica.clone())),
        });

        //TODO: Handle replica disconnects
        self.connected_clients.write().insert(peer_id.id() as u64, peer.clone());
        self.connected_client_count.fetch_add(1, Ordering::Relaxed);

        peer
    }

    pub fn resolve_connection(&self, peer_id: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        match self.connected_clients.read().get(peer_id.id() as u64) {
            None => {
                None
            }
            Some(peer) => {
                Some(Arc::clone(peer))
            }
        }
    }

    pub fn receive_from_replicas(&self) -> T {
        self.channel_rx_replica.recv_sync().unwrap()
    }
}

///
///Client pool design, where each pool contains a number of clients (Maximum of BATCH_SIZE clients
/// per pool). This is to prevent starvation for each client, as when we are performing
/// the fair collection of requests from the clients, if there are more clients than batch size
/// then we will get very unfair distribution of requests
///
/// This will push Vecs of T types into the ChannelTx provided
/// The type T is not wrapped in any other way
/// no socket handling is done here
/// This is just built on top of the actual per client connection socket stuff and each socket
/// should push items into its own ConnectedPeer instance
pub struct ConnectedPeersGroup<T: Send + 'static> {
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    client_pools: Mutex<Vec<Arc<ConnectedPeersPool<T>>>>,
    client_connections_cache: RwLock<IntMap<Arc<ConnectedPeer<T>>>>,
    connected_clients: AtomicUsize,
    per_client_cache: usize,
    batch_size: usize,
    batch_transmission: ClientSender<Vec<T>>,
    own_id: NodeId,
}

pub struct ConnectedPeersPool<T: Send + 'static> {
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    connected_clients: Mutex<Vec<Arc<ConnectedPeer<T>>>>,
    batch_size: usize,
    client_limit: usize,
    batch_transmission: ClientSender<Vec<T>>,
    finish_execution: AtomicBool,
    owner: Arc<ConnectedPeersGroup<T>>,
}

impl<T> ConnectedPeersGroup<T> where T: Send + 'static {
    pub fn new(per_client_bound: usize, batch_size: usize, batch_transmission: crossbeam_channel::Sender<Vec<T>>,
               own_id: NodeId) -> Arc<Self> {
        Arc::new(Self {
            client_pools: parking_lot::Mutex::new(Vec::new()),
            client_connections_cache: RwLock::new(IntMap::new()),
            per_client_cache: per_client_bound,
            connected_clients: AtomicUsize::new(0),
            batch_size,
            batch_transmission,
            own_id,
        })
    }

    pub fn init_client(self: &Arc<Self>, peer_id: NodeId) -> Arc<ConnectedPeer<T>> {
        let (sender, receiver) = client_channel_init(self.per_client_cache);

        let connected_client = Arc::new(ConnectedPeer::PoolConnection {
            client_id: peer_id,
            sender: Mutex::new(Option::Some(sender)),
            receiver,
        });

        let mut cached_clients = self.client_connections_cache.write();

        cached_clients.insert(peer_id.0 as u64, connected_client.clone());

        drop(cached_clients);

        self.connected_clients.fetch_add(1, Ordering::SeqCst);

        let mut clone_queue = connected_client.clone();

        let mut guard = self.client_pools.lock();

        for pool in &*guard {
            match pool.attempt_to_add(clone_queue) {
                Ok(_) => {
                    return connected_client;
                }
                Err(queue) => {
                    clone_queue = queue;
                }
            }
        }

        //In the case all the pools are already full, allocate a new pool
        let pool = ConnectedPeersPool::new(self.batch_size,
                                           self.batch_transmission.clone(),
                                           Arc::clone(self));

        match pool.attempt_to_add(clone_queue) {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to add pool to pool list.")
            }
        };

        let pool_clone = pool.clone();

        guard.push(pool);

        let id = guard.len();

        drop(guard);

        pool_clone.start(id as u32);

        connected_client
    }

    pub fn get_client_conn(&self, client_id: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        let cache_guard = self.client_connections_cache.read();

        return match cache_guard.get(client_id.0 as u64) {
            None => {
                None
            }
            Some(peer) => {
                Some(Arc::clone(peer))
            }
        };
    }

    fn del_cached_clients(&self, clients: Vec<NodeId>) {
        let mut cache_guard = self.client_connections_cache.write();

        for client_id in &clients {
            cache_guard.remove(client_id.0 as u64);
        }

        drop(cache_guard);

        self.connected_clients.fetch_sub(clients.len(), Ordering::Relaxed);
    }

    pub fn del_client(&self, client_id: &NodeId) -> bool {
        let mut cache_guard = self.client_connections_cache.write();

        cache_guard.remove(client_id.0 as u64);

        drop(cache_guard);

        let mut guard = self.client_pools.lock();

        for i in 0..guard.len() {
            match guard[i].attempt_to_remove(client_id) {
                Ok(empty) => {
                    if empty {
                        //Since order of the pools is not really important
                        //Use the O(1) remove instead of the O(n) normal remove
                        guard.swap_remove(i).shutdown();
                    }

                    return true;
                }
                Err(_) => {}
            }
        }

        self.connected_clients.fetch_sub(1, Ordering::SeqCst);

        false
    }
}

impl<T> ConnectedPeersPool<T> where T: Send {
    //We mark the owner as static since if the pool is active then
    //The owner also has to be active
    pub fn new(batch_size: usize, batch_transmission: crossbeam_channel::Sender<Vec<T>>,
               owner: Arc<ConnectedPeersGroup<T>>) -> Arc<Self> {
        let result = Self {
            connected_clients: parking_lot::Mutex::new(Vec::new()),
            batch_size,
            batch_transmission,
            client_limit: batch_size * 10,
            finish_execution: AtomicBool::new(false),
            owner,
        };

        let pool = Arc::new(result);

        pool
    }

    pub fn start(self: Arc<Self>, pool_id: u32) {

        //Spawn the thread that will collect client requests
        //and then send the batches to the channel.
        std::thread::Builder::new().name(format!("Peer pool collector thread #{}", pool_id))
            .spawn(
                move || {
                    let backoff = BackoffN::new();

                    let mut total_rqs_collected: u128 = 0;
                    let mut collections: u64 = 0;

                    loop {
                        if self.finish_execution.load(Ordering::Relaxed) {
                            break;
                        }

                        let vec = self.collect_requests(self.batch_size, &self.owner);

                        total_rqs_collected += vec.len() as u128;
                        collections += 1;

                        if !vec.is_empty() {
                            self.batch_transmission.send(vec);
                        }

                        if collections % 100000 == 0 {
                            let current_time_millis = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                            println!("{:?} // {:?} // {} rqs collected in {} collections", self.owner.own_id, current_time_millis,
                                     total_rqs_collected, collections);

                            total_rqs_collected = 0;
                            collections = 0;
                        }

                        // backoff.spin();
                    }
                }).unwrap();
    }

    pub fn attempt_to_add(&self, client: Arc<ConnectedPeer<T>>) -> std::result::Result<(), Arc<ConnectedPeer<T>>> {
        let mut guard = self.connected_clients.lock();

        if guard.len() < self.client_limit {
            guard.push(client);

            return Ok(());
        }

        Err(client)
    }

    pub fn attempt_to_remove(&self, client_id: &NodeId) -> std::result::Result<bool, ()> {
        let mut guard = self.connected_clients.lock();

        return match guard.iter().position(|client| client.client_id().eq(client_id)) {
            None => {
                Err(())
            }
            Some(position) => {
                guard.swap_remove(position);

                Ok(guard.is_empty())
            }
        };
    }

    pub fn collect_requests(&self, batch_size: usize, owner: &Arc<ConnectedPeersGroup<T>>) -> Vec<T> {
        let mut batch = Vec::with_capacity(batch_size);

        let mut guard = self.connected_clients.lock();

        let mut dced = Vec::new();

        if guard.len() == 0 {
            return vec![];
        }

        let requests_per_client = std::cmp::max(batch_size / guard.len(), 1);
        let requests_remainder = batch_size % guard.len();

        let start_point = fastrand::usize(0..guard.len());

        //We don't want to leave any slot in the batch unfilled...
        let mut next_client_requests = requests_per_client + requests_remainder;

        for index in 0..guard.len() {
            let client = &guard[(start_point + index) % guard.len()];

            if client.is_dc() {
                dced.push(client.client_id().clone());

                //Assign the remaining slots to the next client
                next_client_requests += requests_per_client;

                continue;
            }

            let rqs_dumped = match client.dump_n_requests(next_client_requests, &mut batch) {
                Ok(rqs) => { rqs }
                Err(_) => {
                    dced.push(client.client_id().clone());

                    //Assign the remaining slots to the next client
                    next_client_requests += requests_per_client;

                    continue;
                }
            };

            //Leave the requests that were not used open for the following clients, in a greedy fashion
            next_client_requests -= rqs_dumped;
            //Add the requests for the upcoming requests
            next_client_requests += requests_per_client;
        }


        //This might cause some lag since it has to access the intmap, but
        //Should be fine as it will only happen on client dcs
        if !dced.is_empty() {
            for node in &dced {
                //This is O(n*c) but there isn't really a much better way to do it I guess
                let option = guard.iter().position(|x| {
                    x.client_id().0 == node.0
                }).unwrap();

                guard.swap_remove(option);
            }

            drop(guard);

            owner.del_cached_clients(dced);
        }


        batch
    }

    pub fn shutdown(&self) {
        self.finish_execution.store(true, Ordering::Relaxed);
    }
}

impl<T> ConnectedPeer<T> where T: Send {
    pub fn client_id(&self) -> &NodeId {
        match self {
            Self::PoolConnection { client_id, .. } => {
                client_id
            }
            Self::UnpooledConnection { client_id, .. } => {
                client_id
            }
        }
    }

    pub fn is_dc(&self) -> bool {
        match self {
            Self::PoolConnection { receiver, .. } => {
                receiver.is_dc()
            }
            Self::UnpooledConnection { sender, .. } => {
                sender.lock().is_none()
            }
        }
    }

    pub fn disconnect(&self) {
        match self {
            Self::PoolConnection { sender, .. } => {
                sender.lock().take();
            }
            Self::UnpooledConnection { sender, .. } => {
                sender.lock().take();
            }
        };
    }

    ///Dump n requests into the provided vector
    ///Returns the amount of requests that were dumped into the array
    pub fn dump_n_requests(&self, rq_bound: usize, dump_vec: &mut Vec<T>) -> Result<usize> {
        return match self {
            Self::PoolConnection { receiver, .. } => {
                return match receiver.try_recv_mult(dump_vec, rq_bound) {
                    Ok(rqs) => {
                        Ok(rqs)
                    }
                    Err(err) => {
                        Err(Error::simple_with_msg(ErrorKind::Communication, "Client has already disconnected."))
                    }
                };
            }
            Self::UnpooledConnection { .. } => {
                Ok(0)
            }
        };
    }

    pub fn push_request_sync(&self, msg: T) {
        match self {
            Self::PoolConnection { sender, .. } => {
                let sender_guard = sender.lock().as_ref().unwrap().clone();

                match sender_guard.send(msg) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to send because {:?}", err);
                    }
                };
            }
            Self::UnpooledConnection { sender, .. } => {
                let mut send_clone;

                {
                    let send_lock = sender.lock();
                    let mut sender_guard = send_lock.as_ref();

                    match sender_guard {
                        None => {
                            //debug!("{:?} // Failed to receive because there is no sender.", self.client_id());
                            return;
                        }
                        Some(send) => {
                            send_clone = send.clone();
                        }
                    }
                }

                match send_clone.send_sync(msg) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to receive data from {:?} because {:?}", self.client_id(), err);
                    }
                }
            }
        }
    }

    pub async fn push_request_(&self, msg: T, own_id: &NodeId) where T: Debug {
        match self {
            Self::PoolConnection { sender, .. } => {
                // debug!("{:?} // Pushing request {:?} into queue, from {:?}",
                //     own_id, msg, self.client_id());

                let sender_guard = sender.lock().as_ref().unwrap().clone();

                match sender_guard.send_async(msg).await {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to send because {:?}", err);
                    }
                };
            }
            Self::UnpooledConnection { sender, .. } => {
                //debug!("{:?} // Pushing request {:?} into queue, from {:?}",
                //    own_id, msg, self.client_id());

                let mut send_clone;

                {
                    let send_lock = sender.lock();
                    let mut sender_guard = send_lock.as_ref();

                    match sender_guard {
                        None => {
                            //debug!("{:?} // Failed to receive because there is no sender.", self.client_id());
                            return;
                        }
                        Some(send) => {
                            send_clone = send.clone();
                        }
                    }
                }

                match send_clone.send(msg).await {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to receive data from {:?} because {:?}", self.client_id(), err);
                    }
                }
            }
        }
    }

    pub async fn push_request(&self, msg: T) {
        match self {
            Self::PoolConnection { sender, .. } => {
                let sender_guard = sender.lock().as_ref().unwrap().clone();

                match sender_guard.send_async(msg).await {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to send because {:?}", err);
                    }
                };
            }
            Self::UnpooledConnection { sender, .. } => {
                let mut send_clone;

                {
                    let send_lock = sender.lock();
                    let mut sender_guard = send_lock.as_ref();

                    match sender_guard {
                        None => {
                            //debug!("{:?} // Failed to receive because there is no sender.", self.client_id());
                            return;
                        }
                        Some(send) => {
                            send_clone = send.clone();
                        }
                    }
                }

                match send_clone.send(msg).await {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to receive data from {:?} because {:?}", self.client_id(), err);
                    }
                }
            }
        }
    }
}