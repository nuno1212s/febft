use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;

use log::{error, info};

use crate::bft::communication::{channel, NODE_CHAN_BOUND, NodeId};
use crate::bft::communication::channel::{ChannelMultRx, ChannelMultTx, ChannelSyncTx, TryRecvError};
use crate::bft::communication::channel::ChannelSyncRx;
use crate::bft::communication::channel::new_bounded_sync;
use crate::bft::error::*;

fn channel_init<T>(capacity: usize) -> (ChannelMultTx<T>, ChannelMultRx<T>) {
    channel::new_bounded_mult(capacity)
}

fn client_channel_init<T>(capacity: usize) -> (ChannelMultTx<T>, ChannelMultRx<T>) {
    channel::new_bounded_mult(capacity)
}

///Handles the communication between two peers (replica - replica, replica - client)
///Only handles reception of requests, not transmission
/// It's also built on top of the default networking layer, which handles
/// actually serializing the messages. This only handles already serialized messages.
pub struct PeerIncomingRqHandling<T: Send + 'static> {
    batch_size: usize,
    //The first client id, so we can distinguish clients from replicas
    first_cli: NodeId,
    //Our own ID
    own_id: NodeId,
    //The loopback channel to our own node reception
    peer_loopback: Arc<ConnectedPeer<T>>,
    //Replica connection handling
    replica_handling: Arc<ReplicaHandling<T>>,
    //Client request collection handling (Pooled), is only available on the replicas
    client_handling: Option<Arc<ConnectedPeersGroup<T>>>,
    client_tx: Option<ChannelSyncTx<Vec<T>>>,
    client_rx: Option<ChannelSyncRx<Vec<T>>>,
}

const DEFAULT_CLIENT_QUEUE: usize = 16000;
const DEFAULT_REPLICA_QUEUE: usize = 1024;

///We make this class Sync and send since the clients are going to be handled by a single class
///And the replicas are going to be handled by another class.
/// There is no possibility of 2 threads accessing the client_rx or replica_rx concurrently
unsafe impl<T> Sync for PeerIncomingRqHandling<T> where T: Send {}

unsafe impl<T> Send for PeerIncomingRqHandling<T> where T: Send {}

impl<T> PeerIncomingRqHandling<T> where T: Send {
    pub fn new(id: NodeId, first_cli: NodeId, batch_size: usize, clients_per_pool: usize,
               batch_timeout_micros: u64, batch_sleep_micros: u64) -> PeerIncomingRqHandling<T> {
        //We only want to setup client handling if we are a replica
        let client_handling;

        let client_channel;

        if id < first_cli {
            let (client_tx, client_rx) = channel::new_bounded_sync(NODE_CHAN_BOUND);

            client_handling = Some(ConnectedPeersGroup::new(DEFAULT_CLIENT_QUEUE,
                                                            batch_size,
                                                            client_tx.clone(),
                                                            id,
                                                            clients_per_pool,
                                                            batch_timeout_micros,
                                                            batch_sleep_micros));
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

        let peers = PeerIncomingRqHandling {
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

    ///Initialize a new peer connection
    /// This will be used by the networking layer to deliver the received messages to the
    /// Actual system
    pub fn init_peer_conn(&self, peer: NodeId) -> Arc<ConnectedPeer<T>> {
        //debug!("Initializing peer connection for peer {:?} on peer {:?}", peer, self.own_id);

        return if peer >= self.first_cli {
            self.client_handling.as_ref().expect("Tried to init client request from client itself?")
                .init_client(peer)
        } else {
            self.replica_handling.init_client(peer)
        };
    }

    ///Get the incoming request queue for a given node
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

    ///Get our loopback request queue
    pub fn loopback_connection(&self) -> &Arc<ConnectedPeer<T>> {
        &self.peer_loopback
    }

    /// Get how many client request batches are waiting in the queue
    pub fn rqs_len_from_clients(&self) -> usize {
        return match &self.client_rx {
            None => { 0 }
            Some(rx) => {
                rx.len()
            }
        };
    }

    ///Receive request vector from clients. Block until we get the requests
    pub fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<T>> {
        let rx = self.get_client_rx()?;

        match timeout {
            None => {
                match rx.recv() {
                    Ok(vec) => {
                        Ok(vec)
                    }
                    Err(_) => {
                        Err(Error::simple_with_msg(ErrorKind::CommunicationIncomingPeerHandling, "Failed to receive"))
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
                            TryRecvError::Timeout => {
                                Ok(vec![])
                            }
                            _ => {
                                Err(Error::simple_with_msg(ErrorKind::CommunicationIncomingPeerHandling, "Failed to receive"))
                            }
                        }
                    }
                }
            }
        }
    }

    /// Try to receive from the clients.
    /// It's possible that there are no messages currently available, so
    /// we return a result with an option
    pub fn try_receive_from_clients(&self) -> Result<Option<Vec<T>>> {
        let rx = self.get_client_rx()?;

        match rx.try_recv() {
            Ok(msgs) => {
                Ok(Some(msgs))
            }
            Err(err) => {
                match &err {
                    TryRecvError::ChannelEmpty => {
                        Ok(None)
                    }
                    _ => {
                        Err(Error::wrapped(ErrorKind::CommunicationIncomingPeerHandling, err))
                    }
                }
            }
        }
    }

    fn get_client_rx(&self) -> Result<&ChannelSyncRx<Vec<T>>> {
        return match &self.client_rx {
            None => {
                Err(Error::simple_with_msg(ErrorKind::CommunicationIncomingPeerHandling, "Failed to receive from clients as there are no clients connected"))
            }
            Some(rx) => {
                Ok(rx)
            }
        };
    }

    ///Receive a single request from the replicas
    pub fn receive_from_replicas(&self) -> Result<T> {
        Ok(self.replica_handling.receive_from_replicas())
    }

    ///Count the amount of clients present (not including replicas)
    ///Returns None if this is a client and therefore has no client conns
    pub fn client_count(&self) -> Option<usize> {
        return match &self.client_handling {
            None => { None }
            Some(client) => {
                Some(client.connected_clients.load(Ordering::Relaxed))
            }
        };
    }

    ///Count the replicas connected
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
        queue: Mutex<Option<Vec<T>>>,
        disconnected: AtomicBool,
    },
    UnpooledConnection {
        client_id: NodeId,
        sender: Mutex<Option<ChannelSyncTx<T>>>,
    },
}

///Handling replicas is different from handling clients
///We want to handle the requests differently because in communication between replicas
///Latency is extremely important and we have to minimize it to the least amount possible
/// So in this implementation, we will just use a single channel to receive and collect
/// all messages
///
/// FIXME: See if having a multiple channel approach with something like a select is
/// worth the overhead of having to pool multiple channels. We may also get problems with fairness.
/// Probably not worth it
pub struct ReplicaHandling<T> where T: Send {
    capacity: usize,
    //The channel we push replica sent requests into
    channel_tx_replica: ChannelSyncTx<T>,
    //The channel used to read requests that were pushed by replicas
    channel_rx_replica: ChannelSyncRx<T>,
    connected_clients: DashMap<u32, Arc<ConnectedPeer<T>>>,
    connected_client_count: AtomicUsize,
}

impl<T> ReplicaHandling<T> where T: Send {
    pub fn new(capacity: usize) -> Arc<Self> {
        let (sender, receiver) = new_bounded_sync(capacity);

        Arc::new(
            Self {
                capacity,
                channel_rx_replica: receiver,
                channel_tx_replica: sender,
                connected_clients: DashMap::new(),
                connected_client_count: AtomicUsize::new(0),
            }
        )
    }

    pub fn init_client(&self, peer_id: NodeId) -> Arc<ConnectedPeer<T>> {
        let peer = Arc::new(ConnectedPeer::UnpooledConnection {
            client_id: peer_id,
            sender: Mutex::new(Some(self.channel_tx_replica.clone())),
        });

        match self.connected_clients.insert(peer_id.id(), peer.clone()) {
            None => {
                //Only count connected replicas when we were previously not connected to
                //it, or we would get double counting
                self.connected_client_count.fetch_add(1, Ordering::Relaxed);
            }
            Some(old) => {
                //When we insert a new channel, we want the old channel to become closed.
                old.disconnect();
            }
        };

        peer
    }

    pub fn resolve_connection(&self, peer_id: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        match self.connected_clients.get(&peer_id.id()) {
            None => {
                None
            }
            Some(peer) => {
                Some(Arc::clone(peer.value()))
            }
        }
    }

    pub fn receive_from_replicas(&self) -> T {
        self.channel_rx_replica.recv().unwrap()
    }
}

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
    own_id: NodeId,
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    client_pools: Mutex<BTreeMap<usize, Arc<ConnectedPeersPool<T>>>>,
    client_connections_cache: DashMap<u32, Arc<ConnectedPeer<T>>>,
    connected_clients: AtomicUsize,
    batch_transmission: ChannelSyncTx<Vec<T>>,
    per_client_cache: usize,
    //What batch size should we target for each batch (there is no set limit on requests,
    //Just a hint on when it should move on)
    batch_target_size: usize,
    //How much time can be spent gathering batches
    batch_timeout_micros: u64,
    //How much time should the thread sleep in between batch collection
    batch_sleep_micros: u64,
    clients_per_pool: usize,
    //Counter used to keep track of the created pools
    pool_id_counter: AtomicUsize,
}

pub struct ConnectedPeersPool<T: Send + 'static> {
    pool_id: usize,
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    connected_clients: Mutex<Vec<Arc<ConnectedPeer<T>>>>,
    batch_transmission: ChannelSyncTx<Vec<T>>,
    finish_execution: AtomicBool,
    owner: Arc<ConnectedPeersGroup<T>>,
    batch_size: usize,
    client_limit: usize,
    batch_timeout_micros: u64,
    batch_sleep_micros: u64,
}

impl<T> ConnectedPeersGroup<T> where T: Send + 'static {
    pub fn new(per_client_bound: usize, batch_size: usize, batch_transmission: ChannelSyncTx<Vec<T>>,
               own_id: NodeId, clients_per_pool: usize, batch_timeout_micros: u64,
               batch_sleep_micros: u64) -> Arc<Self> {
        Arc::new(Self {
            own_id,
            client_pools: Mutex::new(BTreeMap::new()),
            client_connections_cache: DashMap::new(),
            per_client_cache: per_client_bound,
            connected_clients: AtomicUsize::new(0),
            batch_timeout_micros,
            batch_sleep_micros,
            batch_target_size: batch_size,
            batch_transmission,
            clients_per_pool,
            pool_id_counter: AtomicUsize::new(0),
        })
    }

    fn get_pool_id(&self) -> Result<usize> {
        const IT_LIMIT: usize = 100;

        let mut it_count = 0;

        let pool_id = loop {
            let pool_id = self.pool_id_counter.fetch_add(1, Ordering::Relaxed);

            it_count += 1;

            if it_count >= IT_LIMIT {
                return Err(Error::simple(ErrorKind::Communication));
            }

            if !self.client_pools.lock().unwrap().contains_key(&pool_id) {
                break pool_id;
            }
        };

        Ok(pool_id)
    }

    pub fn init_client(self: &Arc<Self>, peer_id: NodeId) -> Arc<ConnectedPeer<T>> {
        let connected_client = Arc::new(ConnectedPeer::PoolConnection {
            client_id: peer_id,
            disconnected: AtomicBool::new(false),
            queue: Mutex::new(Some(Vec::with_capacity(self.per_client_cache))),
        });

        self.connected_clients.fetch_add(1, Ordering::SeqCst);

        match self.client_connections_cache.insert(peer_id.0, connected_client.clone()) {
            None => {}
            Some(old_conn) => {
                old_conn.disconnect();
            }
        };

        let mut clone_queue = connected_client.clone();

        {
            let guard = self.client_pools.lock().unwrap();

            for (_pool_id, pool) in &*guard {
                match pool.attempt_to_add(clone_queue) {
                    Ok(_) => {
                        return connected_client;
                    }
                    Err(queue) => {
                        clone_queue = queue;
                    }
                }
            }
        }

        //In the case all the pools are already full, allocate a new pool
        let pool_id = match self.get_pool_id() {
            Ok(pool_id) => {
                pool_id
            }
            Err(_err) => {
                panic!("Failed to allocate new pool id");
            }
        };

        let pool = ConnectedPeersPool::new(
            pool_id,
            self.batch_target_size,
            self.batch_transmission.clone(),
            Arc::clone(self),
            self.clients_per_pool,
            self.batch_timeout_micros,
            self.batch_sleep_micros);

        match pool.attempt_to_add(clone_queue) {
            Ok(_) => {}
            Err(_e) => {
                panic!("Failed to add pool to pool list.")
            }
        };
        {
            let mut guard = self.client_pools.lock().unwrap();

            let pool_clone = pool.clone();

            guard.insert(pool.pool_id, pool);

            let id = guard.len();

            pool_clone.start(id as u32);
        }


        connected_client
    }

    pub fn get_client_conn(&self, client_id: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        return match self.client_connections_cache.get(&client_id.0) {
            None => {
                None
            }
            Some(peer) => {
                Some(Arc::clone(peer.value()))
            }
        };
    }

    fn del_pool(&self, pool_id: usize) -> bool {
        println!("{:?} // DELETING POOL {}", self.own_id, pool_id);

        match self.client_pools.lock().unwrap().remove(&pool_id) {
            None => { false }
            Some(pool) => {
                pool.shutdown();
                println!("{:?} // DELETED POOL {}", self.own_id, pool_id);

                true
            }
        }
    }

    fn del_cached_clients(&self, clients: Vec<NodeId>) {
        for client_id in &clients {
            self.client_connections_cache.remove(&client_id.0);
        }

        self.connected_clients.fetch_sub(clients.len(), Ordering::Relaxed);
    }
}

impl<T> ConnectedPeersPool<T> where T: Send {
    //We mark the owner as static since if the pool is active then
    //The owner also has to be active
    pub fn new(pool_id: usize, batch_size: usize, batch_transmission: ChannelSyncTx<Vec<T>>,
               owner: Arc<ConnectedPeersGroup<T>>, client_per_pool: usize,
               batch_timeout_micros: u64, batch_sleep_micros: u64) -> Arc<Self> {
        let result = Self {
            pool_id,
            connected_clients: Mutex::new(Vec::new()),
            batch_size,
            batch_transmission,
            batch_timeout_micros,
            batch_sleep_micros,
            client_limit: client_per_pool,
            finish_execution: AtomicBool::new(false),
            owner,
        };

        let pool = Arc::new(result);

        pool
    }

    pub fn start(self: Arc<Self>, pool_id: u32) {

        //Spawn the thread that will collect client requests
        //and then send the batches to the channel.
        std::thread::Builder::new()
            .name(format!("Peer pool collector thread #{}", pool_id))
            .spawn(move || {
                let mut total_rqs_collected: u128 = 0;
                let mut collections: u64 = 0;

                loop {
                    if self.finish_execution.load(Ordering::Relaxed) {
                        break;
                    }

                    let vec = match self.collect_requests(self.batch_size, &self.owner) {
                        Ok(vec) => { vec }
                        Err(err) => {
                            match err.kind() {
                                ErrorKind::Communication => {
                                    //The pool is empty, so to save CPU, delete it
                                    self.owner.del_pool(self.pool_id);

                                    self.finish_execution.store(true, Ordering::SeqCst);

                                    break;
                                }
                                _ => { break; }
                            }
                        }
                    };

                    total_rqs_collected += vec.len() as u128;
                    collections += 1;

                    if !vec.is_empty() {
                        self.batch_transmission.send(vec).expect("Failed to send proposed batch");
                        // Sleep for a determined amount of time to allow clients to send requests
                        let three_quarters_sleep = (self.batch_sleep_micros / 4) * 3;
                        let five_quarters_sleep = (self.batch_sleep_micros / 4) * 5;

                        let sleep_micros = fastrand::u64(three_quarters_sleep..=five_quarters_sleep);

                        std::thread::sleep(Duration::from_micros(sleep_micros));
                    }

                    if collections % 10000000 == 0 {
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
        let mut guard = self.connected_clients.lock().unwrap();

        if guard.len() < self.client_limit {
            guard.push(client);

            return Ok(());
        }

        Err(client)
    }

    pub fn attempt_to_remove(&self, client_id: &NodeId) -> std::result::Result<bool, ()> {
        let mut guard = self.connected_clients.lock().unwrap();

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

    pub fn collect_requests(&self, batch_target_size: usize, owner: &Arc<ConnectedPeersGroup<T>>) -> Result<Vec<T>> {
        let vec_size = std::cmp::max(batch_target_size, self.owner.per_client_cache);

        let mut batch = Vec::with_capacity(vec_size);

        let guard = self.connected_clients.lock().unwrap();

        let mut dced = Vec::new();

        let mut connected_peers = Vec::with_capacity(guard.len());

        if guard.len() == 0 {
            return Err(Error::simple(ErrorKind::Communication));
        }

        for connected_peer in &*guard {
            connected_peers.push(Arc::clone(connected_peer));
        }

        drop(guard);

        let start_point = fastrand::usize(0..connected_peers.len());

        let ind_limit = usize::MAX;

        let start_time = Instant::now();

        let mut replacement_vec = Vec::with_capacity(self.owner.per_client_cache);

        for index in 0..ind_limit {
            let client = &connected_peers[(start_point + index) % connected_peers.len()];

            if client.is_dc() {
                dced.push(client.client_id().clone());

                //Assign the remaining slots to the next client
                continue;
            }

            //Collect all possible requests from each client

            let mut rqs_dumped = match client.dump_requests(replacement_vec) {
                Ok(rqs) => { rqs }
                Err(vec) => {
                    dced.push(client.client_id().clone());

                    replacement_vec = vec;
                    continue;
                }
            };

            batch.append(&mut rqs_dumped);

            //The previous vec is now the new vec of the next node
            replacement_vec = rqs_dumped;

            if index % connected_peers.len() == 0 {
                //We have done a full circle on the requests

                if batch.len() >= batch_target_size {
                    //We only check on each complete revolution since if we didn't do that
                    //We could have a situation where a single client's requests were
                    //Enough to fill an entire batch, so the rest of the clients
                    //Wouldn't even be checked
                    break;
                } else {
                    let current_time = Instant::now();

                    if current_time.duration_since(start_time).as_micros() >= self.batch_timeout_micros as u128 {
                        //Check if a given amount of time limit has passed, to prevent us getting
                        //Stuck while checking for requests
                        break;
                    }

                    std::thread::yield_now();
                }
            }
        }

        //This might cause some lag since it has to access the intmap, but
        //Should be fine as it will only happen on client dcs
        if !dced.is_empty() {
            let mut guard = self.connected_clients.lock().unwrap();

            for node in &dced {
                //This is O(n*c) but there isn't really a much better way to do it I guess
                let option = guard.iter().position(|x| {
                    x.client_id().0 == node.0
                });

                match option {
                    None => {
                        //The client was already removed from the guard
                    }
                    Some(option) => {
                        guard.swap_remove(option);
                    }
                }
            }

            //If the pool is empty, delete it
            let should_delete_pool = guard.is_empty();

            drop(guard);

            owner.del_cached_clients(dced);

            if should_delete_pool {
                return Err(Error::simple(ErrorKind::Communication));
            }
        }

        Ok(batch)
    }

    pub fn shutdown(&self) {
        info!("{:?} // Pool {} is shutting down", self.owner.own_id, self.pool_id);

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
            Self::PoolConnection { disconnected, .. } => {
                disconnected.load(Ordering::Relaxed)
            }
            Self::UnpooledConnection { sender, .. } => {
                sender.lock().unwrap().is_none()
            }
        }
    }

    pub fn disconnect(&self) {
        match self {
            Self::PoolConnection { disconnected, .. } => {
                disconnected.store(false, Ordering::Relaxed)
            }
            Self::UnpooledConnection { sender, .. } => {
                sender.lock().unwrap().take();
            }
        };
    }

    ///Dump n requests into the provided vector
    ///Returns the amount of requests that were dumped into the array
    pub fn dump_requests(&self, replacement_vec: Vec<T>) -> std::result::Result<Vec<T>, Vec<T>> {
        return match self {
            Self::PoolConnection { queue, .. } => {
                let mut guard = queue.lock().unwrap();

                match &mut *guard {
                    None => {
                        Err(replacement_vec)
                    }
                    Some(rqs) => {
                        Ok(std::mem::replace(rqs, replacement_vec))
                    }
                }
            }
            Self::UnpooledConnection { .. } => {
                Ok(vec![])
            }
        };
    }

    pub fn push_request_sync(&self, msg: T) -> Result<()> {
        match self {
            Self::PoolConnection { queue, client_id, .. } => {
                let mut sender_guard = queue.lock().unwrap();

                match &mut *sender_guard {
                    None => {
                        error!("Failed to send to client {:?} as he was already disconnected", client_id);

                        Err(Error::simple_with_msg(ErrorKind::Communication, "Channel is closed"))
                    }
                    Some(sender) => {
                        //We don't clone and ditch the lock since each replica
                        //has a thread dedicated to receiving his requests, but only the single thread
                        //So, no more than one thread will be trying to acquire this lock at the same time
                        sender.push(msg);

                        Ok(())
                    }
                }
            }
            Self::UnpooledConnection { sender, client_id } => {
                let send_lock = sender.lock().unwrap();
                let sender_guard = send_lock.as_ref();

                match sender_guard {
                    None => {
                        error!("Failed to send to replica {:?} as he was already disconnected", client_id);

                        Err(Error::simple_with_msg(ErrorKind::Communication, "Channel is closed"))
                    }
                    Some(send) => {
                        //We don't clone and ditch the lock since each client
                        //has a thread dedicated to receiving his requests, but only the single thread
                        //So, no more than one thread will be trying to acquire this lock at the same time
                        match send.send(msg) {
                            Ok(_) => {
                                Ok(())
                            }
                            Err(err) => {
                                error!("Failed to receive data from {:?} because {:?}", self.client_id(), err);

                                Err(Error::simple_with_msg(ErrorKind::Communication, "Channel is closed"))
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn push_request(&self, msg: T) -> Result<()> {
        match self {
            Self::PoolConnection { queue, client_id, .. } => {
                let mut sender_guard = queue.lock().unwrap();

                match &mut *sender_guard {
                    Some(guard) => {
                        guard.push(msg);

                        Ok(())
                    }
                    None => {
                        error!("Failed to send to client {:?} as he was already disconnected", client_id);

                        Err(Error::simple_with_msg(ErrorKind::Communication, "Channel is closed"))
                    }
                }
            }
            Self::UnpooledConnection { sender, client_id, .. } => {
                let sender_guard = sender.lock().unwrap();

                match &*sender_guard {
                    Some(send) => {
                        match send.send(msg) {
                            Ok(_) => {
                                Ok(())
                            }
                            Err(err) => {
                                Err(Error::simple_with_msg(ErrorKind::Communication, format!("Failed to receive data from {:?} because {:?}", self.client_id(), err).as_str()))
                            }
                        }
                    }
                    None => {
                        error!("Failed to receive to client queue {:?} as he was already disconnected", client_id);

                        Err(Error::simple_with_msg(ErrorKind::Communication, "Channel is closed"))
                    }
                }
            }
        }
    }
}