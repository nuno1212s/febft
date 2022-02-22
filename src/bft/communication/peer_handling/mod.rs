use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use dsrust::queues::mqueue::MQueue;
use dsrust::queues::queues::{BQueue, PartiallyDumpable, SizableQueue};
use futures::select;
use intmap::IntMap;
use parking_lot::{Mutex, RwLock};

use crate::bft::communication::{NODE_CHAN_BOUND, NodeConfig, NodeId};
use crate::bft::communication::channel::{ChannelRx, ChannelTx, new_bounded};
use crate::bft::error::*;
use crate::bft::threadpool;

///Handles the communication between two peers (replica - replica, replica - client)
/// Only handles reception of requests, not transmission

pub struct NodePeers<T> {
    first_cli: NodeId,
    own_id: NodeId,
    peer_loopback: Arc<ConnectedPeer<T>>,
    replica_handling: ConnectedPeersGroup<T>,
    client_handling: Option<ConnectedPeersGroup<T>>,
    replica_channel: (ChannelTx<Vec<T>>, ChannelRx<Vec<T>>),
    client_channel: Option<(ChannelTx<Vec<T>>, ChannelRx<Vec<T>>)>,
}

impl<T> NodePeers<T> {
    pub fn new(cfg: &NodeConfig) -> NodePeers<T> {
        let id = cfg.id;

        //We only want to setup client handling if we are a replica
        let client_handling;

        let client_channel;

        if id >= cfg.first_cli {
            let (client_tx, client_rx) = new_bounded(NODE_CHAN_BOUND);

            client_handling = Some(ConnectedPeersGroup::new(32,
                                                            NODE_CHAN_BOUND,
                                                            client_tx.clone()));
            client_channel = Some((client_tx, client_rx));
        } else {
            client_handling = None;
            client_channel = None;
        };

        //TODO: maybe change the channel bound?
        let (replica_tx, replica_rx) = new_bounded(NODE_CHAN_BOUND);

        //TODO: Batch size is not correct, should be the value found in env
        //Both replicas and clients have to interact with replicas, so we always need this pool
        //We have a much larger queue because we don't want small slowdowns slowing down the connections
        let replica_handling = ConnectedPeersGroup::new(1024, NODE_CHAN_BOUND,
                                                        replica_tx.clone());

        let loopback_address = replica_handling.init_client(id);

        NodePeers {
            first_cli: cfg.first_cli,
            own_id: cfg.id,
            peer_loopback: loopback_address,
            replica_handling,
            client_handling,
            replica_channel: (replica_tx, replica_rx),
            client_channel,
        }
    }

    pub fn resolve_peer_conn(&self, peer: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        if peer == self.own_id {
            return Some(self.peer_loopback.clone());
        }

        return if peer < self.first_cli {
            self.replica_handling.get_client_conn(peer)
        } else {
            self.client_handling.expect("Tried to resolve client conn in the client")
                .get_client_conn(peer);
        };
    }

    pub async fn receive_client_messages(&mut self) -> Result<Vec<T>> {

    }

    pub async fn receive_messages(&mut self) -> Result<Vec<T>> {
        return if self.client_handling.is_some() {
            let msgs: Result<Vec<T>> = select! {

                result = self.replica_channel.1.recv() => {
                    result
                },
                result = self.client_channel.unwrap().1.recv() => {
                    result
                }

            };

            msgs
        } else {
            self.replica_channel.1.recv().await
        };
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
pub struct ConnectedPeersGroup<T> {
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    client_pools: Mutex<Vec<Arc<ConnectedPeersPool<T>>>>,
    client_connections_cache: RwLock<IntMap<Arc<ConnectedPeer<T>>>>,
    per_client_cache: usize,
    batch_size: usize,
    batch_transmission: ChannelTx<Vec<T>>,
}

pub struct ConnectedPeersPool<T> {
    //We can use mutexes here since there will only be concurrency on client connections and dcs
    //And since each client has his own reference to push data to, this only needs to be accessed by the thread
    //That's producing the batches and the threads of clients connecting and disconnecting
    connected_clients: Mutex<Vec<Arc<ConnectedPeer<T>>>>,
    batch_size: usize,
    batch_transmission: ChannelTx<Vec<T>>,
    finish_execution: AtomicBool,
    thread_handle: Option<JoinHandle<()>>,
    owner: &'static ConnectedPeersGroup<T>,
}

pub struct ConnectedPeer<T> {
    client_id: NodeId,
    request_queue: MQueue<T>,
    disconnected: AtomicBool,
}

impl<T> ConnectedPeersGroup<T> {
    pub fn new(per_client_bound: usize, batch_size: usize, batch_transmission: ChannelTx<Vec<T>>) -> Self {
        Self {
            client_pools: parking_lot::Mutex::new(Vec::new()),
            client_connections_cache: RwLock::new(IntMap::new()),
            per_client_cache: per_client_bound,
            batch_size,
            batch_transmission,
        }
    }

    pub fn init_client(&self, peer_id: NodeId) -> Arc<ConnectedPeer<T>> {
        let connected_client = Arc::new(ConnectedPeer::new(peer_id, self.per_client_cache));

        let mut cached_clients = self.client_connections_cache.write();

        cached_clients.insert(peer_id.0 as u64, connected_client.clone());

        drop(cached_clients);

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
                                           self);

        pool.attempt_to_add(clone_queue).unwrap();

        guard.push(pool);

        connected_client
    }

    pub fn get_client_conn(&self, client_id: NodeId) -> Option<Arc<ConnectedPeer<T>>> {
        let cache_guard = self.client_connections_cache.read();

        return match cache_guard.get(client_id.0 as u64) {
            None => {
                None
            }
            Some(peer) => {
                Ok(Arc::clone(peer))
            }
        };
    }

    fn del_cached_clients(&self, clients: Vec<NodeId>) {
        let mut cache_guard = self.client_connections_cache.write();

        for client_id in clients {
            cache_guard.remove(client_id.0 as u64);
        }

        drop(cache_guard);
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
                        //Use the O(1) remove instead of the O(n)
                        guard.swap_remove(i).shutdown();
                    }

                    return true;
                }
                Err(_) => {}
            }
        }

        false
    }
}

impl<T> ConnectedPeersPool<T> {
    //We mark the owner as static since if the pool is active then
    //The owner also has to be active
    pub fn new(client_count: usize, batch_transmission: ChannelTx<Vec<T>>,
               owner: &'static ConnectedPeersGroup<T>) -> Arc<Self> {
        let result = Self {
            connected_clients: parking_lot::Mutex::new(Vec::new()),
            batch_size: client_count,
            batch_transmission,
            finish_execution: AtomicBool::new(false),
            thread_handle: None,
            owner,
        };

        let mut pool = Arc::new(result);

        let pool_clone = pool.clone();

        //Spawn the thread that will collect client requests
        //and then send the batches to the channel.
        pool.thread_handle = Some(std::thread::spawn(move || {
            loop {
                if pool_clone.finish_execution.load(Ordering::Relaxed) {
                    break;
                }

                let vec = pool_clone.collect_requests(pool_clone.batch_size, owner);

                if !vec.is_empty() {
                    pool_clone.batch_transmission.send(vec);
                }
            }
        }));

        pool
    }

    pub fn attempt_to_add(&self, client: Arc<ConnectedPeer<T>>) -> std::result::Result<(), Arc<ConnectedPeer<T>>> {
        let mut guard = self.connected_clients.lock();

        if guard.len() < self.batch_size {
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

    pub fn collect_requests(&self, batch_size: usize, owner: &ConnectedPeersGroup<T>) -> Vec<T> {
        let mut batch = Vec::with_capacity(batch_size);

        let mut guard = self.connected_clients.lock();

        let mut dced = Vec::new();

        //We can do this because our pooling system prevents the number of clients
        //In each pool to be larger than the batch size, so the requests_per_client is always
        //> 1, leading to no starvation
        let requests_per_client = batch_size / guard.len();
        let requests_remainder = batch_size % guard.len();

        let start_point = fastrand::usize(0..guard.len());

        //We don't want to leave any slot in the batch unfilled...
        let mut next_client_requests = requests_per_client + requests_remainder;

        for index in 0..guard.len() {
            let client = &guard[(start_point + index) % guard.len()];

            if client.is_dc() {
                dced.push(guard.swap_remove(index).client_id);

                //Assign the remaining slots to the next client
                next_client_requests += requests_per_client;

                continue;
            }

            let rqs_dumped = client
                .request_queue().dump_n_requests(next_client_requests, &mut batch);

            //Leave the requests that were not used open for the following clients, in a greedy fashion
            next_client_requests -= rqs_dumped;
            //Add the requests for the upcoming requests
            next_client_requests += requests_per_client;
        }

        drop(guard);

        //This might cause some lag since it has to access the intmap, but
        //Should be fine as it will only happen on client dcs
        if !dced.is_empty() {
            owner.del_cached_clients(dced);
        }

        batch
    }

    pub fn shutdown(&self) {
        self.finish_execution.store(true, Ordering::Relaxed);
    }
}

impl<T> ConnectedPeer<T> {
    pub fn new(client_id: NodeId, per_client_bound: usize) -> Self {
        Self {
            client_id,
            request_queue: MQueue::new(per_client_bound, true),
            disconnected: AtomicBool::new(false),
        }
    }

    pub fn client_id(&self) -> &NodeId {
        &self.client_id
    }

    pub fn request_queue(&self) -> &MQueue<T> {
        &self.request_queue
    }

    pub fn is_dc(&self) -> bool {
        self.disconnected.load(Ordering::Relaxed) && self.request_queue.is_empty()
    }

    pub fn disconnect(&self) {
        self.disconnected.store(true, Ordering::Relaxed);
    }

    ///Dump n requests into the provided vector
    ///Returns the amount of requests that were dumped into the array
    pub fn dump_n_requests(&self, rq_bound: usize, dump_vec: &mut Vec<T>) -> usize {
        self.request_queue.dump_partial(dump_vec, rq_bound).unwrap()
    }

    pub async fn push_request(&self, msg: T) {
        self.request_queue.enqueue_blk(msg)
    }
}

