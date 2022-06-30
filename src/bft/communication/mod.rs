//! Communication primitives for `febft`, such as wire message formats.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_tls::{
    TlsAcceptor,
    TlsConnector,
};
use dashmap::DashMap;
use either::{
    Either,
    Left,
    Right,
};
use futures::io::{
    AsyncReadExt,
    AsyncWriteExt,
    BufReader,
    BufWriter,
};
use futures_timer::Delay;
use intmap::IntMap;
use tracing::{debug, instrument, error};
use parking_lot::{RwLock};

use rustls::{ClientConfig, ServerConfig};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::bft::async_runtime as rt;
use crate::bft::benchmarks::{CommStats};
use crate::bft::communication::message::{Header, Message, SerializedMessage, StoredSerializedSystemMessage, SystemMessage, WireMessage};
use crate::bft::communication::peer_handling::{ConnectedPeer, NodePeers};
use crate::bft::communication::peer_sending_threads::ConnectionHandle;

use crate::bft::communication::serialize::{
    Buf,
    DigestData,
    SharedData,
};
use crate::bft::communication::socket::{AsyncListener, SyncListener, SyncSocket, SecureSocketRecvAsync, SecureSocketRecvSync, SecureSocketSend, SecureSocketSendAsync, SecureSocketSendSync, AsyncSocket, SocketSendSync, SocketSendAsync};
use crate::bft::crypto::hash::Digest;
use crate::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};
use crate::bft::error::*;
use crate::bft::ordering::Orderable;
use crate::bft::prng;
use crate::bft::prng::ThreadSafePrng;
use crate::bft::threadpool;

pub mod socket;
pub mod serialize;
pub mod message;
pub mod channel;
pub mod peer_handling;
pub mod peer_sending_threads;

//pub trait HijackMessage {
//    fn hijack_message(&self, stored: ) -> Either<M
//}

/// A `NodeId` represents the id of a process in the BFT system.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[repr(transparent)]
pub struct NodeId(pub u32);

impl NodeId {
    pub fn targets_u32<I>(into_iterator: I) -> impl Iterator<Item=Self>
        where
            I: IntoIterator<Item=u32>,
    {
        into_iterator
            .into_iter()
            .map(Self)
    }

    pub fn targets<I>(into_iterator: I) -> impl Iterator<Item=Self>
        where
            I: IntoIterator<Item=usize>,
    {
        into_iterator
            .into_iter()
            .map(NodeId::from)
    }

    pub fn id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for NodeId {
    #[inline]
    fn from(id: u32) -> NodeId {
        NodeId(id)
    }
}

impl From<u64> for NodeId {
    #[inline]
    fn from(id: u64) -> NodeId {
        NodeId(id as u32)
    }
}

impl From<usize> for NodeId {
    #[inline]
    fn from(id: usize) -> NodeId {
        NodeId(id as u32)
    }
}

impl From<NodeId> for usize {
    #[inline]
    fn from(id: NodeId) -> usize {
        id.0 as usize
    }
}

impl From<NodeId> for u64 {
    #[inline]
    fn from(id: NodeId) -> u64 {
        id.0 as u64
    }
}

impl From<NodeId> for u32 {
    #[inline]
    fn from(id: NodeId) -> u32 {
        id.0 as u32
    }
}

// TODO: maybe researh cleaner way to share the connections
// hashmap between two async tasks on the client
#[derive(Clone)]
enum PeerTx {
    // NOTE: comments below are invalid because of the changes we made to
    // the research branch; we now share a `SendNode` with the execution
    // layer, to allow faster reply delivery!
    //
    // clients need shared access to the hashmap; the `Arc` on the second
    // lock allows us to take ownership of a copy of the socket, so we
    // don't block the thread with the guard of the first lock waiting
    // on the second one
    Client {
        first_cli: NodeId,
        connected: Arc<RwLock<IntMap<ConnectionHandle>>>,
    },
    // replicas don't need shared access to the hashmap, so
    // we only need one lock (to restrict I/O to one producer at a time)
    Server {
        first_cli: NodeId,
        connected: Arc<RwLock<IntMap<ConnectionHandle>>>,
    },
}

impl PeerTx {
    ///Add a tx peer connection to the registry
    ///Requires knowing the first_cli
    pub fn add_peer(&self, client_id: u64, socket: ConnectionHandle) {
        match self {
            PeerTx::Client { connected, .. } => {
                let mut guard = connected.write();

                guard.insert(client_id, socket);
            }
            PeerTx::Server { connected, .. } => {
                let mut guard = connected.write();

                guard.insert(client_id, socket);
            }
        }
    }

    pub fn find_peer(&self, client_id: u64) -> Option<ConnectionHandle> {
        match self {
            PeerTx::Client { connected, .. } => {
                let option = {
                    let guard = connected.read();

                    guard.get(client_id).cloned()
                };

                option
            }
            PeerTx::Server { connected, .. } => {
                let option = {
                    let guard = connected.read();

                    guard.get(client_id).cloned()
                };

                option
            }
        }
    }
}

pub struct NodeShared {
    my_key: KeyPair,
    peer_keys: IntMap<PublicKey>,
}

pub struct SignDetached {
    shared: Arc<NodeShared>,
}

impl SignDetached {
    pub fn key_pair(&self) -> &KeyPair {
        &self.shared.my_key
    }
}

/// Container for handles to other processes in the system.
///
/// A `Node` constitutes the core component used in the wire
/// communication between processes.
pub struct Node<D: SharedData + 'static> {
    id: NodeId,
    first_cli: NodeId,
    node_handling: NodePeers<Message<D::State, D::Request, D::Reply>>,
    rng: ThreadSafePrng,
    shared: Arc<NodeShared>,
    peer_tx: PeerTx,
    connector: TlsConnector,
    sync_connector: Arc<ClientConfig>,
    peer_addrs: IntMap<PeerAddr>,
    comm_stats: Option<Arc<CommStats>>,

    pub sent_rqs: Option<Arc<Vec<DashMap<u64, ()>>>>,
    pub recv_rqs: Option<Arc<RwLock<Vec<DashMap<u64, ()>>>>>,
}


///Represents the server addresses of a peer
///Clients will only have 1 address while replicas will have 2 addresses (1 for facing clients,
/// 1 for facing replicas)
pub struct PeerAddr {
    client_addr: (SocketAddr, String),
    replica_addr: Option<(SocketAddr, String)>,
}

impl PeerAddr {
    pub fn new(client_addr: (SocketAddr, String)) -> Self {
        Self {
            client_addr,
            replica_addr: None,
        }
    }

    pub fn new_replica(client_addr: (SocketAddr, String), replica_addr: (SocketAddr, String)) -> Self {
        Self {
            client_addr,
            replica_addr: Some(replica_addr),
        }
    }
}

/// Represents a configuration used to bootstrap a `Node`.
pub struct NodeConfig {
    /// The total number of nodes in the system.
    ///
    /// Typically, BFT systems set this parameter to 4.
    /// This parameter is constrained by the following: `n >= 3*f + 1`.
    pub n: usize,
    /// The number of nodes allowed to fail in the system.
    ///
    /// Typically, BFT systems set this parameter to 1.
    pub f: usize,
    /// The id of this `Node`.
    pub id: NodeId,
    /// The first id assigned to a client`Node`.
    ///
    /// Every other client id of the form `first_cli + i`.
    pub first_cli: NodeId,
    ///The max size for batches of client operations
    pub batch_size: usize,
    /// The addresses of all nodes in the system (including clients),
    /// as well as the domain name associated with each address.
    ///
    /// For any `NodeConfig` assigned to `c`, the IP address of
    /// `c.addrs[&c.id]` should be equivalent to `localhost`.
    pub addrs: IntMap<PeerAddr>,
    /// The list of public keys of all nodes in the system.
    pub pk: IntMap<PublicKey>,
    /// The secret key of this particular `Node`.
    pub sk: KeyPair,
    /// The TLS configuration used to connect to replica nodes. (from client nodes)
    pub async_client_config: ClientConfig,
    /// The TLS configuration used to accept connections from client nodes.
    pub async_server_config: ServerConfig,
    ///The TLS configuration used to accept connections from replica nodes (Synchronously)
    pub sync_server_config: rustls::ServerConfig,
    ///The TLS configuration used to connect to replica nodes (from replica nodes) (Synchronousy)
    pub sync_client_config: rustls::ClientConfig,
    ///How many clients should be placed in a single collecting pool (seen in peer_handling)
    pub clients_per_pool: usize,
    ///The timeout for batch collection in each client pool.
    /// (The first to reach between batch size and timeout)
    pub batch_timeout_micros: u64,
    ///How long should a client pool sleep for before attempting to collect requests again
    /// (It actually will sleep between 3/4 and 5/4 of this value, to make sure they don't all sleep / wake up at the same time)
    pub batch_sleep_micros: u64,
    ///Statistics for communications
    pub comm_stats: Option<Arc<CommStats>>,
}

// max no. of messages allowed in the channel
const NODE_CHAN_BOUND: usize = 50000;

// max no. of SendTo's to inline before doing a heap alloc
const NODE_VIEWSIZ: usize = 16;

type SendTos<D> = SmallVec<[SendTo<D>; NODE_VIEWSIZ]>;

type SerializedSendTos<D> = SmallVec<[SerializedSendTo<D>; NODE_VIEWSIZ]>;

impl<D> Node<D>
    where
        D: SharedData + 'static,
        D::State: Send + Clone + 'static,
        D::Request: Send + 'static,
        D::Reply: Send + 'static,
{
    async fn setup_client_facing_socket(id: NodeId, cfg: &NodeConfig) ->
    Result<Either<SyncListener, AsyncListener>> {
        let peer_addr = cfg.addrs.get(id.into()).unwrap();

        let server_addr = &peer_addr.client_addr;

        let either: Either<SyncListener, AsyncListener>;

        {
            let mut server_addr = server_addr.0.clone();

            server_addr.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
            //TODO: Maybe add support for asynchronous listeners?
            println!("{:?} // Binding to address (clients) {:?}", id, server_addr);

            let socket = socket::bind_sync_server(server_addr)
                .wrapped_msg(ErrorKind::Communication, format!("Failed to bind to address {:?}", server_addr).as_str())?;

            either = Left(socket);
        }

        Ok(either)
    }


    async fn setup_replica_facing_socket(id: NodeId, cfg: &NodeConfig) ->
    Result<Option<Either<SyncListener, AsyncListener>>> {
        let peer_addr = cfg.addrs.get(id.into()).unwrap();

        ///Initialize the replica<->replica facing server
        let replica_listener = if id >= cfg.first_cli {
            //Clients don't have a replica<->replica facing server
            None
        } else {
            let replica_facing_addr = peer_addr.replica_addr.as_ref().unwrap();

            //TODO: Maybe add support for asynchronous listeners?

            let mut server_addr = replica_facing_addr.0.clone();

            //Listen on all interfaces.
            server_addr.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));

            println!("{:?} // Binding to address (replicas) {:?}", id, server_addr);

            Some(Left(socket::bind_sync_server(server_addr)
                .wrapped_msg(ErrorKind::Communication, format!("Failed to bind to address {:?}", server_addr).as_str())?))
        };

        Ok(replica_listener)
    }

    ///Sets up the thread or task (depending on runtime) to receive new connection attempts
    fn setup_socket_connection_workers_socket(self: Arc<Self>,
                                              sync_acceptor: Arc<ServerConfig>,
                                              async_acceptor: TlsAcceptor,
                                              replica_listener: Either<SyncListener, AsyncListener>, ) {
        match replica_listener {
            Left(sync_listener) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let sync_acceptor = sync_acceptor.clone();

                std::thread::Builder::new().name(format!("{:?} connection acceptor", self.id()))
                    .spawn(move || {
                        self.rx_side_accept_sync(first_cli, my_id, sync_listener, sync_acceptor);
                    }).expect("Failed to start replica's connection acceptor");
            }
            Right(async_listener) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let async_acceptor = async_acceptor.clone();

                rt::spawn(self.rx_side_accept(first_cli, my_id, async_listener, async_acceptor));
            }
        }
    }


    /// Bootstrap a `Node`, i.e. create connections between itself and its
    /// peer nodes.
    ///
    /// Rogue messages (i.e. not pertaining to the bootstrapping protocol)
    /// are returned in a `Vec`.
    pub async fn bootstrap(
        cfg: NodeConfig,
    ) -> Result<(Arc<Self>, Vec<Message<D::State, D::Request, D::Reply>>)> {
        let id = cfg.id;

        // initial checks of correctness
        if cfg.n < (3 * cfg.f + 1) {
            return Err("Invalid number of replicas")
                .wrapped(ErrorKind::Communication);
        }

        if id >= NodeId::from(cfg.n) && id < cfg.first_cli {
            return Err("Invalid node ID")
                .wrapped(ErrorKind::Communication);
        }

        ///Initialize the client facing server
        let client_listener = Self::setup_client_facing_socket(id, &cfg).await?;

        let replica_listener = Self::setup_replica_facing_socket(id, &cfg).await?;

        let async_acceptor: TlsAcceptor = cfg.async_server_config.into();
        let async_connector: TlsConnector = cfg.async_client_config.into();

        let sync_acceptor = Arc::new(cfg.sync_server_config);
        let sync_connector = Arc::new(cfg.sync_client_config);

        // node def
        let peer_tx = if id >= cfg.first_cli {
            PeerTx::Client {
                first_cli: cfg.first_cli,
                connected: Arc::new(RwLock::new(IntMap::new())),
            }
        } else {
            PeerTx::Server {
                first_cli: cfg.first_cli,
                connected: Arc::new(RwLock::new(IntMap::new())),
            }
        };

        let shared = Arc::new(NodeShared {
            my_key: cfg.sk,
            peer_keys: cfg.pk,
        });

        //Setup all the peer message reception handling.
        let peers = NodePeers::new(cfg.id, cfg.first_cli, cfg.batch_size,
                                   cfg.clients_per_pool,
                                   cfg.batch_timeout_micros,
                                   cfg.batch_sleep_micros);

        let rng = ThreadSafePrng::new();

        //TESTING
        let sent_rqs = if id > cfg.first_cli {
            Some(Arc::new(std::iter::repeat_with(|| { DashMap::with_capacity(20000) })
                .take(30)
                .collect()))
        } else { None };

        let rcv_rqs = None;

        /*if id < cfg.first_cli {

        //We want the replicas to log recved requests
        let arc = Arc::new(RwLock::new(
            std::iter::repeat_with(|| { DashMap::with_capacity(20000) })
                .take(30)
                .collect()));

        let rqs = arc.clone();

        std::thread::Builder::new().name(String::from("Logging thread")).spawn(move || {
            loop {
                let new_vec: Vec<DashMap<u64, ()>> = std::iter::repeat_with(|| { DashMap::with_capacity(20000) })
                    .take(30)
                    .collect();

                let mut old_vec = {
                    let mut write_guard = rqs.write();

                    std::mem::replace(&mut *write_guard, new_vec)
                };

                let mut print = String::new();

                for bucket in old_vec {
                    for (key, _) in bucket.into_iter() {
                        print = print + " , " + &*format!("{}", key);
                    }
                }

                println!("{}", print);

                std::thread::sleep(Duration::from_secs(1));
            }
        }).expect("Failed to start logging thread");

        Some(arc)
    } else { None };*/
        //

        let mut node = Arc::new(Node {
            id,
            rng,
            shared,
            peer_tx,
            node_handling: peers,
            connector: async_connector.clone(),
            sync_connector: sync_connector.clone(),
            peer_addrs: cfg.addrs,
            first_cli: cfg.first_cli,
            comm_stats: cfg.comm_stats,
            sent_rqs,
            recv_rqs: rcv_rqs,
        });

        let rx_node_clone = node.clone();


        //Setup the worker threads for receiving new connections before starting to connect to
        //other peers
        if let Some(replica_listener) = replica_listener
        {
            rx_node_clone.clone().setup_socket_connection_workers_socket(sync_acceptor.clone(),
                                                                         async_acceptor.clone(),
                                                                         replica_listener);
        }

        //Setup client listener
        {
            rx_node_clone.clone().setup_socket_connection_workers_socket(sync_acceptor.clone(),
                                                                         async_acceptor.clone(),
                                                                         client_listener);
        }

        // tx side (connect to replica)
        if id < cfg.first_cli {
            //If we are a replica, use the std regular sync library as it has better
            //Latency and overall performance (since it does very little context switching)
            let mut rng = prng::State::new();

            node.clone().tx_side_connect_sync(
                cfg.n as u32,
                cfg.first_cli,
                id,
                sync_connector,
                &node.peer_addrs,
                &mut rng,
            );
        } else {
            let node_cpy = node.clone();

            let n = cfg.n as u32;
            let first_cli = cfg.first_cli;

            //Connect to all replicas
            threadpool::execute(move || {
                let mut rng = prng::State::new();

                node_cpy.clone().tx_side_connect_sync(
                    n,
                    first_cli,
                    id,
                    sync_connector,
                    &node_cpy.peer_addrs,
                    &mut rng, );
            });
        }

        let mut rogue = Vec::new();

        while node.node_handling.replica_count() < 4 {

            //Any received messages will be handled by the connection pool buffers
            println!("Connected to {} replicas on the node {:?}", node.node_handling.replica_count(), node.id);

            Delay::new(Duration::from_millis(500)).await;
        }

        println!("Found all nodes required {}", node.node_handling.replica_count());

        // success
        Ok((node, rogue))
    }

    pub fn batch_size(&self) -> usize {
        self.node_handling.batch_size()
    }

    fn resolve_client_rx_connection(&self, node_id: NodeId) -> Option<Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>> {
        self.node_handling.resolve_peer_conn(node_id)
    }

    // clone the shared data and pass it to a new object
    pub fn sign_detached(&self) -> SignDetached {
        let shared = Arc::clone(&self.shared);
        SignDetached { shared }
    }

    /// Returns the public key of the node with the given id `id`.
    pub fn get_public_key(&self, id: NodeId) -> Option<&PublicKey> {
        self.shared.peer_keys.get(id.into())
    }

    /// Reports the id of this `Node`.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Reports the id of the first client.
    pub fn first_client_id(&self) -> NodeId {
        self.first_cli
    }

    /// Returns a `SendNode` sharing the same handles as this `Node`.
    pub fn send_node(self: &Arc<Self>) -> SendNode<D> {
        SendNode {
            id: self.id,
            first_cli: self.first_cli,
            rng: prng::State::new(),
            shared: Arc::clone(&self.shared),
            peer_tx: self.peer_tx.clone(),
            parent_node: Arc::clone(self),
            channel: Arc::clone(self.loopback_channel()),
            comm_stats: self.comm_stats.clone(),
        }
    }

    /// Returns a handle to the loopback channel of this `Node`. (Sending messages to ourselves)
    pub fn loopback_channel(&self) -> &Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.peer_loopback()
    }

    /// Send a `SystemMessage` to a single destination.
    ///
    /// This method is somewhat more efficient than calling `broadcast()`
    /// on a single target id.
    pub fn send(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
        flush: bool,
    ) {
        let start_instant = Instant::now();

        match self.resolve_client_rx_connection(target) {
            None => {
                error!("Failed to send message to client {:?} as the connection to it was not found!", target);
            }
            Some(conn) => {
                let send_to = Self::send_to(
                    flush,
                    self.id,
                    target,
                    None,
                    conn,
                    &self.peer_tx,
                );

                let my_id = self.id;
                let nonce = self.rng.next_state();

                let comm_stats = if let Some(comm_stats) = &self.comm_stats {
                    Some((comm_stats.clone(), start_instant))
                } else {
                    None
                };

                Self::send_impl(message, send_to, my_id, target, self.first_cli, nonce,
                                comm_stats);
            }
        };
    }

    /// Send a `SystemMessage` to a single destination.
    ///
    /// This method is somewhat more efficient than calling `broadcast()`
    /// on a single target id.
    ///
    /// This variant of `send()` signs the sent message.
    pub fn send_signed(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
    ) {
        match self.resolve_client_rx_connection(target) {
            None => {
                error!("Failed to send message to client {:?} as the connection to it was not found!", target);
            }
            Some(conn) => {
                let start_time = Instant::now();

                let send_to = Self::send_to(
                    true,
                    self.id,
                    target,
                    Some(&self.shared),
                    conn,
                    &self.peer_tx,
                );

                let my_id = self.id;

                let nonce = self.rng.next_state();

                let comm_stats = if let Some(comm_stats) = &self.comm_stats {
                    Some((comm_stats.clone(), start_time))
                } else {
                    None
                };

                Self::send_impl(message, send_to, my_id, target, self.first_cli, nonce,
                                comm_stats);
            }
        };
    }

    #[inline]
    fn send_impl(
        message: SystemMessage<D::State, D::Request, D::Reply>,
        mut send_to: SendTo<D>,
        my_id: NodeId,
        target: NodeId,
        first_cli: NodeId,
        nonce: u64,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            // serialize
            let start_serialization = Instant::now();

            let mut buf: Buf = Buf::new();
            let digest = <D as DigestData>::serialize_digest(
                &message,
                &mut buf,
            ).unwrap();

            if let Some((comm_stats, _)) = &comm_stats {
                let time_taken_signing = Instant::now().duration_since(start_serialization).as_nanos();

                //Broadcasts are always for replicas, so make this
                comm_stats.insert_message_signing_time(NodeId::from(0u32), time_taken_signing);
            }

            // send
            if my_id == target {
                // Right -> our turn

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                let before_send_time = Instant::now();

                //Send to myself, always synchronous since only replicas send to themselves
                send_to.value_sync(Right((message, nonce, digest, buf)), None);

                if let Some((comm_stats, start_time)) = &comm_stats {
                    let dur_since = before_send_time.duration_since(*start_time).as_nanos();

                    let dur_send = Instant::now().duration_since(before_send_time).as_nanos();

                    comm_stats.insert_message_passing_latency_own(dur_since);
                    comm_stats.insert_message_sending_time_own(dur_send);
                    comm_stats.register_rq_sent(my_id);
                }
            } else {

                // Left -> peer turn
                match send_to.socket_type().unwrap() {
                    ConnectionHandle::Async(_) => {
                        rt::spawn(async move {
                            //Measuring time taken to get to the point of sending the message
                            //We don't actually want to measure how long it takes to send the message
                            let before_send_time = Instant::now();

                            send_to.value(Left((nonce, digest, buf))).await;

                            if let Some((comm_stats, start_time)) = &comm_stats {
                                let dur_since = before_send_time.duration_since(*start_time).as_nanos();

                                comm_stats.insert_message_passing_latency(target, dur_since);
                            }
                        });
                    }
                    ConnectionHandle::Sync(_) => {
                        //Measuring time taken to get to the point of sending the message
                        //We don't actually want to measure how long it takes to send the message
                        let before_send_time = Instant::now();

                        send_to.value_sync(Left((nonce, digest, buf)), None);

                        if let Some((comm_stats, start_time)) = &comm_stats {
                            let dur_since = before_send_time.duration_since(*start_time).as_nanos();

                            comm_stats.insert_message_passing_latency(target, dur_since);
                        }
                    }
                }
            }
        });
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    pub fn broadcast(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let start_time = Instant::now();

        let (mine, others) = self.send_tos(
            self.id,
            &self.peer_tx,
            None,
            targets,
        );

        let nonce = self.rng.next_state();

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        Self::broadcast_impl(message, mine, others, self.first_cli, nonce,
                             comm_stats,);
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    ///
    /// This variant of `broadcast()` signs the sent message.
    pub fn broadcast_signed(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let start_time = Instant::now();

        let (mine, others) = self.send_tos(
            self.id,
            &self.peer_tx,
            Some(&self.shared),
            targets,
        );

        let nonce = self.rng.next_state();

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        Self::broadcast_impl(message, mine, others, self.first_cli, nonce,
                             comm_stats);
    }

    pub fn broadcast_serialized(
        &self,
        messages: IntMap<StoredSerializedSystemMessage<D>>,
    ) {
        let start_time = Instant::now();
        let headers = messages
            .values()
            .map(|stored| stored.header());

        let (mine, others) = self.serialized_send_tos(
            self.id,
            &self.peer_tx,
            headers,
        );

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        Self::broadcast_serialized_impl(messages, mine, others,
                                        self.first_client_id(),
                                        comm_stats);
    }

    #[inline]
    fn broadcast_serialized_impl(
        mut messages: IntMap<StoredSerializedSystemMessage<D>>,
        my_send_to: Option<SerializedSendTo<D>>,
        other_send_tos: SerializedSendTos<D>,
        first_client: NodeId,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            // send to ourselves
            if let Some(mut send_to) = my_send_to {
                let id = match &send_to {
                    SerializedSendTo::Me { id, .. } => *id,
                    _ => unreachable!(),
                };

                let (header, message) = messages
                    .remove(id.into())
                    .map(|stored| stored.into_inner())
                    .unwrap();

                let comm_stats = comm_stats.clone();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                let before_sending = Instant::now();

                send_to.value_sync(header, message);

                if let Some((comm_stats, start_send)) = &comm_stats {
                    let dur_since = before_sending.duration_since(*start_send).as_nanos();

                    let dur_sending = Instant::now().duration_since(before_sending).as_nanos();

                    comm_stats.insert_message_passing_latency_own(dur_since);
                    comm_stats.insert_message_sending_time_own(dur_sending);

                    comm_stats.register_rq_sent(id);
                }
            }

            // send to others
            for mut send_to in other_send_tos {
                let id = match &send_to {
                    SerializedSendTo::Peers { id, .. } => *id,
                    _ => unreachable!(),
                };

                let (header, message) = messages
                    .remove(id.into())
                    .map(|stored| stored.into_inner())
                    .unwrap();

                let comm_stats = comm_stats.clone();

                match send_to.socket_type().unwrap() {
                    ConnectionHandle::Async(_) => {
                        rt::spawn(async move {
                            //Measuring time taken to get to the point of sending the message
                            //We don't actually want to measure how long it takes to send the message
                            let before_sending = Instant::now();

                            send_to.value(header, message).await;

                            if let Some((comm_stats, start_send)) = &comm_stats {
                                let dur_since = before_sending.duration_since(*start_send).as_nanos();

                                comm_stats.insert_message_passing_latency(id, dur_since);
                            }
                        });
                    }
                    ConnectionHandle::Sync(_) => {
                        //Measuring time taken to get to the point of sending the message
                        //We don't actually want to measure how long it takes to send the message
                        let before_sending = Instant::now();

                        send_to.value_sync(header, message);

                        if let Some((comm_stats, start_send)) = &comm_stats {
                            let dur_since = before_sending.duration_since(*start_send).as_nanos();

                            comm_stats.insert_message_passing_latency(id, dur_since);
                        }
                    }
                }
            }
        });
    }

    #[inline]
    fn broadcast_impl(
        message: SystemMessage<D::State, D::Request, D::Reply>,
        my_send_to: Option<SendTo<D>>,
        other_send_tos: SendTos<D>,
        first_cli: NodeId,
        nonce: u64,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            let start_serialization = Instant::now();

            // serialize
            let mut buf: Buf = Buf::new();

            let digest = <D as DigestData>::serialize_digest(
                &message,
                &mut buf,
            ).unwrap();

            if let Some((comm_stats, _)) = &comm_stats {
                let time_taken_signing = Instant::now().duration_since(start_serialization).as_nanos();

                //Broadcasts are always for replicas, so make this
                comm_stats.insert_message_signing_time(NodeId::from(0u32), time_taken_signing);
            }

            let rq_key = match &message {
                //We only care about requests, as we only want this for the client
                SystemMessage::Request(req) => {
                    let session = get_request_key(req.session_id(), req.sequence_number());

                    Some(session)
                }
                _ => {
                    None
                }
            };

            // send to ourselves
            if let Some(mut send_to) = my_send_to {
                let id = match &send_to {
                    SendTo::Me { my_id, .. } => *my_id,
                    _ => unreachable!(),
                };

                let buf = buf.clone();

                let comm_stats = comm_stats.clone();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                let before_send_time = Instant::now();

                // Right -> our turn
                send_to.value_sync(Right((message, nonce, digest, buf)), rq_key.clone());

                if let Some((comm_stats, start_time)) = &comm_stats {
                    let dur_since = before_send_time.duration_since(*start_time).as_nanos();

                    let dur_send = Instant::now().duration_since(before_send_time).as_nanos();

                    comm_stats.insert_message_passing_latency_own(dur_since);
                    comm_stats.insert_message_sending_time_own(dur_send);
                    comm_stats.register_rq_sent(id);
                }
            }

            // send to others

            for mut send_to in other_send_tos {
                let id = match &send_to {
                    SendTo::Peers { peer_id, .. } => *peer_id,
                    _ => unreachable!()
                };

                let buf = buf.clone();

                let comm_stats = comm_stats.clone();

                match send_to.socket_type().unwrap() {
                    ConnectionHandle::Async(_) => {
                        rt::spawn(async move {
                            // Left -> peer turn
                            send_to.value(Left((nonce, digest, buf))).await;
                        });
                    }
                    ConnectionHandle::Sync { .. } => {
                        //Measuring time taken to get to the point of sending the message
                        //We don't actually want to measure how long it takes to send the message
                        let before_send_time = Instant::now();

                        send_to.value_sync(Left((nonce, digest, buf)), rq_key.clone());

                        if let Some((comm_stats, start_time)) = &comm_stats {
                            let dur_since = before_send_time.duration_since(*start_time).as_nanos();

                            comm_stats.insert_message_passing_latency(id, dur_since);
                        }
                    }
                }
            }


            // NOTE: an either enum is used, which allows
            // rustc to prove only one task gets ownership
            // of the `message`, i.e. `Right` = ourselves
        });
    }

    #[inline]
    fn send_tos(
        &self,
        my_id: NodeId,
        peer_tx: &PeerTx,
        shared: Option<&Arc<NodeShared>>,
        targets: impl Iterator<Item=NodeId>,
    ) -> (Option<SendTo<D>>, SendTos<D>) {
        let mut my_send_to = None;
        let mut other_send_tos = SendTos::new();

        self.create_send_tos(
            my_id,
            shared,
            peer_tx,
            targets,
            &mut my_send_to,
            &mut other_send_tos,
        );

        (my_send_to, other_send_tos)
    }

    #[inline]
    fn serialized_send_tos<'a>(
        &self,
        my_id: NodeId,
        peer_tx: &PeerTx,
        headers: impl Iterator<Item=&'a Header>,
    ) -> (Option<SerializedSendTo<D>>, SerializedSendTos<D>) {
        let mut my_send_to = None;
        let mut other_send_tos = SerializedSendTos::new();

        self.create_serialized_send_tos(my_id,
                                        peer_tx, headers,
                                        &mut my_send_to,
                                        &mut other_send_tos);

        (my_send_to, other_send_tos)
    }

    #[inline]
    fn create_serialized_send_tos<'a>(
        &self,
        my_id: NodeId,
        peer_tx: &PeerTx,
        headers: impl Iterator<Item=&'a Header>,
        mine: &mut Option<SerializedSendTo<D>>,
        others: &mut SerializedSendTos<D>,
    ) {
        for header in headers {
            let id = header.to();
            if id == my_id {
                let s = SerializedSendTo::Me {
                    id,
                    //get our own channel to send to ourselves
                    tx: self.loopback_channel().clone(),
                };
                *mine = Some(s);
            } else {
                let rx = self.resolve_client_rx_connection(id);

                let (sock, tx) = match (peer_tx.find_peer(id.id() as u64), rx) {
                    (None, None) => {
                        error!("Could not find socket nor rx for peer {:?}", id.id());

                        continue;
                    }
                    (None, Some(tx)) => {
                        error!("Cound not find socket but found rx, closing it {:?}", id.id());

                        tx.disconnect();

                        continue;
                    }
                    (Some(sock), None) => {
                        error!("Found socket but didn't find rx? Closing {:?}", id.id());

                        continue;
                    }
                    (Some(socket), Some(tx)) => {
                        (socket, tx)
                    }
                };

                //Get the RX channel for the corresponding peer to mark as disconnected if the sending fails

                let s = SerializedSendTo::Peers {
                    id,
                    our_id: my_id,
                    sock,
                    //Get the RX channel for the peer to mark as DCed if it fails
                    tx,
                };

                others.push(s);
            }
        }
    }

    #[inline]
    fn create_send_tos(
        &self,
        my_id: NodeId,
        shared: Option<&Arc<NodeShared>>,
        tx_peers: &PeerTx,
        targets: impl Iterator<Item=NodeId>,
        mine: &mut Option<SendTo<D>>,
        others: &mut SendTos<D>,
    ) {
        for id in targets {
            if id == my_id {
                let s = SendTo::Me {
                    my_id,
                    //get our own channel to send to ourselves
                    tx: self.loopback_channel().clone(),
                    shared: shared.map(|sh| Arc::clone(sh)),
                };
                *mine = Some(s);
            } else {
                let sock = tx_peers.find_peer(id.id() as u64);

                let rx_conn = self.resolve_client_rx_connection(id);

                let (sock, rx_conn) = match (sock, rx_conn) {
                    (None, None) => {
                        error!("Could not find socket nor rx for peer {:?}", id.id());

                        continue;
                    }
                    (None, Some(tx)) => {
                        error!("Cound not find socket but found rx, closing it {:?}", id.id());

                        tx.disconnect();

                        continue;
                    }
                    (Some(sock), None) => {
                        error!("Found socket but didn't find rx? Closing {:?}", id.id());

                        continue;
                    }
                    (Some(socket), Some(tx)) => {
                        (socket, tx)
                    }
                };

                let s = SendTo::Peers {
                    sock,
                    my_id,
                    peer_id: id,
                    flush: true,
                    //Get the RX channel for the peer to mark as DCed if it fails
                    tx: rx_conn,
                    shared: shared.map(|sh| Arc::clone(sh)),
                };

                others.push(s);
            }
        }
    }

    #[inline]
    fn send_to(
        flush: bool,
        my_id: NodeId,
        peer_id: NodeId,
        shared: Option<&Arc<NodeShared>>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
        peer_tx: &PeerTx,
    ) -> SendTo<D> {
        let shared = shared.map(|sh| Arc::clone(sh));
        if my_id == peer_id {
            SendTo::Me {
                shared,
                my_id,
                tx: cli,
            }
        } else {
            let sock = peer_tx.find_peer(peer_id.id() as u64).unwrap().clone();

            SendTo::Peers {
                flush,
                sock,
                shared,
                peer_id,
                my_id,
                tx: cli,
            }
        }
    }

    //Get how many pending messages are in the requests channel
    pub fn rqs_len_from_clients(&self) -> usize {
        self.node_handling.rqs_len_from_clients()
    }

    //Receive messages from the clients we are connected to
    pub fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.receive_from_clients(timeout)
    }

    pub fn try_recv_from_clients(&self) -> Result<Option<Vec<Message<D::State, D::Request, D::Reply>>>> {
        self.node_handling.try_receive_from_clients()
    }

    //Receive messages from the replicas we are connected to
    pub fn receive_from_replicas(&self) -> Result<Message<D::State, D::Request, D::Reply>> {
        self.node_handling.receive_from_replicas()
    }

    /// Registers the newly created transmission socket to the peer
    pub fn handle_connected_tx(&self, peer_id: NodeId, sock: SecureSocketSend) {
        debug!("{:?} // Connected TX to peer {:?}", self.id, peer_id);

        let conn_handle = match sock {
            SecureSocketSend::Async(socket) => {
                peer_sending_threads::initialize_async_sending_task_for(
                    peer_id.clone(),
                    socket,
                    self.comm_stats.clone(),
                )
            }
            SecureSocketSend::Sync(socket) => {
                peer_sending_threads::initialize_sync_sending_thread_for(
                    peer_id.clone(),
                    socket,
                    self.comm_stats.clone(),
                    self.sent_rqs.clone()
                )
            }
        };

        self.peer_tx.add_peer(peer_id.id() as u64, conn_handle);
    }

    ///Connect to all other replicas in the cluster
    #[inline]
    fn tx_side_connect_sync(
        self: Arc<Self>,
        n: u32,
        first_cli: NodeId,
        my_id: NodeId,
        connector: Arc<ClientConfig>,
        addrs: &IntMap<PeerAddr>,
        rng: &mut prng::State,
    ) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != my_id) {
            debug!("{:?} // Connecting to the node {:?}",my_id, peer_id);

            let addr = match addrs.get(peer_id.id() as u64) {
                None => {
                    error!("{:?} // Failed to find peer address for peer {:?}", my_id, peer_id);

                    continue;
                }
                Some(addr) => { addr }
            }.clone();

            let connector = connector.clone();
            let nonce = rng.next_state();

            let peer_addr = if my_id >= first_cli {
                addr.client_addr.clone()
            } else {
                addr.replica_addr.as_ref().unwrap().clone()
            };

            //println!("Attempting to connect to peer {:?} with address {:?} from node {:?}", peer_id, addr, my_id);

            let arc = self.clone();

            threadpool::execute(move || {
                debug!("{:?} // Starting connection to node {:?} with address {:?}",my_id, peer_id, peer_addr.0);

                arc.tx_side_connect_task_sync(my_id, first_cli, peer_id,
                                              nonce, connector, peer_addr);
            });
        }
    }

    ///Connect to all other replicas in the cluster, but without using tokio (utilizing regular
    /// synchronous APIs)
    #[inline]
    #[instrument(skip(self, addrs, rng, first_cli, connector))]
    async fn tx_side_connect(
        self: Arc<Self>,
        n: u32,
        first_cli: NodeId,
        my_id: NodeId,
        connector: TlsConnector,
        addrs: &IntMap<PeerAddr>,
        rng: &mut prng::State,
    ) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != my_id) {
            debug!("{:?} // Connecting to the node {:?}",my_id, peer_id);

            // FIXME: this line can crash the program if the user
            // provides an invalid HashMap, maybe return a Result<()>
            // from this function
            let addr = match addrs.get(peer_id.id() as u64) {
                None => {
                    error!("{:?} // Failed to find peer address for peer {:?}", my_id, peer_id);

                    continue;
                }
                Some(addr) => { addr }
            };

            let connector = connector.clone();
            let nonce = rng.next_state();

            let peer_addr = if my_id >= first_cli {
                addr.client_addr.clone()
            } else {
                addr.replica_addr.as_ref().unwrap().clone()
            };

            //println!("Attempting to connect to peer {:?} with address {:?} from node {:?}", peer_id, addr, my_id);

            let arc = self.clone();

            rt::spawn(async move {
                debug!("{:?} // Starting connection to node {:?}",my_id, peer_id);

                arc.tx_side_connect_task(my_id, first_cli, peer_id,
                                         nonce, connector, peer_addr).await;
            });
        }
    }

    ///Connect to a particular replica
    /// Should be called from a threadpool as initializing a thread just for this
    /// Would be kind of overkill
    fn tx_side_connect_task_sync(
        self: Arc<Self>,
        my_id: NodeId,
        first_cli: NodeId,
        peer_id: NodeId,
        nonce: u64,
        connector: Arc<ClientConfig>,
        (addr, hostname): (SocketAddr, String),
    ) {
        const SECS: u64 = 1;
        const RETRY: usize = 3 * 60;

        // NOTE:
        // ========
        //
        // 1) not an issue if `tx` is closed, this is not a
        // permanently running task, so channel send failures
        // are tolerated
        //
        // 2) try to connect up to `RETRY` times, then announce
        // failure with a channel send op
        for _try in 0..RETRY {
            match socket::connect_sync(addr) {
                Ok(mut sock) => {
                    // create header
                    let (header, _) = WireMessage::new(
                        my_id,
                        peer_id,
                        vec![],
                        nonce,
                        None,
                        None,
                    ).into_inner();

                    // serialize header
                    let mut buf = [0; Header::LENGTH];
                    header.serialize_into(&mut buf[..]).unwrap();

                    // send header
                    if let Err(_) = sock.write_all(&buf[..]) {
                        // errors writing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    if let Err(_) = sock.flush() {
                        // errors flushing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    // TLS handshake; drop connection if it fails
                    let sock = if peer_id >= first_cli || my_id >= first_cli {
                        SecureSocketSendSync::Plain(sock)
                    } else {
                        let dns_ref = webpki::DNSNameRef::try_from_ascii_str(hostname.as_str()).expect("Failed to parse DNS hostname");

                        let mut session = rustls::ClientSession::new(&connector, dns_ref);

                        SecureSocketSendSync::new_tls(session, sock)
                    };

                    let final_sock = SecureSocketSend::Sync(SocketSendSync::new(sock));

                    // success
                    self.handle_connected_tx(peer_id, final_sock);

                    //println!("Ended connection attempt {} for Node {:?} from peer {:?}", _try, peer_id, my_id);
                    return;
                }
                Err(err) => {
                    error!("{:?} // Error on connecting to addr {:?}: {:?}", peer_id, addr, err);
                }
            }

            // sleep for `SECS` seconds and retry
            std::thread::sleep(Duration::from_secs(SECS));
        }
    }

    #[instrument(skip(self, first_cli, nonce, connector))]
    async fn tx_side_connect_task(
        self: Arc<Self>,
        my_id: NodeId,
        first_cli: NodeId,
        peer_id: NodeId,
        nonce: u64,
        connector: TlsConnector,
        (addr, hostname): (SocketAddr, String),
    ) {
        const SECS: u64 = 1;
        const RETRY: usize = 3 * 60;

        // NOTE:
        // ========
        //
        // 1) not an issue if `tx` is closed, this is not a
        // permanently running task, so channel send failures
        // are tolerated
        //
        // 2) try to connect up to `RETRY` times, then announce
        // failure with a channel send op
        for _try in 0..RETRY {
            //println!("Trying attempt {} for Node {:?} from peer {:?}", _try, peer_id, my_id);
            if let Ok(mut sock) = socket::connect_async(addr).await {
                // create header
                let (header, _) = WireMessage::new(
                    my_id,
                    peer_id,
                    vec![],
                    nonce,
                    None,
                    None,
                ).into_inner();

                // serialize header
                let mut buf = [0; Header::LENGTH];
                header.serialize_into(&mut buf[..]).unwrap();

                // send header
                if let Err(_) = sock.write_all(&buf[..]).await {
                    // errors writing -> faulty connection;
                    // drop this socket
                    break;
                }

                if let Err(_) = sock.flush().await {
                    // errors flushing -> faulty connection;
                    // drop this socket
                    break;
                }

                // TLS handshake; drop connection if it fails
                let sock = if peer_id >= first_cli || my_id >= first_cli {
                    debug!("{:?} // Connecting with plain text to node {:?}", my_id, peer_id);

                    SecureSocketSendAsync::Plain(BufWriter::new(sock))
                } else {
                    match connector.connect(hostname, sock).await {
                        Ok(s) => SecureSocketSendAsync::Tls(s),
                        Err(_) => { break; }
                    }
                };

                let final_sock = SecureSocketSend::Async(SocketSendAsync::new(sock));

                // success
                self.handle_connected_tx(peer_id, final_sock);

                return;
            }

            // sleep for `SECS` seconds and retry
            Delay::new(Duration::from_secs(SECS)).await;
        }

        // announce we have failed to connect to the peer node
        //if we fail to connect, then just ignore
    }

    ///Accept synchronous connections
    fn rx_side_accept_sync(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        listener: SyncListener,
        acceptor: Arc<ServerConfig>,
    ) {
        loop {
            if let Ok(sock) = listener.accept() {
                let replica_acceptor = acceptor.clone();

                let rx_ref = self.clone();

                let first_cli = first_cli.clone();
                let my_id = my_id.clone();

                threadpool::execute(move || {
                    rx_ref.rx_side_establish_conn_task_sync(first_cli, my_id, replica_acceptor, sock);
                });
            }
        }
    }

    ///Accept connections from other nodes. Utilizes the async environment
    #[instrument(skip(self, first_cli, listener, acceptor))]
    async fn rx_side_accept(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        listener: AsyncListener,
        acceptor: TlsAcceptor,
    ) {
        loop {
            debug!("{:?} // Awaiting for new connections", my_id);

            match listener.accept().await {
                Ok(sock) => {
                    let rand = fastrand::u32(0..);

                    debug!("{:?} // Accepting connection with rand {}", my_id, rand);

                    let acceptor = acceptor.clone();

                    rt::spawn(self.clone().rx_side_establish_conn_task(first_cli, my_id, acceptor, sock, rand));
                }
                Err(err) => {
                    error!("{:?} // Failed to accept connection {:?}", my_id, err);
                }
            }
        }
    }

    /// performs a cryptographic handshake with a peer node;
    /// header doesn't need to be signed, since we won't be
    /// storing this message in the log
    /// So the same as [`rx_side_accept_task()`] but synchronously.
    fn rx_side_establish_conn_task_sync(self: Arc<Self>,
                                        first_cli: NodeId,
                                        my_id: NodeId,
                                        acceptor: Arc<ServerConfig>,
                                        mut sock: SyncSocket) {
        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            //println!("Node {:?} received connection from node", my_id);

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, vec![]) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            //println!("Node {:?} received connection from node {:?}", my_id, peer_id);

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                SecureSocketRecvSync::Plain(sock)
            } else {
                let mut tls_session = rustls::ServerSession::new(&acceptor);

                SecureSocketRecvSync::new_tls(tls_session, sock)
            };

            let cpy_peer_id = peer_id.clone();

            std::thread::Builder::new().name(format!("Reception thread client {:?}", peer_id)).spawn(move || {
                self.handle_connected_rx_sync(peer_id, sock);
            }).expect(format!("Failed to create client connection thread for client {:?}", cpy_peer_id).as_str());

            return;
        }
    }

    /// performs a cryptographic handshake with a peer node;
    /// header doesn't need to be signed, since we won't be
    /// storing this message in the log
    #[instrument(skip(self, first_cli, acceptor, sock, rand))]
    async fn rx_side_establish_conn_task(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        acceptor: TlsAcceptor,
        mut sock: AsyncSocket,
        rand: u32,
    ) {
        let mut buf_header = [0; Header::LENGTH];

        debug!("{:?} // Started handling connection from node {}", my_id, rand);

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            debug!("{:?} // Received header from node {}", my_id, rand);

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, vec![]) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            debug!("{:?} // Received connection from node {:?}, {}", my_id, peer_id, rand);

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                SecureSocketRecvAsync::Plain(BufReader::new(sock))
            } else {
                match acceptor.accept(sock).await {
                    Ok(s) => SecureSocketRecvAsync::Tls(s),
                    Err(_) => {
                        error!("{:?} // Failed to setup tls connection to node {:?}", my_id, peer_id);

                        break;
                    }
                }
            };

            self.handle_connected_rx(peer_id, sock).await;

            return;
        }

        // announce we have failed to connect to the peer node
    }

    /// Handles client connections, attempts to connect to the client that connected to us
    /// If we are a replica and the other client is a node
    #[instrument(skip(self, sock))]
    pub async fn handle_connected_rx(self: Arc<Self>, peer_id: NodeId, mut sock: SecureSocketRecvAsync) {
        // we are a server node
        if let PeerTx::Server { .. } = &self.peer_tx {
            // the node whose conn we accepted is a client
            // and we aren't connected to it yet
            if peer_id >= self.first_cli {
                // fetch client address
                //
                match self.peer_addrs.get(peer_id.id() as u64) {
                    None => {
                        error!("{:?} // Failed to find peer address for tx connection for peer {:?}", self.id(), peer_id);
                    }
                    Some(addr) => {
                        debug!("{:?} // Received connection from client {:?}, establish TX connection on port {:?}", self.id, peer_id,
                            addr.client_addr.0);

                        // connect
                        let nonce = self.rng.next_state();

                        rt::spawn(Self::tx_side_connect_task(
                            self.clone(),
                            self.id,
                            self.first_cli,
                            peer_id,
                            nonce,
                            self.connector.clone(),
                            addr.client_addr.clone(),
                        ));
                    }
                };
            }
        }

        //Init the per client queue and start putting the received messages into it
        debug!("{:?} // Handling connection of peer {:?}", self.id, peer_id);

        let client = self.node_handling.init_peer_conn(peer_id.clone());

        let mut buf = SmallVec::<[u8; 16384]>::new();

        // TODO
        //  - verify signatures???
        //  - exit condition (when the `Replica` or `Client` is dropped)
        loop {
            // reserve space for header
            buf.clear();
            buf.resize(Header::LENGTH, 0);

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf[..Header::LENGTH]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf[..Header::LENGTH]).unwrap();

            // reserve space for message
            //
            // FIXME: add a max bound on the message payload length;
            // if the length is exceeded, reject connection;
            // the bound can be application defined, i.e.
            // returned by `SharedData`
            buf.clear();
            buf.reserve(header.payload_length());
            buf.resize(header.payload_length(), 0);

            // read the peer's payload
            if let Err(_) = sock.read_exact(&mut buf[..header.payload_length()]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // deserialize payload
            let message = match D::deserialize_message(&buf[..header.payload_length()]) {
                Ok(m) => m,
                Err(_) => {
                    // errors deserializing -> faulty connection;
                    // drop this socket
                    break;
                }
            };

            let msg = Message::System(header, message);

            client.push_request(msg).await;

            if let Some(comm_stats) = &self.comm_stats {
                comm_stats.register_rq_received(peer_id);
            }
        }

        // announce we have disconnected
        client.disconnect();
    }

    /// Handles replica connections, reading from stream and pushing message into the correct queue
    pub fn handle_connected_rx_sync(self: Arc<Self>, peer_id: NodeId, mut sock: SecureSocketRecvSync) {
        if let PeerTx::Server { .. } = &self.peer_tx {
            if peer_id >= self.first_cli {
                //If we are the server and the other connection is a client
                //We want to automatically establish a tx connection as well as a
                //rx connection

                // fetch client address
                //
                // FIXME: this line can crash the program if the user
                // provides an invalid HashMap
                let addr = self.peer_addrs.get(peer_id.id() as u64)
                    .expect(format!("Failed to get address for client {:?}", peer_id).as_str()).client_addr.clone();

                debug!("{:?} // Received connection from client {:?}, establish TX connection on address {:?}", self.id, peer_id,
                    addr.0);

                // connect
                let nonce = self.rng.next_state();

                let self_cpy = self.clone();

                threadpool::execute(move || {
                    let id = self_cpy.id;
                    let first_cli = self_cpy.first_cli;
                    let sync_conn = self_cpy.sync_connector.clone();

                    Self::tx_side_connect_task_sync(
                        self_cpy,
                        id,
                        first_cli,
                        peer_id,
                        nonce,
                        sync_conn,
                        addr,
                    );
                });
            }
        }

        let client = self.node_handling.init_peer_conn(peer_id.clone());

        let mut buf = SmallVec::<[u8; 16384]>::new();

        // TODO
        //  - verify signatures???
        //  - exit condition (when the `Replica` or `Client` is dropped)
        loop {
            // reserve space for header
            buf.clear();
            buf.resize(Header::LENGTH, 0);

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf[..Header::LENGTH]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf[..Header::LENGTH]).unwrap();

            // reserve space for message
            //
            // FIXME: add a max bound on the message payload length;
            // if the length is exceeded, reject connection;
            // the bound can be application defined, i.e.
            // returned by `SharedData`
            buf.clear();
            buf.reserve(header.payload_length());
            buf.resize(header.payload_length(), 0);

            // read the peer's payload
            if let Err(_) = sock.read_exact(&mut buf[..header.payload_length()]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // deserialize payload
            let message = match D::deserialize_message(&buf[..header.payload_length()]) {
                Ok(m) => m,
                Err(_) => {
                    // errors deserializing -> faulty connection;
                    // drop this socket
                    break;
                }
            };

            let req_key = match &message {
                SystemMessage::Request(req) => {
                    Some(get_request_key(req.session_id(), req.sequence_number()))
                }
                _ => { None }
            };

            let msg = Message::System(header, message);

            client.push_request_sync(msg);

            if let Some(comm_stats) = &self.comm_stats {
                comm_stats.register_rq_received(peer_id);
            }

            if let Some(req_recv) = &self.recv_rqs {
                if let Some(req_key) = req_key {
                    let guard = req_recv.read();

                    let bucket = &guard[req_key as usize % guard.len()];

                    bucket.insert(req_key, ());
                }
            }
        }

        // announce we have disconnected
        client.disconnect();
    }
}

/// Represents a node with sending capabilities only.
pub struct SendNode<D: SharedData + 'static> {
    id: NodeId,
    first_cli: NodeId,
    shared: Arc<NodeShared>,
    rng: prng::State,
    peer_tx: PeerTx,
    parent_node: Arc<Node<D>>,
    channel: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    comm_stats: Option<Arc<CommStats>>,
}

impl<D: SharedData> Clone for SendNode<D> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            first_cli: self.first_cli,
            rng: prng::State::new(),
            shared: Arc::clone(&self.shared),
            peer_tx: self.peer_tx.clone(),
            parent_node: self.parent_node.clone(),
            channel: self.channel.clone(),
            comm_stats: self.comm_stats.clone(),
        }
    }
}

impl<D> SendNode<D>
    where
        D: SharedData + 'static,
        D::State: Send + Clone + 'static,
        D::Request: Send + 'static,
        D::Reply: Send + 'static,
{
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn loopback_channel(&self) -> &Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>> {
        &self.channel
    }

    pub fn parent_node(&self) -> &Arc<Node<D>> {
        &self.parent_node
    }

    /// Check the `master_channel()` documentation for `Node`.

    /// Check the `send()` documentation for `Node`.
    pub fn send(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
        flush: bool,
    ) {
        let start_time = Instant::now();

        match self.parent_node.resolve_client_rx_connection(target) {
            None => {
                error!("Failed to send message to client {:?} as the connection to it was not found!", target);
            }
            Some(conn) => {
                let send_to = <Node<D>>::send_to(
                    flush,
                    self.id,
                    target,
                    None,
                    conn,
                    &self.peer_tx,
                );

                let my_id = self.id;
                let nonce = self.rng.next_state();

                let comm_stats = if let Some(comm_stats) = &self.comm_stats {
                    Some((comm_stats.clone(), start_time))
                } else {
                    None
                };

                <Node<D>>::send_impl(message, send_to, my_id, target, self.first_cli, nonce,
                                     comm_stats);
            }
        }
    }

    /// Check the `send_signed()` documentation for `Node`.
    pub fn send_signed(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
    ) {
        let start_time = Instant::now();

        match self.parent_node.resolve_client_rx_connection(target) {
            None => {
                error!("Failed to send message to client {:?} as the connection to it was not found!", target);
            }
            Some(conn) => {
                let send_to = <Node<D>>::send_to(
                    true,
                    self.id,
                    target,
                    Some(&self.shared),
                    conn,
                    &self.peer_tx,
                );
                let my_id = self.id;
                let nonce = self.rng.next_state();

                let comm_stats = if let Some(comm_stats) = &self.comm_stats {
                    Some((comm_stats.clone(), start_time))
                } else {
                    None
                };

                <Node<D>>::send_impl(message, send_to, my_id, target, self.first_cli, nonce,
                                     comm_stats);
            }
        }
    }

    /// Check the `broadcast()` documentation for `Node`.
    pub fn broadcast(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let start_time = Instant::now();

        let (mine, others) = self.parent_node.send_tos(
            self.id,
            &self.peer_tx,
            None,
            targets,
        );

        let nonce = self.rng.next_state();

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        <Node<D>>::broadcast_impl(message, mine, others, self.first_cli, nonce,
                                  comm_stats);
    }

    /// Check the `broadcast_signed()` documentation for `Node`.
    pub fn broadcast_signed(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let start_time = Instant::now();

        let (mine, others) = self.parent_node.send_tos(
            self.id,
            &self.peer_tx,
            Some(&self.shared),
            targets,
        );

        let nonce = self.rng.next_state();

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        <Node<D>>::broadcast_impl(message, mine, others, self.first_cli, nonce,
                                  comm_stats);
    }
}

// helper type used when either a `send()` or a `broadcast()`
// is called by a `Node` or `SendNode`.
//
// holds some data that can be shared between threads, relevant
// to a network write operation, or channel write operation,
// depending on whether we're sending a message to a peer node
// or ourselves
pub enum SendTo<D: SharedData> {
    Me {
        // our id
        my_id: NodeId,
        // shared data
        shared: Option<Arc<NodeShared>>,
        // a handle to our client handle
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
    Peers {
        // should we flush write calls?
        flush: bool,
        // our id
        my_id: NodeId,
        // the id of the peer
        peer_id: NodeId,
        // shared data
        shared: Option<Arc<NodeShared>>,
        // handle to socket
        sock: ConnectionHandle,
        // a handle to the message channel of the corresponding client
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
}

pub enum SerializedSendTo<D: SharedData> {
    Me {
        // our id
        id: NodeId,
        // a handle to our client handle
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
    Peers {
        // the id of the peer
        id: NodeId,
        //Our own ID
        our_id: NodeId,
        // handle to socket
        sock: ConnectionHandle,
        // a handle to the message channel of the corresponding client
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
}

impl<D> SendTo<D>
    where
        D: SharedData + 'static,
        D::State: Send + Clone + 'static,
        D::Request: Send + 'static,
        D::Reply: Send + 'static,
{
    fn socket_type(&self) -> Option<&ConnectionHandle> {
        match self {
            SendTo::Me { .. } => {
                None
            }
            SendTo::Peers { sock, .. } => {
                Some(sock)
            }
        }
    }

    fn value_sync(self,
                  m: Either<(u64, Digest, Buf), (SystemMessage<D::State, D::Request, D::Reply>, u64, Digest, Buf)>,
                    rq_key: Option<u64>) {
        match self {
            SendTo::Me { my_id, shared: ref sh, tx } => {
                let key = sh.as_ref().map(|ref sh| &sh.my_key);

                if let Right((m, n, d, b)) = m {
                    Self::me_sync(my_id, m, n, d, b, key, tx);
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
            SendTo::Peers {
                flush, my_id, peer_id,
                shared: ref sh, sock, tx
            } => {
                match &sock {
                    ConnectionHandle::Sync(_) => {}
                    ConnectionHandle::Async(_) => {
                        panic!("Attempted to send messages synchronously through a async channel")
                    }
                }

                let key = sh.as_ref().map(|ref sh| &sh.my_key);
                if let Left((n, d, b)) = m {
                    Self::peers_sync(flush, my_id, peer_id, n, d, b, key, sock, tx, rq_key);
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
        }
    }

    async fn value(
        self,
        m: Either<(u64, Digest, Buf), (SystemMessage<D::State, D::Request, D::Reply>, u64, Digest, Buf)>,
    ) {
        match self {
            SendTo::Me { my_id, shared: ref sh, tx } => {
                let key = sh.as_ref().map(|ref sh| &sh.my_key);

                if let Right((m, n, d, b)) = m {
                    Self::me(my_id, m, n, d, b, key, tx).await;
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
            SendTo::Peers {
                flush, my_id, peer_id,
                shared: ref sh, sock, tx
            } => {
                match &sock {
                    ConnectionHandle::Sync(_) => {}
                    ConnectionHandle::Async(_) => {
                        panic!("Attempted to send messages synchronously through a async channel")
                    }
                }

                let key = sh.as_ref().map(|ref sh| &sh.my_key);
                if let Left((n, d, b)) = m {
                    Self::peers(flush, my_id, peer_id, n, d, b, key,
                                sock, tx).await;
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
        }
    }

    fn me_sync(my_id: NodeId,
               m: SystemMessage<D::State, D::Request, D::Reply>,
               n: u64,
               d: Digest,
               b: Buf,
               sk: Option<&KeyPair>,
               cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>) {

        // create wire msg
        let (h, _) = WireMessage::new(
            my_id,
            my_id,
            b,
            n,
            Some(d),
            sk,
        ).into_inner();

        // send
        cli.push_request_sync(Message::System(h, m));
    }

    async fn me(
        my_id: NodeId,
        m: SystemMessage<D::State, D::Request, D::Reply>,
        n: u64,
        d: Digest,
        b: Buf,
        sk: Option<&KeyPair>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {
        // create wire msg
        let (h, _) = WireMessage::new(
            my_id,
            my_id,
            b,
            n,
            Some(d),
            sk,
        ).into_inner();

        // send
        cli.push_request(Message::System(h, m)).await;
    }

    async fn peers(
        flush: bool,
        my_id: NodeId,
        peer_id: NodeId,
        n: u64,
        d: Digest,
        b: Buf,
        sk: Option<&KeyPair>,
        mut conn_handle: ConnectionHandle,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {

        //let print = format!("DONE SENDING MESSAGE {:?}", d);
        // create wire msg
        let wm = WireMessage::new(
            my_id,
            peer_id,
            b,
            n,
            Some(d),
            sk,
        );

        match conn_handle.async_send(wm).await {
            Ok(_) => {}
            Err(_) => { cli.disconnect(); }
        }
    }

    fn peers_sync(
        flush: bool,
        my_id: NodeId,
        peer_id: NodeId,
        n: u64,
        d: Digest,
        b: Buf,
        sk: Option<&KeyPair>,
        conn_handle: ConnectionHandle,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
        rq_key: Option<u64>) {

        // create wire msg
        let wm = WireMessage::new(
            my_id,
            peer_id,
            b,
            n,
            Some(d),
            sk,
        );

        match conn_handle.send(wm, rq_key) {
            Ok(_) => {}
            Err(_) => {
                cli.disconnect();
            }
        }
    }
}

impl<D> SerializedSendTo<D>
    where
        D: SharedData + 'static,
        D::State: Send + Clone + 'static,
        D::Request: Send + 'static,
        D::Reply: Send + 'static,
{
    fn socket_type(&self) -> Option<&ConnectionHandle> {
        match self {
            SerializedSendTo::Me { .. } => {
                None
            }
            SerializedSendTo::Peers { sock, .. } => {
                Some(sock)
            }
        }
    }

    fn value_sync(
        self,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
    ) {
        match self {
            SerializedSendTo::Me { tx, .. } => {
                Self::me_sync(h, m, tx);
            }
            SerializedSendTo::Peers { id, our_id, sock, tx } => {
                match &sock {
                    ConnectionHandle::Sync(_) => {}
                    ConnectionHandle::Async(_) => {
                        panic!("Attempted to send messages synchronously through a async channel")
                    }
                }

                Self::peers_sync(id, h, m, sock, tx);
            }
        }
    }

    async fn value(
        self,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
    ) {
        match self {
            SerializedSendTo::Me { tx, .. } => {
                //let msg = format!("{:?}", m.original());
                //let peer = format!("{:?}", tx.client_id());

                //debug!("{:?} // Sending SERIALIZED message {:?} to myself", peer,  msg);
                Self::me(h, m, tx).await;
            }
            SerializedSendTo::Peers { id, our_id, sock, tx } => {
                //let msg = format!("{:?}", m.original());
                //let peer = format!("{:?}", tx.client_id());

                //debug!("{:?} // Sending SERIALIZED message {} to other peer {:?} ",our_id,  msg, id);

                match &sock {
                    ConnectionHandle::Sync(_) => {
                        panic!("Attempted to send messages asynchronously through a sync channel")
                    }
                    ConnectionHandle::Async(_) => {}
                }

                Self::peers(id, h, m, sock, tx).await;
            }
        }
    }

    fn me_sync(
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>, ) {
        let (original, _) = m.into_inner();

        // send to ourselves
        cli.push_request_sync(Message::System(h, original));
    }

    async fn me(
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {
        let (original, _) = m.into_inner();

        // send to ourselves
        cli.push_request(Message::System(h, original)).await;
    }

    fn peers_sync(
        peer_id: NodeId,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
        conn_handle: ConnectionHandle,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {
        // create wire msg
        let (_, raw) = m.into_inner();
        let wm = WireMessage::from_parts(h, raw).unwrap();

        match conn_handle.send(wm, None) {
            Ok(_) => {}
            Err(_) => {
                cli.disconnect();
            }
        }
    }

    ///Asynchronous sending to peers
    ///Sends Client->Replica, Replica->Client
    async fn peers(
        peer_id: NodeId,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
        mut conn_handle: ConnectionHandle,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {

        // create wire msg
        let (_, raw) = m.into_inner();
        let wm = WireMessage::from_parts(h, raw).unwrap();

        match conn_handle.async_send(wm).await {
            Ok(_) => {}
            Err(_) => {
                cli.disconnect();
            }
        }
    }
}

///TODO: REMOVE THIS AFTER TESTING
#[inline]
fn get_request_key(session_id: crate::bft::ordering::SeqNo, operation_id: crate::bft::ordering::SeqNo) -> u64 {
    let sess: u64 = session_id.into();
    let opid: u64 = operation_id.into();
    sess | (opid << 32)
}