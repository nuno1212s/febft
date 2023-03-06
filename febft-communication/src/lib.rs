//! Communication primitives for `febft`, such as wire message formats.

use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use async_tls::{TlsAcceptor, TlsConnector};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use either::{Either, Left, Right};

use futures::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use futures_timer::Delay;
use intmap::IntMap;
use log::{debug, error, warn};

use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection, ServerName};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use febft_common::async_runtime as rt;
use febft_common::crypto::signature::{KeyPair, PublicKey};
use febft_common::prng::ThreadSafePrng;
use febft_common::socket::{AsyncListener, AsyncSocket, SecureSocketRecvAsync, SecureSocketRecvSync, SecureSocketSend, SecureSocketSendAsync, SecureSocketSendSync, SocketSendAsync, SocketSendSync, SyncListener, SyncSocket};
use febft_common::error::*;
use febft_common::{prng, socket, threadpool};
use febft_common::crypto::hash::Digest;
use febft_common::ordering::SeqNo;
use crate::benchmarks::CommStats;
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{Header, NetworkMessage, NetworkMessageContent, StoredSerializedNetworkMessage, WireMessage};
use crate::peer_sending_threads::ConnectionHandle;
use crate::ping_handler::PingHandler;
use crate::serialize::{Buf, Serializable};

pub mod message;
pub mod client_pooling;
pub mod peer_sending_threads;
pub mod serialize;
pub mod ping_handler;
pub mod benchmarks;
pub mod connection_util;

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
        into_iterator.into_iter().map(Self)
    }

    pub fn targets<I>(into_iterator: I) -> impl Iterator<Item=Self>
        where
            I: IntoIterator<Item=usize>,
    {
        into_iterator.into_iter().map(NodeId::from)
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
pub enum PeerTx {
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
        connected: Arc<DashMap<u64, ConnectionHandle>>,
    },
    // replicas don't need shared access to the hashmap, so
    // we only need one lock (to restrict I/O to one producer at a time)
    Server {
        first_cli: NodeId,
        connected: Arc<DashMap<u64, ConnectionHandle>>,
    },
}

impl PeerTx {
    ///Add a tx peer connection to the registry
    ///Requires knowing the first_cli
    pub fn add_peer(&self, client_id: u64, socket: ConnectionHandle) {
        let previous_conn = match self {
            PeerTx::Client { connected, .. } => connected.insert(client_id, socket),
            PeerTx::Server { connected, .. } => connected.insert(client_id, socket),
        };

        if let Some(existing_conn) = previous_conn {
            existing_conn.close();
        };
    }

    pub fn connected(&self) -> usize {
        match self {
            //+ 1 as we don't store the loopback connection here, it's in the replica handling.
            PeerTx::Client { connected, .. } => connected.len() + 1,
            PeerTx::Server { connected, .. } => connected.len() + 1
        }
    }

    pub fn find_peer(&self, client_id: u64) -> Option<ConnectionHandle> {
        match self {
            PeerTx::Client { connected, .. } => {
                let option = {
                    connected
                        .get(&client_id)
                        .map(|reference| (reference.value()).clone())
                };

                option
            }
            PeerTx::Server { connected, .. } => {
                let option = {
                    connected
                        .get(&client_id)
                        .map(|reference| (reference.value()).clone())
                };

                option
            }
        }
    }

    pub fn disconnect_peer(&self, peer_id: u64) -> Option<ConnectionHandle> {
        match self {
            PeerTx::Client { connected, .. } => {
                connected.remove(&peer_id).map(|(_u, v)| {
                    v
                })
            }
            PeerTx::Server { connected, .. } => {
                connected.remove(&peer_id).map(|(_u, v)| {
                    v
                })
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

/// The connection type used for connections
/// Stores the connector needed
#[derive(Clone)]
pub enum TlsNodeConnector {
    Async(TlsConnector),
    Sync(Arc<ClientConfig>),
}

/// Establish safe tls node connections
#[derive(Clone)]
pub enum TlsNodeAcceptor {
    Async(TlsAcceptor),
    Sync(Arc<ServerConfig>),
}

/// Accept node connections
pub enum NodeConnectionAcceptor {
    Async(AsyncListener),
    Sync(SyncListener),
}

/// Container for handles to other processes in the system.
///
/// A `Node` constitutes the core component used in the wire
/// communication between processes.
pub struct Node<T: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    //Handles the incomming connections' buffering and request collection
    //This is polled by the proposer for client requests and by the
    client_pooling: PeerIncomingRqHandling<NetworkMessage<T>>,
    //Handles the outgoing connection references
    peer_tx: PeerTx,
    //Handles the Pseudo random number generation for this node
    rng: ThreadSafePrng,
    //Some shared node information
    shared: Arc<NodeShared>,
    //A set of the nodes we are currently attempting to connect to
    currently_connecting: Mutex<BTreeSet<NodeId>>,
    //The connector used to establish secure connections between peers
    connector: TlsNodeConnector,
    //Handles pings and timeouts of said pings
    ping_handler: Arc<PingHandler>,
    //An address map of all the peers
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

    pub fn new_replica(
        client_addr: (SocketAddr, String),
        replica_addr: (SocketAddr, String),
    ) -> Self {
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
    pub sync_server_config: ServerConfig,
    ///The TLS configuration used to connect to replica nodes (from replica nodes) (Synchronousy)
    pub sync_client_config: ClientConfig,
    ///How many clients should be placed in a single collecting pool (seen in incoming_peer_handling)
    pub clients_per_pool: usize,
    ///The timeout for batch collection in each client pool.
    /// (The first to reach between batch size and timeout)
    pub batch_timeout_micros: u64,
    ///How long should a client pool sleep for before attempting to collect requests again
    /// (It actually will sleep between 3/4 and 5/4 of this value, to make sure they don't all sleep / wake up at the same time)
    pub batch_sleep_micros: u64,
    ///Statistics for communications
    pub comm_stats: Option<Arc<CommStats>>,
    ///Path for the db file
    pub db_path: String,
}

// max no. of messages allowed in the channel
const NODE_CHAN_BOUND: usize = 50000;

// max no. of SendTo's to inline before doing a heap alloc
const NODE_VIEWSIZ: usize = 16;

type SendTos<D> = SmallVec<[SendTo<D>; NODE_VIEWSIZ]>;

type SerializedSendTos<D> = SmallVec<[SerializedSendTo<D>; NODE_VIEWSIZ]>;

impl<T> Node<T>
    where T: Serializable + 'static
{
    fn setup_connector(
        _sync_connector: Arc<ClientConfig>,
        _async_connector: TlsConnector,
    ) -> TlsNodeConnector {
        //TODO: Support Sync connectors as well
        TlsNodeConnector::Async(_async_connector)
    }

    fn setup_acceptor(
        _sync_acceptor: Arc<ServerConfig>,
        _async_acceptor: TlsAcceptor,
    ) -> TlsNodeAcceptor {
        TlsNodeAcceptor::Async(_async_acceptor)
    }

    async fn setup_socket(
        id: &NodeId,
        server_addr: &SocketAddr,
    ) -> Result<NodeConnectionAcceptor> {
        let mut server_addr = server_addr.clone();

        server_addr.set_ip(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)));
        //TODO: Maybe add support for asynchronous listeners?
        debug!("{:?} // Binding to address (clients) {:?}", id, server_addr);

        //
        // let socket = socket::bind_sync_server(server_addr).wrapped_msg(
        //     ErrorKind::Communication,
        //     format!("Failed to bind to address {:?}", server_addr).as_str(),
        // )?;

        let socket = socket::bind_async_server(server_addr).await.wrapped_msg(
            ErrorKind::Communication,
            format!("Failed to bind to address {:?}", server_addr).as_str(),
        )?;

        Ok(NodeConnectionAcceptor::Async(socket))
    }

    async fn setup_client_facing_socket(
        id: NodeId,
        cfg: &NodeConfig,
    ) -> Result<NodeConnectionAcceptor> {
        debug!("{:?} // Attempt to setup client facing socket.", id);

        let peer_addr = cfg.addrs.get(id.into()).ok_or(Error::simple_with_msg(
            ErrorKind::Communication,
            "Failed to get client facing IP",
        ))?;

        let server_addr = &peer_addr.client_addr;

        Self::setup_socket(&id, &server_addr.0).await
    }

    async fn setup_replica_facing_socket(
        id: NodeId,
        cfg: &NodeConfig,
    ) -> Result<Option<NodeConnectionAcceptor>> {
        let peer_addr = cfg.addrs.get(id.into()).ok_or(Error::simple_with_msg(
            ErrorKind::Communication,
            "Failed to get replica facing IP",
        ))?;

        //Initialize the replica<->replica facing server
        let replica_listener = if id >= cfg.first_cli {
            //Clients don't have a replica<->replica facing server
            None
        } else {
            let replica_facing_addr =
                peer_addr
                    .replica_addr
                    .as_ref()
                    .ok_or(Error::simple_with_msg(
                        ErrorKind::Communication,
                        "Failed to get replica facing IP",
                    ))?;

            Some(Self::setup_socket(&id, &replica_facing_addr.0).await?)
        };

        Ok(replica_listener)
    }

    ///Sets up the thread or task (depending on runtime) to receive new connection attempts
    fn setup_socket_connection_workers_socket(
        self: Arc<Self>,
        node_connector: TlsNodeAcceptor,
        listener: NodeConnectionAcceptor,
    ) {
        match (node_connector, listener) {
            (TlsNodeAcceptor::Sync(cfg), NodeConnectionAcceptor::Sync(sync_listener)) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let sync_acceptor = cfg.clone();

                std::thread::Builder::new()
                    .name(format!("{:?} connection acceptor", self.id()))
                    .spawn(move || {
                        self.rx_side_accept_sync(first_cli, my_id, sync_listener, sync_acceptor);
                    })
                    .expect("Failed to start replica's connection acceptor");
            }
            (TlsNodeAcceptor::Async(cfg), NodeConnectionAcceptor::Async(async_listener)) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let async_acceptor = cfg.clone();

                rt::spawn(self.rx_side_accept(first_cli, my_id, async_listener, async_acceptor));
            }
            (_, _) => {
                unreachable!()
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
    ) -> Result<(Arc<Self>, Vec<NetworkMessage<T>>)> {
        let id = cfg.id;

        // initial checks of correctness
        if cfg.n < (3 * cfg.f + 1) {
            return Err("Invalid number of replicas").wrapped(ErrorKind::Communication);
        }

        if id >= NodeId::from(cfg.n) && id < cfg.first_cli {
            return Err("Invalid node ID").wrapped(ErrorKind::Communication);
        }

        debug!("Initializing sockets.");

        //Initialize the client facing server
        let client_listener = Self::setup_client_facing_socket(id, &cfg).await?;

        let replica_listener = Self::setup_replica_facing_socket(id, &cfg).await?;

        debug!("Initializing TLS configurations.");

        let async_acceptor: TlsAcceptor = cfg.async_server_config.into();
        let async_connector: TlsConnector = cfg.async_client_config.into();

        let sync_acceptor = Arc::new(cfg.sync_server_config);
        let sync_connector = Arc::new(cfg.sync_client_config);

        // node def
        let peer_tx = if id >= cfg.first_cli {
            PeerTx::Client {
                first_cli: cfg.first_cli,
                connected: Arc::new(DashMap::new()),
            }
        } else {
            PeerTx::Server {
                first_cli: cfg.first_cli,
                connected: Arc::new(DashMap::new()),
            }
        };

        debug!("Initializing client facing connections");

        let connector = Self::setup_connector(sync_connector, async_connector);

        let acceptor = Self::setup_acceptor(sync_acceptor, async_acceptor);

        let shared = Arc::new(NodeShared {
            my_key: cfg.sk,
            peer_keys: cfg.pk,
        });

        debug!("Initializing node peer handling.");

        let ping_handler = PingHandler::new();

        //Setup all the peer message reception handling.
        let peers = PeerIncomingRqHandling::new(
            cfg.id,
            cfg.first_cli,
            cfg.batch_size,
            cfg.clients_per_pool,
            cfg.batch_timeout_micros,
            cfg.batch_sleep_micros,
        );

        let rng = ThreadSafePrng::new();

        //TESTING
        let sent_rqs = if id > cfg.first_cli {
            None

            /*Some(Arc::new(
                std::iter::repeat_with(|| DashMap::with_capacity(20000))
                    .take(30)
                    .collect(),
            ))*/
        } else {
            None
        };

        let rcv_rqs =
            if id < cfg.first_cli {
                None

                //
                // //We want the replicas to log recved requests
                // let arc = Arc::new(RwLock::new(
                //     std::iter::repeat_with(|| { DashMap::with_capacity(20000) })
                //         .take(30)
                //         .collect()));
                //
                // let rqs = arc.clone();
                //
                // std::thread::Builder::new().name(String::from("Logging thread")).spawn(move || {
                //     loop {
                //         let new_vec: Vec<DashMap<u64, ()>> = std::iter::repeat_with(|| { DashMap::with_capacity(20000) })
                //             .take(30)
                //             .collect();
                //
                //         let mut old_vec = {
                //             let mut write_guard = rqs.write();
                //
                //             std::mem::replace(&mut *write_guard, new_vec)
                //         };
                //
                //         let mut print = String::new();
                //
                //         for bucket in old_vec {
                //             for (key, _) in bucket.into_iter() {
                //                 print = print + " , " + &*format!("{}", key);
                //             }
                //         }
                //
                //         println!("{}", print);
                //
                //         std::thread::sleep(Duration::from_secs(1));
                //     }
                // }).expect("Failed to start logging thread");
                //
                // Some(arc)
            } else { None };

        debug!("Initializing node reference");

        let node = Arc::new(Node {
            id,
            rng,
            shared,
            peer_tx,
            client_pooling: peers,
            connector,
            peer_addrs: cfg.addrs,
            first_cli: cfg.first_cli,
            comm_stats: cfg.comm_stats,
            sent_rqs,
            ping_handler,
            recv_rqs: rcv_rqs,
            currently_connecting: Mutex::new(BTreeSet::new()),
        });

        let rx_node_clone = node.clone();

        debug!("Initializing replica connection workers");

        //Setup the worker threads for receiving new connections before starting to connect to
        //other peers
        if let Some(replica_listener) = replica_listener {
            rx_node_clone
                .clone()
                .setup_socket_connection_workers_socket(
                    acceptor.clone(),
                    replica_listener,
                );
        }

        //Setup client listener
        {
            rx_node_clone
                .clone()
                .setup_socket_connection_workers_socket(
                    acceptor,
                    client_listener,
                );
        }

        let n = cfg.n;

        debug!("Connect to other replicas");

        // tx side (connect to replica)
        if id < cfg.first_cli {
            //We are a replica, connect to all other replicas

            match node.connector.clone() {
                TlsNodeConnector::Async(async_connector) => {
                    let node = node.clone();

                    node.tx_side_connect_async(n as u32, async_connector).await;
                }
                TlsNodeConnector::Sync(sync_connector) => {
                    let node = node.clone();

                    node.tx_side_connect_sync(n as u32, sync_connector);
                }
            }
        } else {
            //Connect to all replicas as a client
            match node.connector.clone() {
                TlsNodeConnector::Async(async_connector) => {
                    let node = node.clone();

                    node.tx_side_connect_async(n as u32, async_connector).await;
                }
                TlsNodeConnector::Sync(sync_connector) => {
                    let node = node.clone();
                    threadpool::execute(move || {
                        node.tx_side_connect_sync(n as u32, sync_connector);
                    });
                }
            }
        }

        let rogue = Vec::new();

        //TODO: We need to receive the amount of replicas in the quorum
        while node.client_pooling.replica_count() < cfg.n || node.peer_tx.connected() < cfg.n {
            //Any received messages will be handled by the connection pool buffers
            debug!(
                "{:?} // Connected to {} RX replicas, {} TX",
                node.id,
                node.client_pooling.replica_count(),
                node.peer_tx.connected()
            );

            Delay::new(Duration::from_millis(500)).await;
        }

        debug!(
            "Found all nodes required {}, {} RX, {} TX",
            node.client_pooling.replica_count(),
            node.client_pooling.replica_count(),
            node.peer_tx.connected()
        );

        // success
        Ok((node, rogue))
    }

    pub fn batch_size(&self) -> usize {
        self.client_pooling.batch_size()
    }

    fn resolve_client_rx_connection(
        &self,
        node_id: NodeId,
    ) -> Option<Arc<ConnectedPeer<NetworkMessage<T>>>> {
        self.client_pooling.resolve_peer_conn(node_id)
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

    /// Get my own key pair
    pub fn get_key_pair(&self) -> &KeyPair {
        &self.shared.my_key
    }

    /// Reports the id of this `Node`.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Reports the id of the first client.
    pub fn first_client_id(&self) -> NodeId {
        self.first_cli
    }

    pub fn connector(&self) -> &TlsNodeConnector {
        &self.connector
    }

    /// Returns a `SendNode` sharing the same handles as this `Node`.
    pub fn send_node(self: &Arc<Self>) -> SendNode<T> {
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
    pub fn loopback_channel(&self) -> &Arc<ConnectedPeer<NetworkMessage<T>>> {
        self.client_pooling.loopback_connection()
    }

    /// Send a `SystemMessage` to a single destination.
    ///
    /// This method is somewhat more efficient than calling `broadcast()`
    /// on a single target id.
    pub fn send(
        &self,
        message: NetworkMessageContent<T>,
        target: NodeId,
        flush: bool,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        match self.resolve_client_rx_connection(target) {
            None => {
                error!(
                    "Failed to send message to client {:?} as the connection to it was not found!",
                    target
                );
            }
            Some(conn) => {
                let send_to = match Self::send_to(flush, self.id, target, None, conn, &self.peer_tx)
                {
                    Ok(send_to) => send_to,
                    Err(err) => {
                        error!("{:?} // {:?}", self.id, err);
                        return;
                    }
                };

                let my_id = self.id;
                let nonce = self.rng.next_state();

                Self::send_impl(
                    message,
                    send_to,
                    my_id,
                    target,
                    self.first_cli,
                    nonce,
                    comm_stats,
                );
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
        message: NetworkMessageContent<T>,
        target: NodeId,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        match self.resolve_client_rx_connection(target) {
            None => {
                error!(
                    "Failed to send message to client {:?} as the connection to it was not found!",
                    target
                );
            }
            Some(conn) => {
                let send_to = match Self::send_to(
                    true,
                    self.id,
                    target,
                    Some(&self.shared),
                    conn,
                    &self.peer_tx,
                ) {
                    Ok(send_to) => send_to,
                    Err(error) => {
                        error!("{:?} // {:?}", self.id, error);
                        return;
                    }
                };

                let my_id = self.id;

                let nonce = self.rng.next_state();

                Self::send_impl(
                    message,
                    send_to,
                    my_id,
                    target,
                    self.first_cli,
                    nonce,
                    comm_stats,
                );
            }
        };
    }

    #[inline]
    fn send_impl(
        message: NetworkMessageContent<T>,
        send_to: SendTo<T>,
        my_id: NodeId,
        target: NodeId,
        _first_cli: NodeId,
        nonce: u64,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            // serialize
            start_measurement!(start_serialization);

            let mut buf = Vec::new();

            let digest = serialize::serialize_digest_message(&message, &mut buf).unwrap();

            let buf = Bytes::from(buf);

            message_digest_time!(&comm_stats, start_serialization);

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            start_measurement!(before_send_time);

            // send
            if my_id == target {
                // Right -> our turn

                //Send to myself, always synchronous since only replicas send to themselves
                send_to.value(Right(((nonce, digest, buf), message)), None);

                message_sent_own!(&comm_stats, before_send_time, my_id);
            } else {

                // Left -> peer turn
                send_to.value(Left((nonce, digest, buf)), None);

                message_dispatched!(&comm_stats, before_send_time, target);
            }
        });
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    ///
    /// This variant of broadcast does not sign the messages that are sent
    pub fn broadcast(
        &self,
        message: NetworkMessageContent<T>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        let (mine, others) = self.send_tos(self.id, &self.peer_tx, None, targets);

        let nonce = self.rng.next_state();

        Self::broadcast_impl(message, mine, others, self.first_cli, nonce, comm_stats);
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    ///
    /// This variant of `broadcast()` signs the sent message.
    pub fn broadcast_signed(
        &self,
        message: NetworkMessageContent<T>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        start_measurement!(start_time);

        let (mine, others) = self.send_tos(self.id, &self.peer_tx, Some(&self.shared), targets);

        let nonce = self.rng.next_state();

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        Self::broadcast_impl(message, mine, others, self.first_cli, nonce, comm_stats);
    }

    pub fn broadcast_serialized(&self, messages: IntMap<StoredSerializedNetworkMessage<T>>) {
        let start_time = Instant::now();
        let headers = messages.values().map(|stored| stored.original().header());

        let (mine, others) = self.serialized_send_tos(self.id, &self.peer_tx, headers);

        let comm_stats = if let Some(comm_stats) = &self.comm_stats {
            Some((comm_stats.clone(), start_time))
        } else {
            None
        };

        Self::broadcast_serialized_impl(messages, mine, others, self.first_client_id(), comm_stats);
    }

    #[inline]
    fn broadcast_serialized_impl(
        mut messages: IntMap<StoredSerializedNetworkMessage<T>>,
        my_send_to: Option<SerializedSendTo<T>>,
        other_send_tos: SerializedSendTos<T>,
        _first_client: NodeId,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            // send to ourselves
            if let Some(send_to) = my_send_to {
                let id = match &send_to {
                    SerializedSendTo::Me { id, .. } => *id,
                    _ => unreachable!(),
                };

                let stored = messages
                    .remove(id.into())
                    .unwrap();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                start_measurement!(before_send_time);

                send_to.value(stored);

                message_sent_own!(&comm_stats, before_send_time, id);
            }

            // send to others
            for send_to in other_send_tos {
                let id = match &send_to {
                    SerializedSendTo::Peers { id, .. } => *id,
                    _ => unreachable!(),
                };

                let stored = messages
                    .remove(id.into())
                    .unwrap();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                start_measurement!(before_send_time);

                send_to.value(stored);

                message_dispatched!(&comm_stats, before_send_time, id);
            }
        });
    }

    #[inline]
    fn broadcast_impl(
        message: NetworkMessageContent<T>,
        my_send_to: Option<SendTo<T>>,
        other_send_tos: SendTos<T>,
        _first_cli: NodeId,
        nonce: u64,
        comm_stats: Option<(Arc<CommStats>, Instant)>,
    ) {
        threadpool::execute(move || {
            start_measurement!(start_serialization);

            // serialize
            let mut buf = Vec::new();

            let digest = match serialize::serialize_digest_message::<T, Vec<u8>>(&message, &mut buf) {
                Ok(dig) => dig,
                Err(err) => {
                    error!("Failed to serialize message {:?}. Message is {:?}", err, message);

                    panic!("Failed to serialize message {:?}", err);
                }
            };

            let buf = Bytes::from(buf);

            message_digest_time!(&comm_stats, start_serialization);

            let rq_key = None;

            // send to ourselves
            if let Some(send_to) = my_send_to {
                let id = match &send_to {
                    SendTo::Me { my_id, .. } => *my_id,
                    _ => unreachable!(),
                };

                let buf = buf.clone();

                let comm_stats = comm_stats.clone();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                start_measurement!(before_send_time);

                // Right -> our turn
                send_to.value(Right(((nonce, digest, buf), message)), rq_key.clone());

                message_sent_own!(&comm_stats, before_send_time, id);
            }

            // send to others
            for send_to in other_send_tos {
                let id = match &send_to {
                    SendTo::Peers { peer_id, .. } => *peer_id,
                    _ => unreachable!(),
                };

                let buf = buf.clone();

                let comm_stats = comm_stats.clone();

                //Measuring time taken to get to the point of sending the message
                //We don't actually want to measure how long it takes to send the message
                start_measurement!(before_send_time);

                send_to.value(Left((nonce, digest, buf)), rq_key.clone());

                message_dispatched!(&comm_stats, before_send_time, id);
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
    ) -> (Option<SendTo<T>>, SendTos<T>) {
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
    ) -> (Option<SerializedSendTo<T>>, SerializedSendTos<T>) {
        let mut my_send_to = None;
        let mut other_send_tos = SerializedSendTos::new();

        self.create_serialized_send_tos(
            my_id,
            peer_tx,
            headers,
            &mut my_send_to,
            &mut other_send_tos,
        );

        (my_send_to, other_send_tos)
    }

    #[inline]
    fn create_serialized_send_tos<'a>(
        &self,
        my_id: NodeId,
        peer_tx: &PeerTx,
        headers: impl Iterator<Item=&'a Header>,
        mine: &mut Option<SerializedSendTo<T>>,
        others: &mut SerializedSendTos<T>,
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
                        error!(
                            "Cound not find socket but found rx, closing it {:?}",
                            id.id()
                        );

                        tx.disconnect();

                        continue;
                    }
                    (Some(_sock), None) => {
                        error!("Found socket but didn't find rx? Closing {:?}", id.id());

                        continue;
                    }
                    (Some(socket), Some(tx)) => (socket, tx),
                };

                //Get the RX channel for the corresponding peer to mark as disconnected if the sending fails

                let s = SerializedSendTo::Peers {
                    id,
                    our_id: my_id,
                    peer_tx: peer_tx.clone(),
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
        mine: &mut Option<SendTo<T>>,
        others: &mut SendTos<T>,
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
                        error!(
                            "Cound not find socket but found rx, closing it {:?}",
                            id.id()
                        );

                        tx.disconnect();

                        continue;
                    }
                    (Some(sock), None) => {
                        error!("Found socket but didn't find rx? Closing {:?}", id.id());

                        sock.close();

                        tx_peers.disconnect_peer(id.into());

                        continue;
                    }
                    (Some(socket), Some(tx)) => (socket, tx),
                };

                let s = SendTo::Peers {
                    sock,
                    my_id,
                    peer_id: id,
                    flush: true,
                    //Get the RX channel for the peer to mark as DCed if it fails
                    tx: rx_conn,
                    shared: shared.map(|sh| Arc::clone(sh)),
                    peer_tx: tx_peers.clone(),
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
        cli: Arc<ConnectedPeer<NetworkMessage<T>>>,
        peer_tx: &PeerTx,
    ) -> Result<SendTo<T>> {
        let shared = shared.map(|sh| Arc::clone(sh));
        if my_id == peer_id {
            Ok(SendTo::Me {
                shared,
                my_id,
                tx: cli,
            })
        } else {
            let sock = match peer_tx.find_peer(peer_id.id() as u64) {
                None => {
                    return Err(Error::simple_with_msg(
                        ErrorKind::Communication,
                        "Failed to find peer id",
                    ));
                }
                Some(sock) => sock,
            }
                .clone();

            Ok(SendTo::Peers {
                flush,
                sock,
                shared,
                peer_id,
                my_id,
                tx: cli,
                peer_tx: peer_tx.clone(),
            })
        }
    }

    /// Check if we are currently connected to a provided node (in terms of message reception)
    pub fn is_connected_to_rx(&self, id: NodeId) -> bool {
        self.client_pooling.resolve_peer_conn(id).is_some()
    }

    /// Are we connected to a node (in terms of message sending)
    pub fn is_connected_to_tx(&self, id: NodeId) -> bool {
        self.peer_tx.find_peer(id.0 as u64).is_some()
    }

    //Get how many pending messages are in the requests channel
    pub fn rqs_len_from_clients(&self) -> usize {
        self.client_pooling.rqs_len_from_clients()
    }

    //Receive messages from the clients we are connected to
    pub fn receive_from_clients(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Vec<NetworkMessage<T>>> {
        self.client_pooling.receive_from_clients(timeout)
    }

    pub fn try_recv_from_clients(
        &self,
    ) -> Result<Option<Vec<NetworkMessage<T>>>> {
        self.client_pooling.try_receive_from_clients()
    }

    //Receive messages from the replicas we are connected to
    pub fn receive_from_replicas(&self) -> Result<NetworkMessage<T>> {
        self.client_pooling.receive_from_replicas()
    }

    /// Registers the newly created transmission socket to the peer
    fn handle_connected_tx(self: &Arc<Self>, peer_id: NodeId, sock: SecureSocketSend) {
        debug!("{:?} // Connected TX to peer {:?}", self.id, peer_id);

        let conn_handle = match sock {
            SecureSocketSend::Async(socket) => {
                peer_sending_threads::initialize_async_sending_task_for(
                    Arc::clone(self),
                    peer_id.clone(),
                    socket,
                    self.comm_stats.clone(),
                )
            }
            SecureSocketSend::Sync(socket) => {
                peer_sending_threads::initialize_sync_sending_thread_for(
                    Arc::clone(self),
                    peer_id.clone(),
                    socket,
                    self.comm_stats.clone(),
                    self.sent_rqs.clone(),
                )
            }
        };

        self.peer_tx.add_peer(peer_id.id() as u64, conn_handle);
    }

    ///Connect to all other replicas in the cluster, utilizing tokio (utilizing regular
    /// synchronous APIs)
    #[inline]
    pub async fn tx_side_connect_async(self: Arc<Self>, n: u32, connector: TlsConnector) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != self.id()) {
            let clone = self.clone();
            let _connector = connector.clone();

            Self::tx_connect_node_async(clone, peer_id, None);
        }
    }

    ///Connect to all other replicas in the cluster, but without using tokio (utilizing regular
    /// synchronous APIs)
    #[inline]
    //#[instrument(skip(self, n, connector))]
    pub fn tx_side_connect_sync(self: Arc<Self>, n: u32, connector: Arc<ClientConfig>) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != self.id()) {
            let clone = self.clone();
            let _connector = connector.clone();

            Self::tx_connect_node_sync(clone, peer_id, None);
        }
    }

    /// Check if we are already attempting to connect to a given node.
    fn is_currently_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let guard = self.currently_connecting.lock().unwrap();

        guard.contains(&peer_id)
    }

    fn register_currently_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let mut guard = self.currently_connecting.lock().unwrap();

        guard.insert(peer_id)
    }

    fn unregister_currently_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let mut guard = self.currently_connecting.lock().unwrap();

        guard.remove(&peer_id)
    }
}

/// Represents a node with sending capabilities only.
pub struct SendNode<T: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    shared: Arc<NodeShared>,
    rng: prng::State,
    peer_tx: PeerTx,
    parent_node: Arc<Node<T>>,
    channel: Arc<ConnectedPeer<NetworkMessage<T>>>,
    comm_stats: Option<Arc<CommStats>>,
}

impl<T: Serializable> Clone for SendNode<T> {
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

impl<T> SendNode<T>
    where
        T: Serializable
{
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn loopback_channel(&self) -> &Arc<ConnectedPeer<NetworkMessage<T>>> {
        &self.channel
    }

    pub fn parent_node(&self) -> &Arc<Node<T>> {
        &self.parent_node
    }

    /// Check the `master_channel()` documentation for `Node`.

    /// Check the `send()` documentation for `Node`.
    pub fn send(
        &mut self,
        message: NetworkMessageContent<T>,
        target: NodeId,
        flush: bool,
    ) {
        let start_time = Instant::now();

        match self.parent_node.resolve_client_rx_connection(target) {
            None => {
                error!(
                    "Failed to send message to client {:?} as the connection to it was not found!",
                    target
                );
            }
            Some(conn) => {
                let send_to =
                    match <Node<T>>::send_to(flush, self.id, target, None, conn, &self.peer_tx) {
                        Ok(send_to) => send_to,
                        Err(error) => {
                            error!("{:?} // {:?}", self.id, error);

                            return;
                        }
                    };

                let my_id = self.id;
                let nonce = self.rng.next_state();

                let comm_stats = if let Some(comm_stats) = &self.comm_stats {
                    Some((comm_stats.clone(), start_time))
                } else {
                    None
                };

                <Node<T>>::send_impl(
                    message,
                    send_to,
                    my_id,
                    target,
                    self.first_cli,
                    nonce,
                    comm_stats,
                );
            }
        }
    }

    /// Check the `send_signed()` documentation for `Node`.
    pub fn send_signed(
        &mut self,
        message: NetworkMessageContent<T>,
        target: NodeId,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        match self.parent_node.resolve_client_rx_connection(target) {
            None => {
                error!(
                    "Failed to send message to client {:?} as the connection to it was not found!",
                    target
                );
            }
            Some(conn) => {
                let send_to = match <Node<T>>::send_to(
                    true,
                    self.id,
                    target,
                    Some(&self.shared),
                    conn,
                    &self.peer_tx,
                ) {
                    Ok(send_to) => send_to,
                    Err(err) => {
                        error!("{:?} // {:?}", self.id, err);

                        return;
                    }
                };

                let my_id = self.id;
                let nonce = self.rng.next_state();

                <Node<T>>::send_impl(
                    message,
                    send_to,
                    my_id,
                    target,
                    self.first_cli,
                    nonce,
                    comm_stats,
                );
            }
        }
    }

    /// Check the `broadcast()` documentation for `Node`.
    pub fn broadcast(
        &mut self,
        message: NetworkMessageContent<T>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        let (mine, others) = self
            .parent_node
            .send_tos(self.id, &self.peer_tx, None, targets);

        let nonce = self.rng.next_state();

        <Node<T>>::broadcast_impl(message, mine, others, self.first_cli, nonce, comm_stats);
    }

    /// Check the `broadcast_signed()` documentation for `Node`.
    pub fn broadcast_signed(
        &mut self,
        message: NetworkMessageContent<T>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let comm_stats = received_network_rq!(&self.comm_stats);

        let (mine, others) =
            self.parent_node
                .send_tos(self.id, &self.peer_tx, Some(&self.shared), targets);

        let nonce = self.rng.next_state();

        <Node<T>>::broadcast_impl(message, mine, others, self.first_cli, nonce, comm_stats);
    }
}

// helper type used when either a `send()` or a `broadcast()`
// is called by a `Node` or `SendNode`.
//
// holds some data that can be shared between threads, relevant
// to a network write operation, or channel write operation,
// depending on whether we're sending a message to a peer node
// or ourselves
pub enum SendTo<T: Serializable + 'static> {
    Me {
        // our id
        my_id: NodeId,
        // shared data
        shared: Option<Arc<NodeShared>>,
        // a handle to our client handle
        tx: Arc<ConnectedPeer<NetworkMessage<T>>>,
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
        // handle to the registry of connected tx peers
        peer_tx: PeerTx,
        // handle to socket
        sock: ConnectionHandle,
        // a handle to the message channel of the corresponding client
        tx: Arc<ConnectedPeer<NetworkMessage<T>>>,
    },
}

pub enum SerializedSendTo<T: Serializable> {
    Me {
        // our id
        id: NodeId,
        // a handle to our client handle
        tx: Arc<ConnectedPeer<NetworkMessage<T>>>,
    },
    Peers {
        // the id of the peer
        id: NodeId,
        //Our own ID
        our_id: NodeId,
        // handle to the registry of connected tx peers
        peer_tx: PeerTx,
        // handle to socket
        sock: ConnectionHandle,
        // a handle to the message channel of the corresponding client
        tx: Arc<ConnectedPeer<NetworkMessage<T>>>,
    },
}

type MessageData = (u64, Digest, Buf);

impl<T> SendTo<T>
    where
        T: Serializable
{
    fn socket_type(&self) -> Option<&ConnectionHandle> {
        match self {
            SendTo::Me { .. } => None,
            SendTo::Peers { sock, .. } => Some(sock),
        }
    }

    fn value(
        self,
        m: Either<MessageData, (MessageData, NetworkMessageContent<T>)>,
        rq_key: Option<u64>,
    ) {
        match self {
            SendTo::Me {
                my_id,
                shared: ref sh,
                tx,
            } => {
                if let Right((mdata, ms)) = m {
                    let key = sh.as_ref().map(|ref sh| &sh.my_key);

                    let (h, _) = WireMessage::new(my_id, my_id, mdata.2, mdata.0, Some(mdata.1), key).into_inner();

                    Self::me(my_id, NetworkMessage::new(h, ms), tx);
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
            SendTo::Peers {
                flush,
                my_id,
                peer_id,
                shared: ref sh,
                sock,
                peer_tx,
                tx: _,
            } => {
                if let Left((nonce, digest, data)) = m {
                    let key = sh.as_ref().map(|ref sh| &sh.my_key);

                    let wm = WireMessage::new(my_id, my_id, data, nonce, Some(digest), key);
                    Self::peers(flush, my_id, peer_id, wm, &peer_tx, sock, rq_key);
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
        }
    }

    fn me(
        my_id: NodeId,
        msg: NetworkMessage<T>,
        cli: Arc<ConnectedPeer<NetworkMessage<T>>>,
    ) {

        // send
        if let Err(inner) = cli.push_request(msg) {
            error!("{:?} // Failed to push to myself! {:?}", my_id, inner);
        };
    }

    fn peers(
        _flush: bool,
        my_id: NodeId,
        peer_id: NodeId,
        wm: WireMessage,
        peer_tx: &PeerTx,
        conn_handle: ConnectionHandle,
        rq_key: Option<u64>,
    ) {
        match conn_handle.send(wm, rq_key) {
            Ok(_) => {}
            Err(_) => {
                conn_handle.close();
                peer_tx.disconnect_peer(peer_id.into());
            }
        }
    }
}

impl<T> SerializedSendTo<T>
    where
        T: Serializable
{
    fn socket_type(&self) -> Option<&ConnectionHandle> {
        match self {
            SerializedSendTo::Me { .. } => None,
            SerializedSendTo::Peers { sock, .. } => Some(sock),
        }
    }

    fn value(
        self,
        m: StoredSerializedNetworkMessage<T>,
    ) {
        match self {
            SerializedSendTo::Me { tx, .. } => {
                Self::me(m, tx);
            }
            SerializedSendTo::Peers {
                id,
                our_id: _,
                sock,
                peer_tx,
                tx,
            } => {
                Self::peers(id, m, &peer_tx, sock, tx);
            }
        }
    }

    fn me(
        m: StoredSerializedNetworkMessage<T>,
        cli: Arc<ConnectedPeer<NetworkMessage<T>>>,
    ) {
        let (original, _) = m.into_inner();

        let myself = original.header().from();

        // send to ourselves
        if let Err(err) = cli.push_request(original) {
            error!("{:?} // FAILED TO SEND TO MYSELF {:?}", myself, err);
        }
    }

    fn peers(
        peer_id: NodeId,
        m: StoredSerializedNetworkMessage<T>,
        peer_tx: &PeerTx,
        conn_handle: ConnectionHandle,
        _cli: Arc<ConnectedPeer<NetworkMessage<T>>>,
    ) {
        // create wire msg
        let (msg, raw) = m.into_inner();

        let wm = WireMessage::from_parts(msg.into_inner().0, Bytes::from(raw)).unwrap();

        match conn_handle.send(wm, None) {
            Ok(_) => {}
            Err(_) => {
                conn_handle.close();

                peer_tx.disconnect_peer(peer_id.into());
            }
        }
    }
}

///TODO: REMOVE THIS AFTER TESTING
#[inline]
fn get_request_key(
    session_id: SeqNo,
    operation_id: SeqNo,
) -> u64 {
    let sess: u64 = session_id.into();
    let opid: u64 = operation_id.into();
    sess | (opid << 32)
}
