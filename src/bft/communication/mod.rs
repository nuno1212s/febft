//! Communication primitives for `febft`, such as wire message formats.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_tls::{
    TlsAcceptor,
    TlsConnector,
};
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
use futures::lock::Mutex;
use futures_timer::Delay;
use intmap::IntMap;
use parking_lot::RwLock;
use rustls::{
    ClientConfig,
    ServerConfig,
};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::bft::async_runtime as rt;
use crate::bft::communication::message::{Header, Message, SerializedMessage, StoredSerializedSystemMessage, SystemMessage, WireMessage};
use crate::bft::communication::peer_handling::{ConnectedPeer, NodePeers};
use crate::bft::communication::serialize::{
    Buf,
    DigestData,
    SharedData,
};
use crate::bft::communication::socket::{
    Listener,
    SecureSocketRecv,
    SecureSocketSend,
    Socket,
};
use crate::bft::crypto::hash::Digest;
use crate::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};
use crate::bft::error::*;
use crate::bft::prng;
use crate::bft::prng::ThreadSafePrng;
use crate::bft::threadpool;

pub mod socket;
pub mod serialize;
pub mod message;
pub mod channel;
pub mod peer_handling;

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
    Client(Arc<RwLock<IntMap<Arc<Mutex<SecureSocketSend>>>>>),
    // replicas don't need shared access to the hashmap, so
    // we only need one lock (to restrict I/O to one producer at a time)
    Server(Arc<RwLock<IntMap<Arc<Mutex<SecureSocketSend>>>>>),
}

struct NodeShared {
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
    rng: prng::ThreadSafePrng,
    shared: Arc<NodeShared>,
    peer_tx: PeerTx,
    connector: TlsConnector,
    peer_addrs: IntMap<(SocketAddr, String)>,
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
    pub addrs: IntMap<(SocketAddr, String)>,
    /// The list of public keys of all nodes in the system.
    pub pk: IntMap<PublicKey>,
    /// The secret key of this particular `Node`.
    pub sk: KeyPair,
    /// The TLS configuration used to connect to peer nodes.
    pub client_config: ClientConfig,
    /// The TLS configuration used to accept connections from peer nodes.
    pub server_config: ServerConfig,
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

        let listener = socket::bind(cfg.addrs.get(id.into()).unwrap().0).await
            .wrapped(ErrorKind::Communication)?;

        let acceptor: TlsAcceptor = cfg.server_config.into();
        let connector: TlsConnector = cfg.client_config.into();

        // node def
        let peer_tx = if id >= cfg.first_cli {
            PeerTx::Client(Arc::new(RwLock::new(IntMap::new())))
        } else {
            PeerTx::Server(Arc::new(RwLock::new(IntMap::new())))
        };

        let shared = Arc::new(NodeShared {
            my_key: cfg.sk,
            peer_keys: cfg.pk,
        });

        //Setup all the peer message reception handling.
        let peers = NodePeers::new(cfg.id, cfg.first_cli, cfg.batch_size);

        let rng = ThreadSafePrng::new();

        let mut node = Arc::new(Node {
            id,
            rng,
            shared,
            peer_tx,
            node_handling: peers,
            connector: connector.clone(),
            peer_addrs: cfg.addrs,
            first_cli: cfg.first_cli,
        });

        let rx_node_clone = node.clone();

        // rx side (accept conns from replica)
        rt::spawn(rx_node_clone.rx_side_accept(cfg.first_cli, id, listener, acceptor));

        // tx side (connect to replica)
        let mut rng = prng::State::new();

        node.clone().tx_side_connect(
            cfg.n as u32,
            cfg.first_cli,
            id,
            connector,
            &node.peer_addrs,
            &mut rng,
        );

        // receive peer connections from channel
        let mut rogue = Vec::new();
        let mut c = vec![0; cfg.n];

        //TODO: Wait for all the replicas to have connected

        while node.node_handling.replica_count() < 4 {

            //Any received messages will be handled by the connection pool buffers
            println!("Connected to {} replicas on the node {:?}", node.node_handling.replica_count(), node.id);

            Delay::new(Duration::from_secs(2)).await;
        }

        println!("Found all nodes required {}", node.node_handling.replica_count());

        // while c
        //     .iter()
        //     .enumerate()
        //     .any(|(id, &n)| id != usize::from(node.id) && n != 2_i32)
        // {
        //     // let message = node..recv().await.unwrap();
        //
        //     match message {
        //         Message::ConnectedTx(id, sock) => {
        //             node.handle_connected_tx(id, sock);
        //             if id < cfg.first_cli {
        //                 // not a client connection, increase count
        //                 c[usize::from(id)] += 1;
        //             }
        //         }
        //         Message::ConnectedRx(id, sock) => {
        //             node.handle_connected_rx(id, sock);
        //             if id < cfg.first_cli {
        //                 // not a client connection, increase count
        //                 c[usize::from(id)] += 1;
        //             }
        //         }
        //         Message::DisconnectedTx(NodeId(i)) => {
        //             let s = format!("Node {} disconnected from send side", i);
        //             return Err(s).wrapped(ErrorKind::Communication);
        //         }
        //         Message::DisconnectedRx(Some(NodeId(i))) => {
        //             let s = format!("Node {} disconnected from receive side", i);
        //             return Err(s).wrapped(ErrorKind::Communication);
        //         }
        //         Message::DisconnectedRx(None) => {
        //             let s = "Disconnected from receive side";
        //             return Err(s).wrapped(ErrorKind::Communication);
        //         }
        //         m => rogue.push(m),
        //     }
        // }

        // success
        Ok((node, rogue))
    }

    fn resolve_client_rx_connection(&self, node_id: NodeId) -> Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.resolve_peer_conn(node_id)
            .expect(&*format!("Failed to resolve peer connection for {:?}", node_id))
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
            rng: prng::State::new(),
            shared: Arc::clone(&self.shared),
            peer_tx: self.peer_tx.clone(),
            parent_node: Arc::clone(self),
            channel: Arc::clone(self.loopback_channel()),
        }
    }

    /// Returns a handle to the loopback channel of this `Node`.
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
        let send_to = Self::send_to(
            flush,
            self.id,
            target,
            None,
            self.resolve_client_rx_connection(target),
            &self.peer_tx,
        );

        let my_id = self.id;
        let nonce = self.rng.next_state();
        Self::send_impl(message, send_to, my_id, target, nonce)
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
        let send_to = Self::send_to(
            true,
            self.id,
            target,
            Some(&self.shared),
            self.resolve_client_rx_connection(target),
            &self.peer_tx,
        );

        let my_id = self.id;

        let nonce = self.rng.next_state();
        Self::send_impl(message, send_to, my_id, target, nonce)
    }

    #[inline]
    fn send_impl(
        message: SystemMessage<D::State, D::Request, D::Reply>,
        mut send_to: SendTo<D>,
        my_id: NodeId,
        target: NodeId,
        nonce: u64,
    ) {
        threadpool::execute(move || {
            // serialize
            let mut buf: Buf = Buf::new();
            let digest = <D as DigestData>::serialize_digest(
                &message,
                &mut buf,
            ).unwrap();

            // send
            if my_id == target {
                // Right -> our turn
                rt::spawn(async move {
                    send_to.value(Right((message, nonce, digest, buf))).await;
                });
            } else {
                // Left -> peer turn
                rt::spawn(async move {
                    send_to.value(Left((nonce, digest, buf))).await;
                });
            }
        });
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    pub fn broadcast(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let (mine, others) = self.send_tos(
            self.id,
            &self.peer_tx,
            None,
            targets,
        );
        let nonce = self.rng.next_state();
        Self::broadcast_impl(message, mine, others, nonce)
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    ///
    /// This variant of `broadcast()` signs the sent message.
    pub fn broadcast_signed(
        &self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let (mine, others) = self.send_tos(
            self.id,
            &self.peer_tx,
            Some(&self.shared),
            targets,
        );

        let nonce = self.rng.next_state();

        Self::broadcast_impl(message, mine, others, nonce)
    }

    pub fn broadcast_serialized(
        &self,
        messages: IntMap<StoredSerializedSystemMessage<D>>,
    ) {
        let headers = messages
            .values()
            .map(|stored| stored.header());

        let (mine, others) = self.serialized_send_tos(
            self.id,
            &self.peer_tx,
            headers,
        );

        Self::broadcast_serialized_impl(messages, mine, others);
    }

    #[inline]
    fn broadcast_serialized_impl(
        mut messages: IntMap<StoredSerializedSystemMessage<D>>,
        my_send_to: Option<SerializedSendTo<D>>,
        other_send_tos: SerializedSendTos<D>,
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
                rt::spawn(async move {
                    send_to.value(header, message).await;
                });
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
                rt::spawn(async move {
                    send_to.value(header, message).await;
                });
            }
        });
    }

    #[inline]
    fn broadcast_impl(
        message: SystemMessage<D::State, D::Request, D::Reply>,
        my_send_to: Option<SendTo<D>>,
        other_send_tos: SendTos<D>,
        nonce: u64,
    ) {
        threadpool::execute(move || {
            // serialize
            let mut buf: Buf = Buf::new();
            let digest = <D as DigestData>::serialize_digest(
                &message,
                &mut buf,
            ).unwrap();

            // send to ourselves
            if let Some(mut send_to) = my_send_to {
                let buf = buf.clone();
                rt::spawn(async move {
                    // Right -> our turn
                    send_to.value(Right((message, nonce, digest, buf))).await;
                });
            }

            // send to others
            for mut send_to in other_send_tos {
                let buf = buf.clone();
                rt::spawn(async move {
                    // Left -> peer turn
                    send_to.value(Left((nonce, digest, buf))).await;
                });
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

        match peer_tx {
            PeerTx::Client(ref lock) => {
                let map = lock.read();

                self.create_send_tos(
                    my_id,
                    shared,
                    &*map,
                    targets,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            }
            PeerTx::Server(ref lock) => {
                let map = lock.read();

                self.create_send_tos(
                    my_id,
                    shared,
                    &*map,
                    targets,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            }
        };

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

        match peer_tx {
            PeerTx::Client(ref lock) => {
                let map = lock.read();
                self.create_serialized_send_tos(
                    my_id,
                    &*map,
                    headers,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            }
            PeerTx::Server(ref lock) => {
                let map = lock.read();
                self.create_serialized_send_tos(
                    my_id,
                    &*map,
                    headers,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            }
        };

        (my_send_to, other_send_tos)
    }

    #[inline]
    fn create_serialized_send_tos<'a>(
        &self,
        my_id: NodeId,
        map: &IntMap<Arc<Mutex<SecureSocketSend>>>,
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
                    tx: self.resolve_client_rx_connection(id),
                };
                *mine = Some(s);
            } else {
                let sock = Arc::clone(map.get(id.into()).unwrap());
                let s = SerializedSendTo::Peers {
                    id,
                    sock,
                    //Get the RX channel for the peer to mark as DCed if it fails
                    tx: self.resolve_client_rx_connection(id),
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
        map: &IntMap<Arc<Mutex<SecureSocketSend>>>,
        targets: impl Iterator<Item=NodeId>,
        mine: &mut Option<SendTo<D>>,
        others: &mut SendTos<D>,
    ) {
        for id in targets {
            if id == my_id {
                let s = SendTo::Me {
                    my_id,
                    //get our own channel to send to ourselves
                    tx: self.resolve_client_rx_connection(id),
                    shared: shared.map(|sh| Arc::clone(sh)),
                };
                *mine = Some(s);
            } else {
                let sock = Arc::clone(map.get(id.into()).unwrap());
                let s = SendTo::Peers {
                    sock,
                    my_id,
                    peer_id: id,
                    flush: true,
                    //Get the RX channel for the peer to mark as DCed if it fails
                    tx: self.resolve_client_rx_connection(id),
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
            let sock = match peer_tx {
                PeerTx::Client(ref lock) => {
                    let map = lock.read();
                    Arc::clone(map.get(peer_id.into()).unwrap())
                }
                PeerTx::Server(ref lock) => {
                    let map = lock.read();
                    Arc::clone(map.get(peer_id.into()).unwrap())
                }
            };
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

    //Receive messages from the clients we are connected to
    pub fn receive_from_clients(&self) -> Result<Vec<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.receive_from_clients()
    }

    //Receive messages from the replicas we are connected to
    pub fn receive_from_replicas(&self) -> Result<Vec<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.receive_from_replicas()
    }

    //Receive messages from the clients we are connected to
    pub async fn receive_from_clients_async(&self) -> Result<Vec<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.receive_from_client_async().await
    }

    //Receive messages from the replicas we are connected to
    pub async fn receive_from_replicas_async(&self) -> Result<Vec<Message<D::State, D::Request, D::Reply>>> {
        self.node_handling.receive_from_replicas_async().await
    }

    /// Method called upon a `Message::ConnectedTx`.
    /// Registers the newly created transmission socket to the peer
    pub fn handle_connected_tx(&self, peer_id: NodeId, sock: SecureSocketSend) {
        println!("Connected TX to peer {:?} from peer {:?}", peer_id, self.id);

        match &self.peer_tx {
            ///If we are a replica?
            PeerTx::Server(ref lock) => {
                let mut peer_tx = lock.write();
                peer_tx.insert(peer_id.into(), Arc::new(Mutex::new(sock)));
            }
            ///If we are a client?
            PeerTx::Client(ref lock) => {
                let mut peer_tx = lock.write();
                peer_tx.insert(peer_id.into(), Arc::new(Mutex::new(sock)));
            }
        }
    }

    /// Method called upon a `Message::ConnectedRx`.
    /// Handles client connections
    pub async fn handle_connected_rx(self: Arc<Self>, peer_id: NodeId, mut sock: SecureSocketRecv) {
        // we are a server node
        if let PeerTx::Server(_) = &self.peer_tx {
            // the node whose conn we accepted is a client
            // and we aren't connected to it yet
            if peer_id >= self.first_cli {
                // fetch client address
                //
                // FIXME: this line can crash the program if the user
                // provides an invalid HashMap
                let addr = self.peer_addrs.get(peer_id.into()).unwrap().clone();

                // connect
                let nonce = self.rng.next_state();

                rt::spawn(Self::tx_side_connect_task(
                    self.clone(),
                    self.id,
                    self.first_cli,
                    peer_id,
                    nonce,
                    self.connector.clone(),
                    addr,
                ));
            }
        }

        //Init the per client queue and start putting the received messages into it
        println!("Handling connection of peer {:?} in peer {:?}", peer_id, self.id);

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

            client.push_request(Message::System(header, message)).await;

            //tx.send(Message::System(header, message)).await.unwrap_or(());
        }

        // announce we have disconnected
        client.disconnect();

        //tx.send(Message::DisconnectedRx(Some(peer_id))).await.unwrap_or(());
    }

    #[inline]
    fn tx_side_connect(
        self: Arc<Self>,
        n: u32,
        first_cli: NodeId,
        my_id: NodeId,
        connector: TlsConnector,
        addrs: &IntMap<(SocketAddr, String)>,
        rng: &mut prng::State,
    ) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != my_id) {
            // FIXME: this line can crash the program if the user
            // provides an invalid HashMap, maybe return a Result<()>
            // from this function
            let addr = addrs.get(peer_id.into()).unwrap().clone();
            let connector = connector.clone();
            let nonce = rng.next_state();

            println!("Attempting to connect to peer {:?} with address {:?} from node {:?}", peer_id, addr, my_id);

            let arc = self.clone();

            rt::spawn(arc.tx_side_connect_task(my_id, first_cli, peer_id,
                                               nonce, connector, addr));
        }
    }

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
            println!("Trying attempt {} for Node {:?} from peer {:?}", _try, peer_id, my_id);
            if let Ok(mut sock) = socket::connect(addr).await {
                // create header
                let (header, _) = WireMessage::new(
                    my_id,
                    peer_id,
                    &[],
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
                    SecureSocketSend::Plain(BufWriter::new(sock))
                } else {
                    match connector.connect(hostname, sock).await {
                        Ok(s) => SecureSocketSend::Tls(s),
                        Err(_) => break,
                    }
                };

                // success
                self.handle_connected_tx(peer_id, sock);

                println!("Ended connection attempt {} for Node {:?} from peer {:?}", _try, peer_id, my_id);
                return;
            }

            // sleep for `SECS` seconds and retry
            Delay::new(Duration::from_secs(SECS)).await;
        }

        // announce we have failed to connect to the peer node
        //if we fail to connect, then just ignore
        //tx.send(Message::DisconnectedTx(peer_id)).await.unwrap_or(());
    }

    // TODO: check if we have terminated the node, and exit
    async fn rx_side_accept(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        listener: Listener,
        acceptor: TlsAcceptor,
    ) {
        loop {
            if let Ok(sock) = listener.accept().await {
                let acceptor = acceptor.clone();
                rt::spawn(self.clone().rx_side_accept_task(first_cli, my_id, acceptor, sock));
            }
        }
    }

    // performs a cryptographic handshake with a peer node;
    // header doesn't need to be signed, since we won't be
    // storing this message in the log
    async fn rx_side_accept_task(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        acceptor: TlsAcceptor,
        mut sock: Socket,
    ) {
        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            println!("Node {:?} received connection from node", my_id);

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, &[]) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            println!("Node {:?} received connection from node {:?}", my_id, peer_id);

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                SecureSocketRecv::Plain(BufReader::new(sock))
            } else {
                match acceptor.accept(sock).await {
                    Ok(s) => SecureSocketRecv::Tls(s),
                    Err(_) => break,
                }
            };

            self.handle_connected_rx(peer_id, sock).await;
            //tx.send(Message::ConnectedRx(peer_id, sock)).await.unwrap_or(());

            return;
        }

        // announce we have failed to connect to the peer node
        //tx.send(Message::DisconnectedRx(None)).await.unwrap_or(());
    }
}

/// Represents a node with sending capabilities only.
pub struct SendNode<D: SharedData + 'static> {
    id: NodeId,
    shared: Arc<NodeShared>,
    rng: prng::State,
    peer_tx: PeerTx,
    parent_node: Arc<Node<D>>,
    channel: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
}

impl<D: SharedData> Clone for SendNode<D> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            rng: prng::State::new(),
            shared: Arc::clone(&self.shared),
            peer_tx: self.peer_tx.clone(),
            parent_node: self.parent_node.clone(),
            channel: self.channel.clone(),
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

    /// Check the `master_channel()` documentation for `Node`.

    /// Check the `send()` documentation for `Node`.
    pub fn send(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
        flush: bool,
    ) {
        let send_to = <Node<D>>::send_to(
            flush,
            self.id,
            target,
            None,
            self.parent_node.resolve_client_rx_connection(target),
            &self.peer_tx,
        );

        let my_id = self.id;
        let nonce = self.rng.next_state();

        <Node<D>>::send_impl(message, send_to, my_id, target, nonce)
    }

    /// Check the `send_signed()` documentation for `Node`.
    pub fn send_signed(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        target: NodeId,
    ) {
        let send_to = <Node<D>>::send_to(
            true,
            self.id,
            target,
            Some(&self.shared),
            self.parent_node.resolve_client_rx_connection(target),
            &self.peer_tx,
        );
        let my_id = self.id;
        let nonce = self.rng.next_state();
        <Node<D>>::send_impl(message, send_to, my_id, target, nonce)
    }

    /// Check the `broadcast()` documentation for `Node`.
    pub fn broadcast(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let (mine, others) = self.parent_node.send_tos(
            self.id,
            &self.peer_tx,
            None,
            targets,
        );
        let nonce = self.rng.next_state();
        <Node<D>>::broadcast_impl(message, mine, others, nonce)
    }

    /// Check the `broadcast_signed()` documentation for `Node`.
    pub fn broadcast_signed(
        &mut self,
        message: SystemMessage<D::State, D::Request, D::Reply>,
        targets: impl Iterator<Item=NodeId>,
    ) {
        let (mine, others) = self.parent_node.send_tos(
            self.id,
            &self.peer_tx,
            Some(&self.shared),
            targets,
        );

        let nonce = self.rng.next_state();
        <Node<D>>::broadcast_impl(message, mine, others, nonce)
    }
}

// helper type used when either a `send()` or a `broadcast()`
// is called by a `Node` or `SendNode`.
//
// holds some data that can be shared between threads, relevant
// to a network write operation, or channel write operation,
// depending on whether we're sending a message to a peer node
// or ourselves
enum SendTo<D: SharedData> {
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
        sock: Arc<Mutex<SecureSocketSend>>,
        // a handle to the message channel of the corresponding client
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
}

enum SerializedSendTo<D: SharedData> {
    Me {
        // our id
        id: NodeId,
        // a handle to our client handle
        tx: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    },
    Peers {
        // the id of the peer
        id: NodeId,
        // handle to socket
        sock: Arc<Mutex<SecureSocketSend>>,
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
    async fn value(
        self,
        m: Either<(u64, Digest, Buf), (SystemMessage<D::State, D::Request, D::Reply>, u64, Digest, Buf)>,
    ) {
        match self {
            SendTo::Me { my_id, shared: ref sh, tx } => {
                let key = sh.as_ref().map(|ref sh| &sh.my_key);
                if let Right((m, n, d, b)) = m {
                    Self::me(my_id, m, n, d, b, key, tx).await
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
            SendTo::Peers {
                flush, my_id, peer_id,
                shared: ref sh, sock, tx
            } => {
                let key = sh.as_ref().map(|ref sh| &sh.my_key);
                if let Left((n, d, b)) = m {
                    Self::peers(flush, my_id, peer_id, n, d, b, key, sock, tx).await
                } else {
                    // optimize code path
                    unreachable!()
                }
            }
        }
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
            &b[..],
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
        lock: Arc<Mutex<SecureSocketSend>>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {
        // create wire msg
        let wm = WireMessage::new(
            my_id,
            peer_id,
            &b[..],
            n,
            Some(d),
            sk,
        );

        // send
        //
        // FIXME: sending may hang forever, because of network
        // problems; add a timeout
        let mut sock = lock.lock().await;
        if let Err(_) = wm.write_to(&mut *sock, flush).await {
            // error sending, drop connection

            //TODO: Since this only handles receiving stuff, do we have to disconnect?
            //Idk...
            cli.disconnect();
            //tx.send(Message::DisconnectedTx(peer_id)).await.unwrap_or(());
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
    async fn value(
        self,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
    ) {
        match self {
            SerializedSendTo::Me { tx, .. } => {
                Self::me(h, m, tx).await
            }
            SerializedSendTo::Peers { id, sock, tx } => {
                Self::peers(id, h, m, sock, tx).await
            }
        }
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

    async fn peers(
        peer_id: NodeId,
        h: Header,
        m: SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>,
        lock: Arc<Mutex<SecureSocketSend>>,
        cli: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>,
    ) {
        // create wire msg
        let (_, raw) = m.into_inner();
        let wm = WireMessage::from_parts(h, &raw[..]).unwrap();

        // send
        //
        // FIXME: sending may hang forever, because of network
        // problems; add a timeout
        let mut sock = lock.lock().await;
        if let Err(_) = wm.write_to(&mut *sock, true).await {
            // error sending, drop connection

            //TODO: Since this only handles receiving stuff, do we have to disconnect?
            //Idk...
            cli.disconnect();
            //tx.send(Message::DisconnectedTx(peer_id)).await.unwrap_or(());
        }
    }
}
