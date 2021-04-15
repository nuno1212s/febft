//! Communication primitives for `febft`, such as wire message formats.

#[cfg(not(feature = "expose_impl"))]
mod socket;

#[cfg(feature = "expose_impl")]
pub mod socket;

pub mod serialize;
pub mod message;
pub mod channel;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use std::sync::Arc;
use std::net::SocketAddr;
use std::time::Duration;

use either::{
    Left,
    Right,
    Either,
};
use rustls::{
    ClientConfig,
    ServerConfig,
};
use async_tls::{
    server::TlsStream as TlsStreamSrv,
    client::TlsStream as TlsStreamCli,
    TlsConnector,
    TlsAcceptor,
};
use futures::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use parking_lot::RwLock;
use futures_timer::Delay;
use futures::lock::Mutex;
use smallvec::SmallVec;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::collections::{self, HashMap};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::socket::{
    Socket,
    Listener,
};
use crate::bft::communication::message::{
    Header,
    Message,
    WireMessage,
    SystemMessage,
};
use crate::bft::communication::channel::{
    MessageChannelTx,
    MessageChannelRx,
    new_message_channel,
};
use crate::bft::crypto::signature::{
    PublicKey,
    KeyPair,
};

/// A `NodeId` represents the id of a process in the BFT system.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[repr(transparent)]
pub struct NodeId(u32);

impl NodeId {
    pub fn targets_u32<I>(into_iterator: I) -> impl Iterator<Item = Self>
    where
        I: IntoIterator<Item = u32>,
    {
        into_iterator
            .into_iter()
            .map(Self)
    }

    pub fn targets<I>(into_iterator: I) -> impl Iterator<Item = Self>
    where
        I: IntoIterator<Item = usize>,
    {
        into_iterator
            .into_iter()
            .map(NodeId::from)
    }
}

impl From<u32> for NodeId {
    #[inline]
    fn from(id: u32) -> NodeId {
        NodeId(id)
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

impl From<NodeId> for u32 {
    #[inline]
    fn from(id: NodeId) -> u32 {
        id.0 as u32
    }
}

enum PeerTx {
    // clients need shared access to the hashmap; the `Arc` on the second
    // lock allows us to take ownership of a copy of the socket, so we
    // don't block the thread with the guard of the first lock waiting
    // on the second one
    Client(Arc<RwLock<HashMap<NodeId, Arc<Mutex<TlsStreamCli<Socket>>>>>>),
    // replicas don't need shared access to the hashmap, so
    // we only need one lock (to restrict I/O to one producer at a time)
    Server(HashMap<NodeId, Arc<Mutex<TlsStreamCli<Socket>>>>),
}

struct NodeShared {
    my_key: KeyPair,
    peer_addrs: HashMap<NodeId, (SocketAddr, String)>,
    peer_keys: HashMap<NodeId, PublicKey>,
}

/// Container for handles to other processes in the system.
///
/// A `Node` constitutes the core component used in the wire
/// communication between processes.
pub struct Node<D: SharedData> {
    id: NodeId,
    my_tx: MessageChannelTx<D::Request, D::Reply>,
    my_rx: MessageChannelRx<D::Request, D::Reply>,
    shared: Arc<NodeShared>,
    peer_tx: PeerTx,
    connector: TlsConnector,
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
    /// The addresses of all nodes in the system (including clients),
    /// as well as the domain name associated with each address.
    ///
    /// For any `NodeConfig` assigned to `c`, the IP address of
    /// `c.addrs[&c.id]` should be equivalent to `localhost`.
    pub addrs: HashMap<NodeId, (SocketAddr, String)>,
    /// The list of public keys of all nodes in the system.
    pub pk: HashMap<NodeId, PublicKey>,
    /// The secret key of this particular `Node`.
    pub sk: KeyPair,
    /// The TLS configuration used to connect to peer nodes.
    pub client_config: ClientConfig,
    /// The TLS configuration used to accept connections from peer nodes.
    pub server_config: ServerConfig,
}

// max no. of messages allowed in the channel
const NODE_CHAN_BOUND: usize = 128;

// max no. of bytes to inline before doing a heap alloc
const NODE_BUFSIZ: usize = 16384;

// max no. of SendTo's to inline before doing a heap alloc
const NODE_VIEWSIZ: usize = 8;

type SendTos<D> = SmallVec<[SendTo<D>; NODE_VIEWSIZ]>;

impl<D> Node<D>
where
    D: SharedData + 'static,
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
    ) -> Result<(Self, Vec<Message<D::Request, D::Reply>>)> {
        let id = cfg.id;

        // initial checks of correctness
        if cfg.n < (3*cfg.f + 1) {
            return Err("Invalid number of replicas")
                .wrapped(ErrorKind::Communication);
        }
        if usize::from(id) >= cfg.n {
            return Err("Invalid node ID")
                .wrapped(ErrorKind::Communication);
        }

        let listener = socket::bind(cfg.addrs[&id].0).await
            .wrapped(ErrorKind::Communication)?;

        let (tx, rx) = new_message_channel::<D::Request, D::Reply>(NODE_CHAN_BOUND);
        let acceptor: TlsAcceptor = cfg.server_config.into();
        let connector: TlsConnector = cfg.client_config.into();

        // rx side (accept conns from replica)
        let first_cli: NodeId = cfg.n.into();
        rt::spawn(Self::rx_side_accept(first_cli, id, listener, acceptor, tx.clone()));

        // tx side (connect to replica)
        Self::tx_side_connect(cfg.n as u32, id, connector.clone(), tx.clone(), &cfg.addrs);

        // node def
        let peer_tx = if id >= first_cli {
            PeerTx::Client(Arc::new(RwLock::new(collections::hash_map())))
        } else {
            PeerTx::Server(collections::hash_map())
        };
        let shared = Arc::new(NodeShared{
            my_key: cfg.sk,
            peer_keys: cfg.pk,
            peer_addrs: cfg.addrs,
        });
        let mut node = Node {
            id,
            shared,
            peer_tx,
            my_tx: tx,
            my_rx: rx,
            connector,
        };

        // receive peer connections from channel
        let mut rogue = Vec::new();
        let mut c = vec![0; cfg.n];

        while c
            .iter()
            .enumerate()
            .any(|(id, &n)| id != usize::from(node.id) && n != 2_i32)
        {
            let message = node.my_rx.recv().await.unwrap();

            match message {
                Message::ConnectedTx(id, sock) => {
                    node.handle_connected_tx(id, sock);
                    let id: usize = id.into();
                    if id < cfg.n {
                        // not a client connection, increase count
                        c[id] += 1;
                    }
                },
                Message::ConnectedRx(id, sock) => {
                    node.handle_connected_rx(id, sock);
                    let id: usize = id.into();
                    if id < cfg.n {
                        // not a client connection, increase count
                        c[id] += 1;
                    }
                },
                Message::DisconnectedTx(NodeId(i)) => {
                    let s = format!("Node {} disconnected from send side", i);
                    return Err(s).wrapped(ErrorKind::Communication);
                },
                Message::DisconnectedRx(Some(NodeId(i))) => {
                    let s = format!("Node {} disconnected from receive side", i);
                    return Err(s).wrapped(ErrorKind::Communication);
                },
                Message::DisconnectedRx(None) => {
                    let s = "Disconnected from receive side";
                    return Err(s).wrapped(ErrorKind::Communication);
                },
                m => rogue.push(m),
            }
        }

        // success
        Ok((node, rogue))
    }

    /// Reports the id of this `Node`.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Send a `SystemMessage` to a single destination.
    ///
    /// This method is somewhat more efficient than calling `broadcast()`
    /// on a single target id.
    pub fn send(
        &self,
        message: SystemMessage<D::Request, D::Reply>,
        target: NodeId,
    ) {
        let send_to = self.send_to(target);
        let my_id = self.id;
        Self::send_impl(message, send_to, my_id, target)
    }

    #[inline]
    fn send_impl(
        message: SystemMessage<D::Request, D::Reply>,
        mut send_to: SendTo<D>,
        my_id: NodeId,
        target: NodeId,
    ) {
        rt::spawn(async move {
            // serialize
            let mut buf: SmallVec<[_; NODE_BUFSIZ]> = SmallVec::new();
            D::serialize_message(&mut buf, &message).unwrap();

            // send
            if my_id == target {
                // Right -> our turn
                send_to.value(Right((message, buf))).await;
            } else {
                // Left -> peer turn
                send_to.value(Left(buf)).await;
            }
        });
    }

    /// Broadcast a `SystemMessage` to a group of nodes.
    pub fn broadcast(
        &self,
        message: SystemMessage<D::Request, D::Reply>,
        targets: impl Iterator<Item = NodeId>,
    ) {
        let (mine, others) = self.send_tos(targets);
        Self::broadcast_impl(message, mine, others)
    }

    #[inline]
    fn broadcast_impl(
        message: SystemMessage<D::Request, D::Reply>,
        my_send_to: Option<SendTo<D>>,
        other_send_tos: SendTos<D>,
    ) {
        rt::spawn(async move {
            // serialize
            let mut buf: SmallVec<[_; NODE_BUFSIZ]> = SmallVec::new();
            D::serialize_message(&mut buf, &message).unwrap();

            // send to ourselves
            if let Some(mut send_to) = my_send_to {
                let buf = buf.clone();
                rt::spawn(async move {
                    // Right -> our turn
                    send_to.value(Right((message, buf))).await;
                });
            }

            // send to others
            for mut send_to in other_send_tos {
                let buf = buf.clone();
                rt::spawn(async move {
                    // Left -> peer turn
                    send_to.value(Left(buf)).await;
                });
            }

            // XXX: an either enum is used, which allows
            // rustc to prove only one task gets ownership
            // of the `message`, i.e. `Right` = oursleves
        });
    }

    #[inline]
    fn send_tos(
        &self,
        targets: impl Iterator<Item = NodeId>,
    ) -> (Option<SendTo<D>>, SendTos<D>) {
        let mut my_send_to = None;
        let mut other_send_tos = SendTos::new();

        match &self.peer_tx {
            PeerTx::Client(ref lock) => {
                let map = lock.read();
                self.create_send_tos(
                    &*map,
                    targets,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            },
            PeerTx::Server(ref map) => {
                self.create_send_tos(
                    map,
                    targets,
                    &mut my_send_to,
                    &mut other_send_tos,
                );
            },
        };


        (my_send_to, other_send_tos)
    }

    #[inline]
    fn create_send_tos(
        &self,
        map: &HashMap<NodeId, Arc<Mutex<TlsStreamCli<Socket>>>>,
        targets: impl Iterator<Item = NodeId>,
        my: &mut Option<SendTo<D>>,
        other: &mut SendTos<D>,
    ) {
        for id in targets {
            if id == self.id {
                let s = SendTo::Me {
                    my_id: self.id,
                    tx: self.my_tx.clone(),
                    shared: Arc::clone(&self.shared),
                };
                *my = Some(s);
            } else {
                let sock = Arc::clone(&map[&id]);
                let s = SendTo::Peers {
                    sock,
                    peer_id: id,
                    my_id: self.id,
                    tx: self.my_tx.clone(),
                    shared: Arc::clone(&self.shared),
                };
                other.push(s);
            }
        }
    }

    #[inline]
    fn send_to(&self, peer_id: NodeId) -> SendTo<D> {
        let my_id = self.id;
        let tx = self.my_tx.clone();
        let shared = Arc::clone(&self.shared);
        let sock = match &self.peer_tx {
            PeerTx::Client(ref lock) => {
                let map = lock.read();
                Arc::clone(&map[&peer_id])
            },
            PeerTx::Server(ref map) => {
                Arc::clone(&map[&peer_id])
            },
        };
        if my_id == peer_id {
            SendTo::Me {
                shared,
                my_id,
                tx,
            }
        } else {
            SendTo::Peers {
                sock,
                shared,
                peer_id,
                my_id,
                tx,
            }
        }
    }

    /// Receive one message from peer nodes or ourselves.
    pub async fn receive(&mut self) -> Result<Message<D::Request, D::Reply>> {
        self.my_rx.recv().await
    }

    /// Method called upon a `Message::ConnectedTx`.
    pub fn handle_connected_tx(&mut self, peer_id: NodeId, sock: TlsStreamCli<Socket>) {
        match &mut self.peer_tx {
            PeerTx::Server(ref mut peer_tx) => {
                peer_tx.insert(peer_id, Arc::new(Mutex::new(sock)));
            },
            PeerTx::Client(ref lock) => {
                let mut peer_tx = lock.write();
                peer_tx.insert(peer_id, Arc::new(Mutex::new(sock)));
            },
        }
    }

    /// Method called upon a `Message::ConnectedRx`.
    pub fn handle_connected_rx(&self, peer_id: NodeId, mut sock: TlsStreamSrv<Socket>) {
        let mut tx = self.my_tx.clone();
        rt::spawn(async move {
            let mut buf: SmallVec<[_; NODE_BUFSIZ]> = SmallVec::new();

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
                    },
                };

                tx.send(Message::System(header, message)).await.unwrap_or(());
            }

            // announce we have disconnected
            tx.send(Message::DisconnectedRx(Some(peer_id))).await.unwrap_or(());
        });
    }

    #[inline]
    fn tx_side_connect(
        n: u32,
        my_id: NodeId,
        connector: TlsConnector,
        tx: MessageChannelTx<D::Request, D::Reply>,
        addrs: &HashMap<NodeId, (SocketAddr, String)>,
    ) {
        for peer_id in NodeId::targets_u32(0..n).filter(|&id| id != my_id) {
            let tx = tx.clone();
            // FIXME: this line can crash the program if the user
            // provides an invalid HashMap, maybe return a Result<()>
            // from this function
            let addr = addrs[&peer_id].clone();
            let connector = connector.clone();
            rt::spawn(Self::tx_side_connect_task(my_id, peer_id, connector, tx, addr));
        }
    }

    async fn tx_side_connect_task(
        my_id: NodeId,
        peer_id: NodeId,
        connector: TlsConnector,
        mut tx: MessageChannelTx<D::Request, D::Reply>,
        (addr, hostname): (SocketAddr, String),
    ) {
        const RETRY: usize = 10;
        // notes
        // ========
        //
        // 1) not an issue if `tx` is closed, this is not a
        // permanently running task, so channel send failures
        // are tolerated
        //
        // 2) try to connect up to `RETRY` times, then announce
        // failure with a channel send op
        for _ in 0..RETRY {
            if let Ok(sock) = socket::connect(addr).await {
                // TLS handshake; drop connection if it fails
                let mut sock = match connector.connect(hostname, sock).await {
                    Ok(s) => s,
                    Err(_) => break,
                };

                // create header
                let (header, _) = WireMessage::new(
                    my_id,
                    peer_id,
                    &[],
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

                // success
                tx.send(Message::ConnectedTx(peer_id, sock)).await.unwrap_or(());
                return;
            }
            // sleep for 1 second and retry
            Delay::new(Duration::from_secs(1)).await;
        }
        // announce we have failed to connect to the peer node
        tx.send(Message::DisconnectedTx(peer_id)).await.unwrap_or(());
    }

    // TODO: check if we have terminated the node, and exit
    async fn rx_side_accept(
        first_cli: NodeId,
        my_id: NodeId,
        listener: Listener,
        acceptor: TlsAcceptor,
        tx: MessageChannelTx<D::Request, D::Reply>,
    ) {
        loop {
            if let Ok(sock) = listener.accept().await {
                let tx = tx.clone();
                let acceptor = acceptor.clone();
                rt::spawn(Self::rx_side_accept_task(first_cli, my_id, acceptor, sock, tx));
            }
        }
    }

    // performs a cryptographic handshake with a peer node;
    // header doesn't need to be signed, since we won't be
    // storing this message in the history log
    async fn rx_side_accept_task(
        first_cli: NodeId,
        my_id: NodeId,
        acceptor: TlsAcceptor,
        sock: Socket,
        mut tx: MessageChannelTx<D::Request, D::Reply>,
    ) {
        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // TLS handshake; drop connection if it fails
            let mut sock = match acceptor.accept(sock).await {
                Ok(s) => s,
                Err(_) => break,
            };

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

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

            tx.send(Message::ConnectedRx(peer_id, sock)).await.unwrap_or(());
            return;
        }

        // announce we have failed to connect to the peer node
        tx.send(Message::DisconnectedRx(None)).await.unwrap_or(());
    }
}

/// Represents a client node, with only sending capabilities.
pub struct ClientNode<D: SharedData> {
    id: NodeId,
    shared: Arc<NodeShared>,
    peer_tx: Arc<RwLock<HashMap<NodeId, Arc<Mutex<TlsStreamCli<Socket>>>>>>,
    my_tx: MessageChannelTx<D::Request, D::Reply>,
}

//impl<D> ClientNode<D>
//where
//    D: SharedData + 'static,
//    D::Request: Send + 'static,
//    D::Reply: Send + 'static,
//{
//    fn send_to(&self, peer_id: NodeId) -> SendTo<D> {
//        let my_id = self.id;
//        let tx = self.my_tx.clone();
//        if my_id != peer_id {
//            SendTo::Peers {
//                tx,
//                my_id,
//                peer_id,
//                data: Arc::clone(&self.peer_tx[&peer_id]),
//            }
//        } else {
//            SendTo::Me {
//                tx,
//                my_id,
//                sk: Arc::clone(&self.my_key),
//            }
//        }
//    }
//}

enum SendTo<D: SharedData> {
    Me {
        // our id
        my_id: NodeId,
        // shared data
        shared: Arc<NodeShared>,
        // a handle to our message channel
        tx: MessageChannelTx<D::Request, D::Reply>,
    },
    Peers {
        // our id
        my_id: NodeId,
        // the id of the peer
        peer_id: NodeId,
        // shared data
        shared: Arc<NodeShared>,
        // handle to socket
        sock: Arc<Mutex<TlsStreamCli<Socket>>>,
        // a handle to our message channel
        tx: MessageChannelTx<D::Request, D::Reply>,
    },
}

type Buf = SmallVec<[u8; NODE_BUFSIZ]>;

impl<D> SendTo<D>
where
    D: SharedData + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
    async fn value(&mut self, m: Either<Buf, (SystemMessage<D::Request, D::Reply>, Buf)>) {
        match self {
            SendTo::Me { my_id, shared: ref sh, ref mut tx } => {
                if let Right((m, b)) = m {
                    Self::me(*my_id, m, b, &sh.my_key, tx).await
                } else {
                    // optimize code path
                    unreachable!()
                }
            },
            SendTo::Peers { my_id, peer_id, shared: ref sh, ref sock, ref mut tx } => {
                if let Left(b) = m {
                    Self::peers(*my_id, *peer_id, b, &sh.my_key, &*sock, tx).await
                } else {
                    // optimize code path
                    unreachable!()
                }
            },
        }
    }

    async fn me(
        my_id: NodeId,
        m: SystemMessage<D::Request, D::Reply>,
        b: Buf,
        sk: &KeyPair,
        tx: &mut MessageChannelTx<D::Request, D::Reply>,
    ) {
        // create wire msg
        let (h, _) = WireMessage::new(
            my_id,
            my_id,
            &b[..],
            Some(sk),
        ).into_inner();

        // send
        tx.send(Message::System(h, m)).await.unwrap_or(())
    }

    async fn peers(
        my_id: NodeId,
        peer_id: NodeId,
        b: Buf,
        sk: &KeyPair,
        lock: &Mutex<TlsStreamCli<Socket>>,
        tx: &mut MessageChannelTx<D::Request, D::Reply>,
    ) {
        // create wire msg
        let wm = WireMessage::new(
            my_id,
            peer_id,
            &b[..],
            Some(sk),
        );

        // send
        //
        // FIXME: sending may hang forever, because of network
        // problems; add a timeout
        let mut sock = lock.lock().await;
        if let Err(_) = wm.write_to(&mut *sock).await {
            // error sending, drop connection
            tx.send(Message::DisconnectedRx(Some(peer_id))).await.unwrap_or(());
        }
    }
}
