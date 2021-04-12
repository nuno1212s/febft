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
use futures_timer::Delay;
use futures::lock::Mutex;
use smallvec::SmallVec;
use futures::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

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

struct NodeTxData {
    sk: Arc<KeyPair>,
    sock: Mutex<TlsStreamCli<Socket>>,
}

/// A `Node` contains handles to other processes in the system, and is
/// the core component used in the wire communication between processes.
pub struct Node<D: SharedData> {
    id: NodeId,
    my_key: Arc<KeyPair>,
    peer_keys: Arc<HashMap<NodeId, PublicKey>>,
    my_tx: MessageChannelTx<D::Request, D::Reply>,
    my_rx: MessageChannelRx<D::Request, D::Reply>,
    peer_addrs: HashMap<NodeId, (SocketAddr, String)>,
    peer_tx: HashMap<NodeId, Arc<NodeTxData>>,
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

impl<D> Node<D>
where
    D: SharedData + 'static,
    D::Request: Clone + Send + 'static,
    D::Reply: Clone + Send + 'static,
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

        // share the secret key between other tasks, but keep it in a
        // single memory location, with an `Arc`
        let my_key = Arc::new(cfg.sk);

        // node def
        let peer_tx = collections::hash_map();
        let peer_keys = Arc::new(cfg.pk);
        let mut node = Node {
            id,
            my_key,
            my_tx: tx,
            my_rx: rx,
            peer_tx,
            peer_keys,
            peer_addrs: cfg.addrs,
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

    /// Broadcast a `SystemMessage` to a group of nodes.
    pub fn broadcast(
        &self,
        message: SystemMessage<D::Request, D::Reply>,
        targets: impl Iterator<Item = NodeId>,
    ) {
        for id in targets {
            let mut send_to = self.send_to(id);
            let message = message.clone();
            rt::spawn(async move {
                send_to.value(message).await;
            });
        }
    }

    fn send_to(&self, peer_id: NodeId) -> SendTo<D> {
        let my_id = self.id;
        let tx = self.my_tx.clone();
        if my_id != peer_id {
            SendTo::Peers {
                tx,
                my_id,
                peer_id,
                data: Arc::clone(&self.peer_tx[&peer_id]),
            }
        } else {
            SendTo::Me {
                tx,
                my_id,
                sk: Arc::clone(&self.my_key),
            }
        }
    }

    /// Receive one message from peer nodes or ourselves.
    pub async fn receive(&mut self) -> Result<Message<D::Request, D::Reply>> {
        self.my_rx.recv().await
    }

    /// Method called upon a `Message::ConnectedTx`.
    pub fn handle_connected_tx(&mut self, peer_id: NodeId, sock: TlsStreamCli<Socket>) {
        self.peer_tx.insert(peer_id, Arc::new(NodeTxData {
            sk: Arc::clone(&self.my_key),
            sock: Mutex::new(sock),
        }));
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

enum SendTo<D: SharedData> {
    Me {
        // our id
        my_id: NodeId,
        // our secret key
        sk: Arc<KeyPair>,
        // a handle to our message channel
        tx: MessageChannelTx<D::Request, D::Reply>,
    },
    Peers {
        // our id
        my_id: NodeId,
        // the id of the peer
        peer_id: NodeId,
        // data associated with peer
        data: Arc<NodeTxData>,
        // a handle to our message channel
        tx: MessageChannelTx<D::Request, D::Reply>,
    },
}

impl<D> SendTo<D>
where
    D: SharedData + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
    async fn value(&mut self, m: SystemMessage<D::Request, D::Reply>) {
        match self {
            SendTo::Me { my_id, ref sk, ref mut tx } => {
                Self::me(*my_id, m, &*sk, tx).await
            },
            SendTo::Peers { my_id, peer_id, ref data, ref mut tx } => {
                Self::peers(*my_id, *peer_id, m, &*data, tx).await
            },
        }
    }

    async fn me(
        my_id: NodeId,
        m: SystemMessage<D::Request, D::Reply>,
        sk: &KeyPair,
        tx: &mut MessageChannelTx<D::Request, D::Reply>,
    ) {
        // serialize
        let mut buf: SmallVec<[_; NODE_BUFSIZ]> = SmallVec::new();
        D::serialize_message(&mut buf, &m).unwrap();

        // create wire msg
        let (h, _) = WireMessage::new(
            my_id,
            my_id,
            &buf[..],
            Some(sk),
        ).into_inner();

        // send
        tx.send(Message::System(h, m)).await.unwrap_or(())
    }

    async fn peers(
        my_id: NodeId,
        peer_id: NodeId,
        m: SystemMessage<D::Request, D::Reply>,
        d: &NodeTxData,
        tx: &mut MessageChannelTx<D::Request, D::Reply>,
    ) {
        // serialize
        let mut buf: SmallVec<[_; NODE_BUFSIZ]> = SmallVec::new();
        D::serialize_message(&mut buf, &m).unwrap();

        // FIXME : AVOID SERIALIZING TWICE ^^^^

        // create wire msg
        let wm = WireMessage::new(
            my_id,
            peer_id,
            &buf[..],
            Some(&d.sk),
        );

        // send
        let mut sock = d.sock.lock().await;
        if let Err(_) = wm.write_to(&mut *sock).await {
            // error sending, drop connection
            tx.send(Message::DisconnectedRx(Some(peer_id))).await.unwrap_or(());
        }
    }
}
