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
use std::collections::HashMap;
use std::time::Duration;

use futures_timer::Delay;
use futures::lock::Mutex;
use smallvec::SmallVec;
use rand_core::{RngCore, OsRng};
use futures::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::socket::{
    Socket,
    Listener,
};
use crate::bft::communication::message::{
    Header,
    Message,
    WireMessage,
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
    pub fn targets<I>(into_iterator: I) -> impl Iterator<Item = Self>
    where
        I: IntoIterator<Item = u32>,
    {
        into_iterator
            .into_iter()
            .map(Self)
    }
}

impl From<u32> for NodeId {
    #[inline]
    fn from(id: u32) -> NodeId {
        NodeId(id)
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
    sock: Mutex<Socket>,
}

/// A `Node` contains handles to other processes in the system, and is
/// the core component used in the wire communication between processes.
pub struct Node<O> {
    id: NodeId,
    my_key: Arc<KeyPair>,
    peer_keys: Arc<HashMap<NodeId, PublicKey>>,
    peer_addrs: HashMap<NodeId, SocketAddr>,
    peer_tx: HashMap<NodeId, Arc<NodeTxData>>,
    my_tx: MessageChannelTx<O>,
    my_rx: MessageChannelRx<O>,
}

/// Represents a configuration used to bootstrap a `Node`.
pub struct NodeConfig {
    /// The number of nodes allowed to fail in the system.
    /// Typically, BFT systems set this parameter to 1.
    pub f: usize,
    /// The id of this `Node`.
    pub id: NodeId,
    /// The addresses of all nodes in the system.
    ///
    /// The number of stored addresses accounts for the `n` parameter
    /// of the BFT system, i.e. `n >= 3*f + 1`. For any `NodeConfig`
    /// assigned to `c`, the IP address of `c.addrs[c.id.into()]`
    /// should be equivalent to `localhost`.
    pub addrs: HashMap<NodeId, SocketAddr>,
    /// The secret key of this particular `Node`.
    pub sk: KeyPair,
    /// The list of public keys of all nodes in the system.
    pub pk: HashMap<NodeId, PublicKey>,
}

impl NodeConfig {
    /// Returns the address of the running `Node`.
    pub fn my_addr(&self) -> SocketAddr {
        self.addr_of(self.id)
    }

    /// Returns the address of a node with id `i`.
    pub fn addr_of(&self, i: NodeId) -> SocketAddr {
        self.addrs[&i]
    }

    /// Returns a reference to our `KeyPair`.
    pub fn key_pair(&self) -> &KeyPair {
        &self.sk
    }

    /// Returns a reference to the `PublicKey` of the node `i`.
    pub fn public_key_of(&self, i: NodeId) -> &PublicKey {
        &self.pk[&i]
    }
}

impl<O: Send + 'static> Node<O> {
    // max no. of messages allowed in the channel
    const CHAN_BOUND: usize = 128;

    // max no. of bytes to inline before doing a heap alloc
    const BUFSIZ_RECV: usize = 16384;

    // number of random bytes to send on handshake
    const NONCE_LENGTH: usize = 8;

    /// Bootstrap a `Node`, i.e. create connections between itself and its
    /// peer nodes.
    ///
    /// Rogue messages (i.e. not pertaining to the bootstrapping protocol)
    /// are returned in a `Vec`.
    pub async fn bootstrap(cfg: NodeConfig) -> Result<(Self, Vec<Message<O>>)> {
        if cfg.addrs.len() < (3*cfg.f + 1) {
            return Err("Invalid number of replicas")
                .wrapped(ErrorKind::Communication);
        }
        if usize::from(cfg.id) >= cfg.addrs.len() {
            return Err("Invalid node ID")
                .wrapped(ErrorKind::Communication);
        }

        let id = cfg.id;
        let n = cfg.addrs.len();

        let listener = socket::bind(cfg.addrs[&id]).await
            .wrapped(ErrorKind::Communication)?;

        let mut peer_tx = HashMap::new();
        let peer_keys = Arc::new(cfg.pk);
        let (tx, rx) = new_message_channel::<O>(Self::CHAN_BOUND);

        // rx side (accept conns from replica)
        rt::spawn(Self::rx_side_accept(id, listener, tx.clone(), Arc::clone(&peer_keys)));

        // share the secret key between other tasks, but keep it in a
        // single memory location, with an `Arc`
        let my_key = Arc::new(cfg.sk);

        // tx side (connect to replica)
        Self::tx_side_connect(id, Arc::clone(&my_key), tx.clone(), &cfg.addrs);

        // success
        let node = Node {
            id,
            my_key,
            my_tx: tx,
            my_rx: rx,
            peer_tx,
            peer_keys,
            peer_addrs: cfg.addrs,
        };
        Ok((node, Vec::new()))
    }

    #[inline]
    fn tx_side_connect(
        id: NodeId,
        sk: Arc<KeyPair>,
        tx: MessageChannelTx<O>,
        addrs: &HashMap<NodeId, SocketAddr>,
    ) {
        for other_id in NodeId::targets((0..n).filter(|&i| i != id)) {
            let tx = tx.clone();
            let sk = Arc::clone(&sk);
            let addr = addrs[&other_id];
            rt::spawn(async move {
                // try 10 times
                for _ in 0..10 {
                    if let Ok(mut sock) = socket::connect(addr).await {
                        conn.write_u32(id).await.unwrap();
                        tx.send(Message::ConnectedTx(other_id, conn)).await.unwrap();
                        return;
                    }
                    // sleep for 1 second and retry
                    Delay::new(Duration::from_secs(1)).await;
                }
                // announce we have failed to connect to the peer node
                let e = Error::simple(ErrorKind::Communication);
                tx.send(Message::Error(other_id, e)).await.unwrap();
            });
        }
    }

    // TODO: check if we have terminated the node, and exit
    async fn rx_side_accept(
        my_id: NodeId,
        listener: Listener,
        mut tx: MessageChannelTx<O>,
        sk: Arc<KeyPair>,
        pk: Arc<HashMap<NodeId, PublicKey>>,
    ) {
        loop {
            if let Ok(sock) = listener.accept().await {
                let tx = tx.clone();
                let sk = Arc::clone(&sk);
                let pk = Arc::clone(&pk);
                rt::spawn(Self::rx_side_accept_task(sock, tx, sk, pk));
            }
        }
    }

    // performs a cryptographic handshake with a peer node
    async fn rx_side_accept_task(
        my_id: NodeId,
        mut sock: Socket,
        mut tx: MessageChannelTx<O>,
        sk: Arc<KeyPair>,
        pk: Arc<HashMap<NodeId, PublicKey>>,
    ) {
        let mut buf = [0; Header::LENGTH + NONCE_LENGTH];
        let mut buf_nonce = [0; 2 * NONCE_LENGTH];

        // since we take the 2nd turn writing our
        // nonce, save it on the 2nd half of the buffer
        OsRng.fill_bytes(&mut buf_nonce[NONCE_LENGTH..]);

        // read the peer's nonce
        if let Err(_) = sock.read_exact(&mut buf_nonce[..NONCE_LENGTH]).await {
            // errors reading -> faulty connection;
            // drop this socket
            return;
        }

        // write our nonce
        if let Err(_) = sock.write_all(&mut buf_nonce[NONCE_LENGTH..]).await {
            // errors reading -> faulty connection;
            // drop this socket
            return;
        }

        // read the peer's header
        if let Err(_) = sock.read_exact(&mut buf[..Header::Length]).await {
            // errors reading -> faulty connection;
            // drop this socket
            return;
        }

        // we are passing the correct length, safe to use unwrap()
        let header = Header::deserialize_from(&buf[..]).unwrap();

        // check if they are who they say who they are
        let peer_id = {
            let id = header.from();

            // try to extract the public key associated with the
            // node in the header
            let pk = match pk.get(&id) {
                Some(pk) => pk,
                None => return,
            };

            // confirm peer identity
            match WireMessage::from_parts(header, &buf_nonce[..]) {
                Ok(wm) if !wm.is_valid(my_id, pk) => {
                    // invalid identity; drop connection
                    return;
                },
                Ok(_) => id,
                Err(_) => return,
            }
        };

        // send our own identity to confirm handshake
        let wm = WireMessage::new(&sk, my_id, peer_id, &buf_nonce[..]);

        // ...

        tx.send(Message::ConnectedRx(id, sock)).await.unwrap_or(());
    }
}
