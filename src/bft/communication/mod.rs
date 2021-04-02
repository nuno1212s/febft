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

use futures::io::AsyncReadExt;
use futures::lock::Mutex;
use smallvec::SmallVec;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{
    Header,
    Message,
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

struct PeerData {
    addr: SocketAddr,
    pk: PublicKey,
}

/// A `Node` contains handles to other processes in the system, and is
/// the core component used in the wire communication between processes.
pub struct Node<O> {
    id: NodeId,
    peers: Arc<HashMap<NodeId, PeerData>>,
    others_tx: HashMap<NodeId, Arc<NodeTxData>>,
    my_tx: MessageChannelTx<O>,
    my_rx: MessageChannelTx<O>,
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

impl<O> Node<O> {
    // max no. of messages allowed in the channel
    const CHAN_BOUND: usize = 128;

    // max no. of bytes to inline before doing a heap alloc
    const BUFSIZ_RECV: usize = 16384;

    /// Bootstrap a `Node`, i.e. create connections between itself and its
    /// peer nodes.
    ///
    /// Rogue messages (i.e. not pertaining to the bootstrapping protocol)
    /// are returned in a `Vec`.
    pub fn bootstrap(c: NodeConfig) -> Result<(Self, Vec<Message<O>>)> {
        if c.addrs.len() < (3*cfg.f + 1) {
            return Err("Invalid number of replicas")
                .wrapped(ErrorKind::Communication);
        }
        if c.id.into() >= c.addrs.len() {
            return Err("Invalid node ID")
                .wrapped(ErrorKind::Communication);
        }

        let id = c.id;
        let n = cfg.addrs.len();

        let listener = socket::bind(cfg.addrs[id]).await
            .wrapped(ErrorKind::Communication)?;

        let mut others_tx = HashMap::new();
        let (tx, mut rx) = new_message_channel(Self::CHAN_BOUND);

        // rx side (accept conns from replica)
        let tx_clone = tx.clone();
        rt::spawn(async move {
            let mut tx = tx_clone;
            let mut buf = [0; Header::LENGTH];
            loop {
                if let Ok(mut sock) = listener.accept().await {
                    // TODO: receive a header with an empty payload, verify
                    // the header signature, extract id from header
                    if let Err(_) = sock.read_exact(&mut buf[..]).await {
                        // errors reading -> faulty connection;
                        // drop this socket
                        continue;
                    }

                    // check if they are who they say who they are
                    let id = {
                        // we are passing the correct length, safe to use unwrap()
                        let h = Header::deserialize_from(&buf[..]).unwrap();
                        let wire_message = WireMessage::from_parts(h, &[]);
                        if !wire_message.is_valid(&pk) {
                            // invalid identity, drop connection
                            continue;
                        }
                        h.from()
                    };

                    if let Err(_) = tx.send(Message::ConnectedRx(id, sock)).await {
                        // if sending fails, the program terminated, so we exit
                        return;
                    }
                }
            }
        });

        // share the secret key between other tasks, but keep it in a
        // single memory location, with an `Arc`
        let sk = Arc::new(c.sk);

        // build peers map
        let mut peers = HashMap::new();
        for i in NodeId::targets(0..n) {
            peers[i] = PeerData {
                pk: c.pk[i],
                addr: c.addrs[i],
            }
        }

        // success
        let node = Node {
            id,
            peers,
            others_tx,
            my_tx,
            my_rx,
        };
        Ok((node, rogue))
    }
}
