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
    peer_addrs: HashMap<NodeId, SocketAddr>,
    peer_keys: Arc<HashMap<NodeId, PublicKey>>,
    others_tx: HashMap<NodeId, Arc<NodeTxData>>,
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

        let mut others_tx = HashMap::new();
        let peer_keys = Arc::new(cfg.pk);
        let (tx, mut rx) = new_message_channel::<O>(Self::CHAN_BOUND);

        // rx side (accept conns from replica)
        let tx_clone = tx.clone();
        let peer_keys_clone = Arc::clone(&peer_keys);
        rt::spawn(async move {
            let tx = tx_clone;
            let pk = peer_keys_clone;
            // TODO: check if we have terminated the node, and exit
            loop {
                if let Ok(mut sock) = listener.accept().await {
                    let mut tx = tx.clone();
                    let pk = Arc::clone(&pk);
                    rt::spawn(async move {
                        let mut buf = [0; Header::LENGTH];

                        if let Err(_) = sock.read_exact(&mut buf[..]).await {
                            // errors reading -> faulty connection;
                            // drop this socket
                            return;
                        }

                        // check if they are who they say who they are
                        let id = {
                            // we are passing the correct length, safe to use unwrap()
                            let header = Header::deserialize_from(&buf[..]).unwrap();
                            let id = header.from();

                            // try to extract the public key associated with the
                            // node in the header
                            let pk = match pk.get(&id) {
                                Some(pk) => pk,
                                None => return,
                            };

                            // use an empty slice since we aren't expecting a payload;
                            // errors will stem from sizes different than 0 in the header
                            match WireMessage::from_parts(header, &[]) {
                                Ok(wm) if !wm.is_valid(&pk) => {
                                    // invalid identity; drop connection
                                    return;
                                },
                                Ok(_) => id,
                                Err(_) => return,
                            }
                        };

                        tx.send(Message::ConnectedRx(id, sock)).await.unwrap_or(());
                    });
                }
            }
        });

        /*
        // share the secret key between other tasks, but keep it in a
        // single memory location, with an `Arc`
        let sk = Arc::new(cfg.sk);

        // build peers map
        let mut peers = HashMap::new();
        for i in NodeId::targets(0..n) {
            peers[i] = PeerData {
                pk: cfg.pk[i],
                addr: cfg.addrs[i],
            }
        }
        */

        // success
        let node = Node {
            id,
            others_tx,
            my_tx: tx,
            my_rx: rx,
            peer_keys,
            peer_addrs: cfg.addrs,
        };
        Ok((node, Vec::new()))
    }
}
