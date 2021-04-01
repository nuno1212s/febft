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

use futures::lock::Mutex;

use crate::bft::error::*;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::channel::{
    MessageChannelTx,
    MessageChannelRx,
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

/// A `Node` contains handles to other processes in the system, and is
/// the core component used in the wire communication between processes.
pub struct Node<O> {
    config: Arc<NodeConfig>,
    others_tx: HashMap<NodeId, Arc<Mutex<Socket>>>,
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
        self.pk[&i]
    }
}

impl<O> Node<O> {
    /// Bootstrap a `Node`, i.e. create connections between itself and its
    /// peer nodes.
    pub fn bootstrap(c: NodeConfig) -> Result<Self> {
        unimplemented!()
        //if c.addrs.len() < (3*cfg.f + 1) {
        //    return Err("Invalid number of replicas")
        //        .wrapped(ErrorKind::Communication);
        //}
        //if c.id.into() >= c.addrs.len() {
        //    return Err("Invalid node ID")
        //        .wrapped(ErrorKind::Communication);
        //}
        //let id = 
    }
}
