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
    id: NodeId,
    peer_addrs: Vec<SocketAddr>,
    others_tx: HashMap<NodeId, Arc<Mutex<Socket>>>,
    my_tx: MessageChannelTx<O>,
    my_rx: MessageChannelTx<O>,
}

pub struct NodeConfig;

impl<O> Node<O> {
    /// Bootstrap a `Node`, i.e. create connections between itself and its
    /// peer nodes.
    pub fn bootstrap(c: NodeConfig) -> Result<Self> {
        unimplemented!()
    }
}
