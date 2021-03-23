#[cfg(not(feature = "expose_impl"))]
mod socket;

#[cfg(feature = "expose_impl")]
pub mod socket;

pub mod serialize;
pub mod message;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use std::sync::Arc;
use std::net::SocketAddr;
use std::collections::HashMap;

use futures::lock::Mutex;

use crate::bft::communication::socket::Socket;

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

pub struct Node {
    id: NodeId,
    peer_addrs: Vec<SocketAddr>,
    others_tx: HashMap<NodeId, Arc<Mutex<Socket>>>,
}

// Add more backends:
// ==================
// #[cfg(feature = "foo_bar_backend")]
// pub struct Node(...);
//
// ...

//impl Node {
//    pub fn id(&self) -> NodeId {
//        self.0.id()
//    }
//
//    pub async fn send(&self, ctx: &Context, 
//}
