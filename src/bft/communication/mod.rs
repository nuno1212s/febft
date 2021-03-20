#[cfg(not(feature = "expose_impl"))]
mod socket;

#[cfg(feature = "expose_impl")]
pub mod socket;

pub mod serialize;
pub mod message;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

//use crate::bft::communication::socket::Socket;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct NodeId(u32);

impl From<NodeId> for usize {
    #[inline]
    fn from(id: NodeId) -> usize {
        id.0 as usize
    }
}

//pub struct Node;

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
