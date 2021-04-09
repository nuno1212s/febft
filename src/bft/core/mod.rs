//! Where all the magic happens.
//!
//! Contains the core protocol logic of `febft`.

use crate::bft::executable::Service;
use crate::bft::communication::{
    Node,
    NodeId,
};

/// This struct contains information related with an
/// active `febft` view.
pub struct SystemInfo {
    leader: NodeId,
    n: usize,
    f: usize,
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service> {
    node: Node<S::Data>,
}
