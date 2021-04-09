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
pub struct ViewInfo {
    leader: NodeId,
    params: SystemParams,
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service> {
    info: ViewInfo,
    node: Node<S::Data>,
}

/// This struct contains the system parameters of
/// a replica or client in `febft`, i.e. `n` and `f`
/// such that `n >= 3*f + 1`.
pub struct SystemParams {
    n: usize,
    f: usize,
}
