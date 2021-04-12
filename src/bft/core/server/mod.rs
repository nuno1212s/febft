//! Contains the server side core protocol logic of `febft`.

use super::SystemParams;
use crate::bft::error::*;
use crate::bft::executable::Service;
use crate::bft::communication::{
    Node,
    NodeId,
};

/// This struct contains information related with an
/// active `febft` view.
#[derive(Copy, Clone)]
pub struct ViewInfo {
    leader: NodeId,
    params: SystemParams,
}

impl ViewInfo {
    /// Creates a new instance of `ViewInfo`.
    pub fn new(leader: NodeId, n: usize, f: usize) -> Result<Self> {
        if leader >= NodeId::from(n) {
            return Err("Invalid NodeId")
                .wrapped(ErrorKind::Core);
        }
        let params = SystemParams::new(n, f)?;
        Ok(ViewInfo { leader, params })
    }

    /// Returns a copy of this node's `SystemParams`.
    pub fn params(&self) -> &SystemParams {
        &self.params
    }

    /// Returns the leader of the current view.
    pub fn leader(&self) -> NodeId {
        self.leader
    }
}

/// Represents a replica in `febft`.
pub struct Replica<S: Service> {
    info: ViewInfo,
    node: Node<S::Data>,
}
