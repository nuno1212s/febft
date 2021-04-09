//! Where all the magic happens.
//!
//! Contains the core protocol logic of `febft`.

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

/// Represents a replica in `febft`.
pub struct Replica<S: Service> {
    info: ViewInfo,
    node: Node<S::Data>,
}

/// This struct contains the system parameters of
/// a replica or client in `febft`, i.e. `n` and `f`
/// such that `n >= 3*f + 1`.
#[derive(Copy, Clone)]
pub struct SystemParams {
    n: usize,
    f: usize,
}

impl SystemParams {
    /// Creates a new instance of `SystemParams`.
    pub fn new(n: usize, f: usize) -> Result<Self> {
        if n < 3*f + 1 {
            return Err("Invalid params: n < 3f + 1")
                .wrapped(ErrorKind::Core);
        }
        Ok(SystemParams { n, f })
    }

    /// Returns the quorum size associated with these
    /// `SystemParams`.
    pub fn quorum(&self) -> usize {
        2*self.f + 1
    }
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
    pub fn params(&self) -> SystemParams {
        self.params
    }

    /// Returns the leader of the current view.
    pub fn leader(&self) -> NodeId {
        self.leader
    }
}
