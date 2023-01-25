//! Contains the core protocol logic of `febft`.

pub mod client;
pub mod server;
pub mod follower;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::error::*;

/// This struct contains the system parameters of
/// a replica or client in `febft`, i.e. `n` and `f`
/// such that `n >= 3*f + 1`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone)]
pub struct SystemParams {
    n: usize,
    f: usize,
}

impl SystemParams {
    
    /// Creates a new instance of `SystemParams`.
    pub fn new(n: usize, f: usize) -> Result<Self> {
        if n < 3 * f + 1 {
            return Err("Invalid params: n < 3f + 1")
                .wrapped(ErrorKind::Core);
        }
        
        Ok(SystemParams { n, f })
    }

    /// Returns the quorum size associated with these
    /// `SystemParams`.
    pub fn quorum(&self) -> usize {
        //2*self.f + 1
        //self.n - self.f
        (self.f << 1) + 1
    }

    /// Returns the `n` parameter.
    pub fn n(&self) -> usize {
        self.n
    }

    /// Returns the `f` parameter.
    pub fn f(&self) -> usize {
        self.f
    }
}
