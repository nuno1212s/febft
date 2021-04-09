//! Where all the magic happens.
//!
//! Contains the core protocol logic of `febft`.

use crate::bft::communication::NodeId;

/// This struct contains information related with an
/// active `febft` view.
pub struct SystemInfo {
    leader: NodeId,
    n: usize,
    f: usize,
}
