//! Module to manage timeouts in FeBFT.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

pub struct TimeoutsHandle {
    tx: (),
}
