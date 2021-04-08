//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

/// Marker trait containing the types used by the application,
/// as well as routines to serialize the application data.
pub trait Data {
    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    type Request;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    type Reply;

    /// The application state, which is mutated by client
    /// requests.
    type State;

    /// Serialize a wire message into the writer `W`.
    pub fn serialize_message<W>(w: W, m: &SystemMessage<Self::Request, Self::Reply>) -> Result<()>
    where
        W: Write;

    /// Deserialize a wire message from a reader `R`.
    pub fn deserialize_message<O, P, R>(r: R) -> Result<SystemMessage<Self::Request, Self::Reply>>
    where
        R: Read;
}
