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
///
/// Both clients and replicas should implement this trait,
/// to communicate with each other.
pub trait SharedData {
    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    type Request;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    type Reply;

    /// Serialize a wire message into the writer `W`.
    fn serialize_message<W>(w: W, m: &SystemMessage<Self::Request, Self::Reply>) -> Result<()>
    where
        W: Write;

    /// Deserialize a wire message from a reader `R`.
    fn deserialize_message<R>(r: R) -> Result<SystemMessage<Self::Request, Self::Reply>>
    where
        R: Read;
}

/// Extension of `SharedData`, pertaining solely to replicas.
pub trait ReplicaData: SharedData {
    /// The application state, which is mutated by client
    /// requests.
    type State;
}

/// Extension of `SharedData`, pertaining solely to clients.
pub trait ClientData: SharedData {}

impl<D: SharedData> ClientData for D {}
