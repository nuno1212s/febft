//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};

use crate::bft::error::*;
use crate::bft::crypto::hash::{Context, Digest};
use crate::bft::communication::message::SystemMessage;

/// Marker trait containing the types used by the application,
/// as well as routines to serialize the application data.
///
/// Both clients and replicas should implement this trait,
/// to communicate with each other.
/// This data type must be Send since it will be sent across
/// threads for processing and follow up reception
pub trait SharedData : Send {
    /// The application state, which is mutated by client
    /// requests.
    type State: Send + Clone;

    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    type Request: Send + Clone;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    type Reply: Send + Clone;

    /// Serialize a wire message into the writer `W`.
    fn serialize_message<W>(w: W, m: &SystemMessage<Self::State, Self::Request, Self::Reply>) -> Result<()>
    where
        W: Write;

    /// Deserialize a wire message from a reader `R`.
    fn deserialize_message<R>(r: R) -> Result<SystemMessage<Self::State, Self::Request, Self::Reply>>
    where
        R: Read;

    /// Serialize the replica state into the writer `W`.
    fn serialize_state<W>(w: W, s: &Self::State) -> Result<()>
    where
        W: Write;

    /// Deserialize the replica state from a reader `R`.
    fn deserialize_state<R>(r: R) -> Result<Self::State>
    where
        R: Read;
}

// max no. of bytes to inline before doing a heap alloc
//const NODE_BUFSIZ: usize = 16384;

/// The buffer type used to serialize messages into.
pub type Buf = Vec<u8>;

/// Extension of `SharedData` to obtain hash digests.
pub trait DigestData: SharedData {
    /// Convenience function to obtain the digest of a request upon
    /// serialization.
    fn serialize_digest<W: Write + AsRef<[u8]>>(
        message: &SystemMessage<Self::State, Self::Request, Self::Reply>,
        mut w: W,
    ) -> Result<Digest> {
        Self::serialize_message(&mut w, message)?;
        let mut ctx = Context::new();
        ctx.update(w.as_ref());
        Ok(ctx.finish())
    }
}

impl<D: SharedData> DigestData for D {}
