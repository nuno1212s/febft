//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};

use smallvec::SmallVec;

use crate::bft::error::*;
use crate::bft::crypto::hash::{Context, Digest};
use crate::bft::communication::message::SystemMessage;

/// Marker trait containing the types used by the application,
/// as well as routines to serialize the application data.
///
/// Both clients and replicas should implement this trait,
/// to communicate with each other.
pub trait SharedData {
    /// The application state, which is mutated by client
    /// requests.
    type State;

    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    type Request;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    type Reply;

    /// Serialize a wire message into the writer `W`.
    fn serialize_message<W>(w: W, m: &SystemMessage<Self>) -> Result<()>
    where
        W: Write;

    /// Deserialize a wire message from a reader `R`.
    fn deserialize_message<R>(r: R) -> Result<SystemMessage<Self>>
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
const NODE_BUFSIZ: usize = 16384;

/// The buffer type used to serialize messages into.
pub type Buf = SmallVec<[u8; NODE_BUFSIZ]>;

/// Extension of `SharedData` to obtain hash digests.
pub trait DigestData: SharedData {
    /// Convenience function to obtain the digest of a request upon
    /// serialization.
    fn serialize_digest<W: Write + AsRef<[u8]>>(
        nonce: u64,
        message: &SystemMessage<Self>,
        mut w: W,
    ) -> Result<Digest> {
        Self::serialize_message(&mut w, message)?;
        let mut ctx = Context::new();
        let nonce = nonce.to_le_bytes();
        ctx.update(&nonce[..]);
        ctx.update(w.as_ref());
        Ok(ctx.finish())
    }
}

impl<D: SharedData> DigestData for D {}
