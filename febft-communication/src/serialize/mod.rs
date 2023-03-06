//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};
#[cfg(feature = "serialize_bincode")]
use ::bincode::{Decode, Encode, BorrowDecode};
#[cfg(feature = "serialize_serde")]
use ::serde::{Serialize, Deserialize};
use bytes::Bytes;
use febft_common::crypto::hash::{Context, Digest};
use febft_common::error::*;

use crate::message::{NetworkMessage, NetworkMessageContent};

/*#[cfg(feature = "serialize_serde")]
pub mod serde;

#[cfg(feature = "serialize_bincode")]
pub mod bincode;*/

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

pub fn serialize_message<T, W>(
    m: NetworkMessageContent<T>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsRef<[u8]>,
    T: Serializable {

    /*
    #[cfg(feature="serialize_bincode")]
    bincode::serialize_message(&m, w)?;

    #[cfg(feature="serialize_serde")]
    serde::serialize_message(&m, w)?; */

    #[cfg(feature="serialize_capnp")]
    capnp::serialize_message(&m , w)?;

    Ok(())
}

pub fn serialize_digest_message<T, W>(
    m: &NetworkMessageContent<T>,
    w: &mut W,
) -> Result<Digest>
    where W: Write + AsRef<[u8]>,
          T: Serializable {
    match m {
        NetworkMessageContent::System(message) => {
            <T as DigestSerializable>::serialize_digest(message, w)
        }
        NetworkMessageContent::Ping(ping) => {
            let mut ctx = Context::new();

            Ok(ctx.finish())
        }
    }
}

pub fn deserialize_message<T, R>(r: R) -> Result<NetworkMessageContent<T>> where R: Read, T: Serializable {

    /*#[cfg(feature="serialize_bincode")]
    let content = bincode::deserialize_message(r)?;

    #[cfg(feature="serialize_serde")]
    let content = serde::deserialize_message(r)?; */

    #[cfg(feature = "serialize_capnp")]
    let content = capnp::deserialize_message(r)?;

    Ok(content)
}

// max no. of bytes to inline before doing a heap alloc
//const NODE_BUFSIZ: usize = 16384;

/// The buffer type used to serialize messages into.
pub type Buf = Bytes;

pub trait Serializable {
    /*#[cfg(feature = "serialize_bincode")]
    type Message: Encode + Decode + for<'a> BorrowDecode<'a> + Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type Message: for<'a> Deserialize<'a> + Serialize + Send + Clone; */

    #[cfg(feature = "serialize_capnp")]
    type Message: Send + Clone;

    fn serialize<W: Write>(
        w: &mut W,
        message: &Self::Message,
    ) -> Result<()>;

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message>;
}

pub trait DigestSerializable: Serializable {
    /// Extension of `SharedData` to obtain hash digests.
    /// Convenience function to obtain the digest of a request upon
    /// serialization.
    fn serialize_digest<W: Write + AsRef<[u8]>>(
        message: &Self::Message,
        w: &mut W,
    ) -> Result<Digest> {
        Self::serialize(w, message)?;

        let mut ctx = Context::new();
        ctx.update(w.as_ref());
        Ok(ctx.finish())
    }
}

impl<D: Serializable> DigestSerializable for D {}

