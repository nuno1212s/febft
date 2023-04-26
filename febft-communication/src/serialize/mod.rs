//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};
use bytes::Bytes;
use febft_common::error::*;

#[cfg(feature = "serialize_serde")]
use ::serde::{Deserialize, Serialize};

use febft_common::crypto::hash::{Context, Digest};
use crate::message::{NetworkMessage, NetworkMessageKind};

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

#[cfg(feature = "serialize_serde")]
pub mod serde;

/// The buffer type used to serialize messages into.
pub type Buf = Bytes;

pub fn serialize_message<W, M>(w: &mut W, msg: &NetworkMessageKind<M>) -> Result<()>
    where W: Write + AsRef<[u8]> + AsMut<[u8]>, M: Serializable {
    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_message::<W, M>(w, msg)?;

    #[cfg(feature = "serialize_serde")]
    serde::serialize_message::<W, M>(msg, w)?;

    Ok(())
}

pub fn deserialize_message<R, M>(r: R) -> Result<NetworkMessageKind<M>>
    where R: Read + AsRef<[u8]>, M: Serializable {
    #[cfg(feature = "serialize_capnp")]
        let result = capnp::deserialize_message::<R, M>(r)?;

    #[cfg(feature = "serialize_serde")]
        let result = serde::deserialize_message::<R, M>(r)?;

    Ok(result)
}

pub fn serialize_digest<W, M>(message: &NetworkMessageKind<M>, w: &mut W, ) -> Result<Digest>
    where W: Write + AsRef<[u8]> + AsMut<[u8]>, M: Serializable {
    serialize_message::<W, M>(w, message)?;

    let mut ctx = Context::new();
    ctx.update(w.as_ref());
    Ok(ctx.finish())
}


/// The trait that should be implemented for all systems which wish to use this communication method
pub trait Serializable {
    #[cfg(feature = "serialize_capnp")]
    type Message: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type Message: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> Result<Self::Message>;
}

