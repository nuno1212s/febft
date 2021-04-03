//! This module is responsible for serializing wire messages in `febft`.
//!
//! If using the [Cap'n'Proto](https://capnproto.org/capnp-tool.html) backend,
//! the API for the module will be slightly different. Users can opt to enable
//! the [serde](https://serde.rs/) backend instead, which has a much more flexible
//! API, but performs worse, in general, because it doesn't have a
//! zero-copy architecture, like Cap'n'Proto.
//!
//! Configuring one over the other is done with the following feature flags:
//!
//! - `serialize_capnp`
//! - `serialize_serde_BACKEND`, where `BACKEND` may be `bincode`, for instance.
//!   Consult the `Cargo.toml` file for more alternatives.

#[cfg(feature = "serialize_capnp")]
mod capnp;

#[cfg(feature = "serialize_serde")]
mod serde;

#[cfg(feature = "serialize_serde")]
use ::serde::{Serialize, Deserialize};

use std::io::{Read, Write};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

#[cfg(feature = "serialize_capnp")]
pub use self::capnp::{ToCapnp, FromCapnp};

/// Serialize a wire message into the writer `W`.
///
/// Once the operation is finished, the buffer is returned.
#[cfg(feature = "serialize_capnp")]
pub fn serialize_message<O: ToCapnp, W: Write>(w: W, m: SystemMessage<O>) -> Result<W> {
    capnp::serialize_message(w, m)
}

/// Serialize a wire message into the write `W`.
///
/// Once the operation is finished, the buffer is returned.
#[cfg(feature = "serialize_serde")]
pub fn serialize_message<O, W>(w: W, m: SystemMessage<O>) -> Result<W>
where
    O: Serialize,
    W: Write,
{
    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::serialize_message(w, m) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::serialize_message(w, m) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::serialize_message(w, m) }
}

/// Deserialize a wire message from a read `B`.
#[cfg(feature = "serialize_capnp")]
pub fn deserialize_message<O: FromCapnp, R: Read>(r: R) -> Result<SystemMessage<O>> {
    capnp::deserialize_message(r)
}

/// Deserialize a wire message from a reader `R`.
#[cfg(feature = "serialize_serde")]
pub fn deserialize_message<O, R>(r: R) -> Result<SystemMessage<O>>
where
    O: for<'de> Deserialize<'de>,
    R: Read,
{
    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::deserialize_message(r) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::deserialize_message(r) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::deserialize_message(r) }
}

////////////////////////////////////////////////////////////
//
//    WARNING !! !! !!
//    ================
//
//    gore below :-)
//
////////////////////////////////////////////////////////////

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_serde")]
pub trait Marshal: Serialize {}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_capnp")]
pub trait Marshal: ToCapnp {}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_serde")]
pub trait Unmarshal: for<'de> Deserialize<'de> {}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_capnp")]
pub trait Unmarshal: FromCapnp {}

#[cfg(feature = "serialize_serde")]
impl<T: Serialize> Marshal for T {}

#[cfg(feature = "serialize_capnp")]
impl<T: ToCapnp> Marshal for T {}

#[cfg(feature = "serialize_serde")]
impl<T: for<'de> Deserialize<'de>> Unmarshal for T {}

#[cfg(feature = "serialize_capnp")]
impl<T: FromCapnp> Unmarshal for T {}
