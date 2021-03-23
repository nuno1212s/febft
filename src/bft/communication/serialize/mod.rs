#[cfg(feature = "serialize_capnp")]
mod capnp;

#[cfg(feature = "serialize_serde")]
mod serde;

#[cfg(feature = "serialize_serde")]
use ::serde::{Serialize, Deserialize};

use bytes::{Buf, BufMut};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

#[cfg(feature = "serialize_capnp")]
use capnp::{ToCapnp, FromCapnp};

#[cfg(feature = "serialize_capnp")]
pub fn serialize_message<O: ToCapnp, B: BufMut>(buf: B, m: SystemMessage<O>) -> Result<B> {
    capnp::serialize_message(buf, m)
}

#[cfg(feature = "serialize_serde")]
pub fn serialize_message<O, B>(buf: B, m: SystemMessage<O>) -> Result<B>
where
    O: Serialize,
    B: BufMut,
{
    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::serialize_message(buf, m) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::serialize_message(buf, m) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::serialize_message(buf, m) }
}

#[cfg(feature = "serialize_capnp")]
pub fn deserialize_message<O: FromCapnp, B: Buf>(buf: B) -> Result<SystemMessage<O>> {
    capnp::deserialize_message(buf)
}

#[cfg(feature = "serialize_serde")]
pub fn deserialize_message<O, B>(buf: B) -> Result<SystemMessage<O>>
where
    O: for<'de> Deserialize<'de>,
    B: Buf,
{
    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::deserialize_message(buf) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::deserialize_message(buf) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::deserialize_message(buf) }
}
