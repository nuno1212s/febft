// TODO: create a trait for capnp serialization
// * serialize takes (SystemMessage, capnp::message::Builder) and returns ()
// * deserialize takes (capnp::message::Builder) and returns SystemMessage
// * no reads or writes performed by the trait

#[cfg(feature = "serialize_capnp")]
mod capnp;

#[cfg(feature = "serialize_serde")]
mod serde;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use bytes::{Buf, BufMut};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

#[cfg(feature = "serialize_capnp")]
pub fn serialize_message<O, B: BufMut>(buf: B, m: SystemMessage<O>) -> Result<B> {
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
pub fn deserialize_message<O, B: Buf>(buf: B) -> Result<SystemMessage<O>> {
    capnp::deserialize_message(buf)
}

#[cfg(feature = "serialize_serde")]
pub fn deserialize_message<'de, O, B>(buf: B) -> Result<SystemMessage<O>>
where
    O: Deserialize<'de>,
    B: Buf,
{
    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::deserialize_message(buf) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::deserialize_message(buf) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::deserialize_message(buf) }
}
