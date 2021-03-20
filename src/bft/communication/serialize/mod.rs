#[cfg(feature = "serialize_capnp")]
mod capnp;

#[cfg(feature = "serialize_serde")]
mod serde;

use bytes::{Buf, BufMut};

use crate::bft::error::*;
use crate::bft::communication::message::ReplicaMessage;

pub fn serialize_to_replica<B: BufMut>(buf: B, m: ReplicaMessage) -> Result<B> {
    #[cfg(feature = "serialize_capnp")]
    { capnp::serialize_to_replica(buf, m) }

    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::serialize_to_replica(buf, m) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::serialize_to_replica(buf, m) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::serialize_to_replica(buf, m) }
}

pub fn deserialize_from_replica<B: Buf>(buf: B) -> Result<ReplicaMessage> {
    #[cfg(feature = "serialize_capnp")]
    { capnp::deserialize_from_replica(buf) }

    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::deserialize_from_replica(buf) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::deserialize_from_replica(buf) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::deserialize_from_replica(buf) }
}
