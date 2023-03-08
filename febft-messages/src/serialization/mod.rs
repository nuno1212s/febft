use std::io::{Read, Write};
#[cfg(feature = "serialize_serde")]
use ::serde::{Deserialize, Serialize};
#[cfg(feature = "serialize_bincode")]
use ::bincode::{Encode, Decode, BorrowDecode};

use febft_common::error::*;
use febft_communication::message::NetworkMessageContent;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

#[cfg(feature = "serialize_serde")]
pub mod serde;

#[cfg(feature = "serialize_bincode")]
pub mod bincode;

/// The data abstraction for the protocol messages
/// Allows for protocols to implement their own messages, which will then be handled by FeBFT
pub trait ProtocolData {
    #[cfg(feature = "serialize_serde")]
    type Message: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_bincode")]
    type Message: Encode + Decode + for<'a> BorrowDecode<'a> + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    type Message: Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::consensus_messages_capnp::protocol_message::Builder, message: &Self::Message) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::consensus_messages_capnp::protocol_message::Reader) -> Result<Self::Message>;
}

impl<D, P> Serializable for SystemMessage<D, P> where D: SharedData, P: ProtocolData {
    type Message = SystemMessage<D, P>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_message_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> Result<()> {
        capnp::serialize_message_capnp(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_message_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> Result<Self::Message> {
        capnp::deserialize_message_capnp(reader)
    }
}
