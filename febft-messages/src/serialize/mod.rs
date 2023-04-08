use std::marker::PhantomData;
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;
#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

//We do not need a serde module since serde serialization is just done on the network level.

pub trait OrderingProtocolMessage {
    #[cfg(feature = "serialize_capnp")]
    type ProtocolMessage: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type ProtocolMessage: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::consensus_messages_capnp::protocol_message::Builder, msg: &Self::ProtocolMessage) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::consensus_messages_capnp::protocol_message::Reader) -> Result<Self::ProtocolMessage>;
}

/// The type that encapsulates all the serializing, so we don't have to constantly use SystemMessage
pub struct System<D: SharedData, P: OrderingProtocolMessage>(PhantomData<D>, PhantomData<P>);

impl<D: SharedData, P: OrderingProtocolMessage> Serializable for System<D, P> {

    type Message = SystemMessage<D, P::ProtocolMessage>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> Result<()> {
        capnp::serialize_message::<D, P>(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> Result<Self::Message> {
        capnp::deserialize_message::<D, P>(reader)
    }
}