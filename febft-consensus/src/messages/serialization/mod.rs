use std::io::{Read, Write};
use febft_capnp::messages_capnp::system::{Builder, Reader};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use febft_messages::serialization::ProtocolData;
use crate::messages::{ConsensusMessage, PBFTProtocolMessage};

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

impl<D> ProtocolData for PBFTProtocolMessage<D> where D: SharedData {

    type Message = PBFTProtocolMessage<D>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::consensus_messages_capnp::protocol_message::Builder,
                       message: &Self::Message) -> Result<()> {
        capnp::serialize_protocol_message_builder(builder, message)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::consensus_messages_capnp::protocol_message::Reader) -> Result<Self::Message> {
        capnp::deserialize_protocol_message_reader(reader)
    }
}

pub(crate) fn serialize_consensus_message<W: Write, D: SharedData>(w: &mut W, msg: &ConsensusMessage<D::Request>) -> Result<()> {
    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_consensus_to_writer::<W, D>(w, msg)?;

    Ok(())
}

pub(crate) fn deserialize_consensus_message<R: Read, D: SharedData>(r: R) -> Result<ConsensusMessage<D::Request>> {
    #[cfg(feature = "serialize_capnp")]
    let result = capnp::deserialize_consensus_from_reader::<R, D>(r)?;

    Ok(result)
}