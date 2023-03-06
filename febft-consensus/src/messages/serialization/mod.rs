use std::io::{Read, Write};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::{ConsensusMessage, ProtocolMessage};

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

impl<D> Serializable for ProtocolMessage<D> where D: SharedData {
    type Message = ProtocolMessage<D>;

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()> {
        #[cfg(feature = "serialize_capnp")]
        capnp::serialize_protocol_message(w, message)
    }

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message> {
        #[cfg(feature = "serialize_capnp")]
        capnp::deserialize_protocol_message(r)
    }
}

pub(crate) fn serialize_consensus_message<W: Write, D: SharedData>(w: &mut W, msg: &ConsensusMessage<D::Request>) -> Result<()> {
    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_consensus_to_writer::<W, D>(w, msg)
}

pub(crate) fn deserialize_consensus_message<R: Read, D: SharedData>(r: R) -> Result<ConsensusMessage<D::Request>> {
    #[cfg(feature = "serialize_capnp")]
    capnp::deserialize_consensus_from_reader::<R, D>(r)
}