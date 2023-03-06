use std::io::{Read, Write};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use crate::messages::{ConsensusMessage, ProtocolMessage};

impl<S, O> Serializable for ProtocolMessage<S, O> where S: Send + Clone, O: Send + Clone {
    type Message = ProtocolMessage<S, O>;

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()> {
        todo!()
    }

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message> {
        todo!()
    }
}

pub fn serialize_consensus_message<W: Write, O>(w: &mut W, message: &ConsensusMessage<O>) -> Result<()> {
    todo!()
}

pub fn deserialize_consensus_message<R: Read, O>(r: R) -> Result<ConsensusMessage<O>> {
    todo!()
}