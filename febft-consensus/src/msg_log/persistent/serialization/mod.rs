use std::io::{Read, Write};
use std::mem::size_of;
use febft_common::error::*;
use febft_common::ordering::SeqNo;
use febft_communication::NodeId;
use febft_execution::serialize::SharedData;
use crate::messages;
use crate::messages::ConsensusMessage;
use crate::msg_log::persistent::ProofInfo;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

/// The persister trait, to add functionality to the Shared Data struct.
/// The functionality added is ease of serialization for individual consensus messages, which
/// are then going to be stored in the persistent log.
pub trait Persister: SharedData + Sized {
    fn serialize_consensus_message<W: Write>(
        message: &ConsensusMessage<Self::Request>,
        w:&mut W,
    ) -> Result<()> {
        messages::serialization::serialize_consensus_message::<W, Self>(w, message)
    }

    fn deserialize_consensus_message<R: Read>(
        r: R
    ) -> Result<ConsensusMessage<Self::Request>> {
        messages::serialization::deserialize_consensus_message::<R, Self>(r)
    }
}

pub(super) fn make_proof_info(pi: &ProofInfo) -> Result<Vec<u8>>
{
    let mut final_vec = Vec::new();

    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_proof_info(&mut final_vec, pi)?;

    #[cfg(not(feature = "capnp-serialization"))]
    unreachable!();

    Ok(final_vec)
}

pub(super) fn read_seq<R>(r: R) -> Result<SeqNo> where R: Read {
    #[cfg(feature = "serialize_capnp")]
    return capnp::read_seq(r);

    #[cfg(not(feature = "serialize_capnp"))]
    unreachable!();
}

pub(super) fn make_seq(seq: SeqNo) -> Result<Vec<u8>> {
    let mut seq_no = Vec::with_capacity(size_of::<SeqNo>());

    #[cfg(feature = "serialize_capnp")]
    capnp::write_seq(&mut seq_no, seq)?;
    #[cfg(not(feature = "serialize_capnp"))]
    unreachable!();

    Ok(seq_no)
}

pub(super) fn make_message_key(seq: SeqNo, from: Option<NodeId>) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(size_of::<SeqNo>() + size_of::<NodeId>());

    #[cfg(feature = "serialize_capnp")]
    capnp::write_message_key(&mut key, seq, from)?;

    #[cfg(not(feature = "serialize_capnp"))]
    unreachable!();

    Ok(key)
}


pub fn deserialize_proof_info<R>(reader: R) -> Result<ProofInfo> where R: Read {
    #[cfg(feature = "serialize_capnp")]
    return capnp::deserialize_proof_info(reader);

    #[cfg(not(feature = "serialize_capnp"))]
    unreachable!();
}

impl<D: SharedData> Persister for D {}