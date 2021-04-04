use std::default::Default;
use std::io::{Read, Write};

use capnp::serialize;
use capnp::message::{
    Reader,
    Builder,
    Allocator,
    HeapAllocator,
    ReaderSegments,
};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

#[cfg(test)]
use crate::bft::communication::message::{
    ConsensusMessageKind,
    ConsensusMessage,
    RequestMessage,
};

#[cfg(test)]
use crate::bft::crypto::hash::Digest;

// FIXME: maybe use `capnp::message::ScratchSpaceHeapAllocator` instead of
// `capnp::message::HeapAllocator`; this requires some wrapper type for
// allocating messages, which is slightly annoying, but ultimately better
// for performance reasons
//
// e.g. `Serializer::new(scratch).serialize(...)`
//
// each task would have its own `Serializer` instance

/// Deserialize a wire message from a Cap'n'Proto segment reader.
pub trait FromCapnp: Sized {
    fn from_capnp<S>(reader: &Reader<S>) -> Result<SystemMessage<Self>>
    where
        S: ReaderSegments;
}

/// Serialize a wire message using a Cap'n'Proto segment builder.
pub trait ToCapnp: Sized {
    fn to_capnp<A>(m: SystemMessage<Self>, root: &mut Builder<A>) -> Result<()>
    where
        A: Allocator;
}

pub fn serialize_message<O: ToCapnp, W: Write>(mut w: W, m: SystemMessage<O>) -> Result<W> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    O::to_capnp(m, &mut root)?;
    serialize::write_message(&mut w, &root)
        .map(|_| w)
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub fn deserialize_message<O: FromCapnp, R: Read>(r: R) -> Result<SystemMessage<O>> {
    let reader = serialize::read_message(r, Default::default())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;
    O::from_capnp(&reader)
}

#[cfg(test)]
impl ToCapnp for () {
    fn to_capnp<A>(m: SystemMessage<()>, root: &mut Builder<A>) -> Result<()>
    where
        A: Allocator
    {
        let mut sys_msg: unit_capnp::system_message::Builder = root.init_root();
        match m {
            SystemMessage::Request(_) => sys_msg.set_request(()),
            SystemMessage::Consensus(m) => {
                let mut consensus = sys_msg.init_consensus();
                consensus.set_sequence_number(m.sequence_number());

                let mut message_kind = consensus.init_message_kind();
                match m.kind() {
                    ConsensusMessageKind::PrePrepare(digest) => {
                        let d = digest.as_ref();
                        let mut pre_prepare = message_kind.init_pre_prepare(d.len() as u32);
                        pre_prepare.copy_from_slice(d);
                    },
                    ConsensusMessageKind::Prepare => message_kind.set_prepare(()),
                    ConsensusMessageKind::Commit => message_kind.set_commit(()),
                }
            },
        }
        Ok(())
    }
}

#[cfg(test)]
impl FromCapnp for () {
    fn from_capnp<S>(reader: &Reader<S>) -> Result<SystemMessage<()>>
    where
        S: ReaderSegments
    {
        let sys_msg: unit_capnp::system_message::Reader = reader
            .get_root()
            .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get system message kind")?;
        let sys_msg_which = sys_msg
            .which()
            .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get system message kind")?;

        match sys_msg_which {
            unit_capnp::system_message::Which::Request(_) => Ok(SystemMessage::Request(RequestMessage::new(()))),
            unit_capnp::system_message::Which::Consensus(Ok(consensus)) => {
                let seq = consensus
                    .reborrow()
                    .get_sequence_number();
                let message_kind = consensus
                    .reborrow()
                    .get_message_kind()
                    .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get consensus message kind")?;
                let message_kind_which = message_kind
                    .which()
                    .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get consensus message kind")?;


                let kind = match message_kind_which {
                    unit_capnp::consensus_message_kind::Which::PrePrepare(Ok(digest_reader)) => {
                        let digest = Digest::from_bytes(digest_reader)
                            .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Invalid digest")?;
                        ConsensusMessageKind::PrePrepare(digest)
                    },
                    unit_capnp::consensus_message_kind::Which::PrePrepare(_) => {
                        return Err("Failed to read consensus message kind")
                            .wrapped(ErrorKind::CommunicationSerializeCapnp);
                    },
                    unit_capnp::consensus_message_kind::Which::Prepare(_) => ConsensusMessageKind::Prepare,
                    unit_capnp::consensus_message_kind::Which::Commit(_) => ConsensusMessageKind::Commit,
                };

                Ok(SystemMessage::Consensus(ConsensusMessage::new(seq, kind)))
            },
            unit_capnp::system_message::Which::Consensus(_) => {
                Err("Failed to read consensus message")
                    .wrapped(ErrorKind::CommunicationSerializeCapnp)
            },
        }
    }
}

#[cfg(test)]
mod unit_capnp {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/unit_capnp.rs"));
}
