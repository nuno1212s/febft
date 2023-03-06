use std::io::{Read, Write};
use febft_common::error::*;
use febft_capnp::objects_capnp;
use ::capnp::message::{Builder, HeapAllocator};
use ::capnp::serialize;
use febft_common::crypto::hash::Digest;
use febft_common::ordering::SeqNo;
use febft_communication::NodeId;
use crate::msg_log::persistent::ProofInfo;

pub(super) fn write_seq<W>(w: &mut W, seq: SeqNo) -> Result<()> where W: Write {
    let mut root = Builder::new(HeapAllocator::new());

    let mut seq_no: objects_capnp::seq::Builder = root.init_root();

    seq_no.set_seq_no(seq.into());

    serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::MsgLogPersistentSerialization,
        "Failed to serialize using capnp",
    )
}

pub(super) fn write_message_key<W>(w: &mut W, seq: SeqNo, from: Option<NodeId>) -> Result<()> where W: Write {
    let mut root = Builder::new(HeapAllocator::new());

    let mut msg_key: objects_capnp::message_key::Builder = root.init_root();

    let mut msg_seq_builder = msg_key.reborrow().init_msg_seq();

    msg_seq_builder.set_seq_no(seq.into());

    let mut msg_from = msg_key.reborrow().init_from();

    msg_from.set_node_id(from.unwrap_or(NodeId(0)).into());

    serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::MsgLogPersistentSerialization,
        "Failed to serialize using capnp",
    )
}


pub(super) fn read_seq<R>(r: R) -> Result<SeqNo> where R: Read {
    let reader = serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::MsgLogPersistentSerialization,
        "Failed to get capnp reader",
    )?;

    let seq_no: objects_capnp::seq::Reader = reader.get_root().wrapped_msg(
        ErrorKind::MsgLogPersistentSerialization,
        "Failed to get system msg root",
    )?;

    Ok(SeqNo::from(seq_no.get_seq_no()))
}

pub(super) fn serialize_proof_info<W>(w: &mut W, proof: &ProofInfo) -> Result<()>
    where W: Write {
    let mut root = Builder::new(HeapAllocator::new());

    let mut proof_info: objects_capnp::proof_info::Builder = root.init_root();

    {
        let mut batch_digest = proof_info.reborrow().init_batch_digest();

        batch_digest.set_digest(proof.batch_digest.as_ref());
    }

    {
        let mut ordering_builder = proof_info.reborrow().init_batch_ordering(proof.pre_prepare_ordering.len() as u32);

        for (i, digest) in proof.pre_prepare_ordering.iter().enumerate() {
            let mut batch_digest = ordering_builder.reborrow().get(i as u32);

            batch_digest.set_digest(digest.as_ref());
        }
    }

    serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::MsgLogPersistentSerialization,
        "Failed to serialize using capnp",
    )
}

pub(super) fn deserialize_proof_info<R>(reader: R) -> Result<ProofInfo> where R: Read {
    let reader = serialize::read_message(reader, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let proof_info: objects_capnp::proof_info::Reader = reader.get_root().wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get system msg root",
    )?;

    let batch_digest_reader = proof_info.get_batch_digest().wrapped(ErrorKind::MsgLogPersistentSerialization)?;

    let digest_bytes = batch_digest_reader.get_digest().wrapped(ErrorKind::MsgLogPersistentSerialization)?
        .to_vec();

    let batch_digest = Digest::from_bytes(&digest_bytes[..])?;

    let order_vec_reader = proof_info.get_batch_ordering().wrapped(ErrorKind::MsgLogPersistentSerialization)?;

    let mut batch_ordering = Vec::with_capacity(order_vec_reader.len() as usize);

    for i in 0..order_vec_reader.len() {
        let digest = order_vec_reader.get(i);

        let digest_bytes = digest.get_digest().wrapped(ErrorKind::MsgLogPersistentSerialization)?
            .to_vec();

        let b_digest = Digest::from_bytes(&digest_bytes[..])?;

        batch_ordering.push(b_digest);
    }

    Ok(ProofInfo {
        batch_digest,
        pre_prepare_ordering: batch_ordering,
    })
}