use std::io::{Read, Write};
use febft_common::error::*;
use febft_execution::serialize::SharedData;
use febft_capnp::consensus_messages_capnp;
use febft_common::crypto::hash::Digest;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_communication::serialize::Buf;
use febft_messages::serialization::capnp as messages_capnp_impl;
use crate::messages::{ConsensusMessage, ConsensusMessageKind, ProtocolMessage};

pub(super) fn serialize_protocol_message<W: Write, D: SharedData>(w:&mut W, msg: &ProtocolMessage<D>) -> Result<()> {
    let allocator = capnp::message::HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let mut writer: consensus_messages_capnp::protocol_message::Builder = root.init_root();

    match msg {
        ProtocolMessage::Consensus(consensus_msg) => {
            let consensus = writer.reborrow().init_consensus_message();

            serialize_consensus_message::<D>(consensus, consensus_msg)?;
        }
        ProtocolMessage::FwdConsensus(fwd_consensus_msg) => {}
        ProtocolMessage::Cst(cst_msg) => {}
        ProtocolMessage::ViewChange(view_change_msg) => {}
        ProtocolMessage::ObserverMessage(ovbserver) => {}
    }

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

pub(super) fn deserialize_protocol_message<R: Read, D: SharedData>(r: R) -> Result<ProtocolMessage<D>> {
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let message: consensus_messages_capnp::protocol_message::Reader = reader.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get msg root")?;

    let which = message.which().wrapped(ErrorKind::CommunicationSerialize)?;

    return match which {
        consensus_messages_capnp::protocol_message::WhichReader::ConsensusMessage(Ok(consensus_msg)) => {
            Ok(ProtocolMessage::Consensus(deserialize_consensus_message::<D>(consensus_msg)?))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ConsensusMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ViewChangeMessage(view_change_msg) => {
            unreachable!()
        }
        consensus_messages_capnp::protocol_message:: WhichReader::StateTransferMessage(state_transfer_msg) => {
            unreachable!()
        }
    };

}

pub (super) fn serialize_consensus_to_writer<W: Write, D: SharedData>(w: &mut W, msg: &ConsensusMessage<D::Request>) -> Result<()> {

    let allocator = capnp::message::HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let mut writer: consensus_messages_capnp::consensus::Builder = root.init_root();

    serialize_consensus_message::<D>(writer, msg)?;

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

pub(super) fn deserialize_consensus_from_reader<R: Read, D: SharedData>(r: R) -> Result<ConsensusMessage<D::Request>> {

    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let message: consensus_messages_capnp::consensus::Reader = reader.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get msg root")?;

    deserialize_consensus_message::<D>(message)
}

fn serialize_consensus_message<D: SharedData>(mut msg_builder: consensus_messages_capnp::consensus::Builder,
                                              consensus: &ConsensusMessage<D::Request>) -> Result<()> {
    msg_builder.set_view(u32::from(consensus.view()));
    msg_builder.set_seq_no(u32::from(consensus.sequence_number()));

    match consensus.kind() {
        ConsensusMessageKind::PrePrepare(requests) => {
            let mut header = [0; Header::LENGTH];
            let mut pre_prepare_requests = msg_builder.init_pre_prepare(requests.len() as u32);

            for (i, stored) in requests.iter().enumerate() {
                let mut forwarded = pre_prepare_requests.reborrow().get(i as u32);

                // set header
                {
                    stored.header().serialize_into(&mut header[..]).unwrap();
                    forwarded.set_header(&header[..]);
                }

                // set request
                {
                    let mut request = forwarded.reborrow().init_request();

                    messages_capnp_impl::serialize_request_message::<D>(request, stored.message())?;
                }
            }
        }
        ConsensusMessageKind::Prepare(digest) => {
            msg_builder.set_prepare(digest.as_ref());
        }
        ConsensusMessageKind::Commit(digest) => {
            msg_builder.set_commit(digest.as_ref());
        }
    }

    Ok(())
}

fn deserialize_consensus_message<D: SharedData>(msg_reader: consensus_messages_capnp::consensus::Reader) -> Result<ConsensusMessage<D::Request>> {
    let view_no: SeqNo = SeqNo::from(msg_reader.get_view());
    let seq_no: SeqNo = SeqNo::from(msg_reader.get_seq_no());

    let kind = match msg_reader.which().wrapped(ErrorKind::CommunicationSerialize)? {
        consensus_messages_capnp::consensus::WhichReader::PrePrepare(Ok(pre_prepare)) => {
            let mut rqs = Vec::with_capacity(pre_prepare.len() as usize);

            for pre_prepare_rq in pre_prepare.iter() {
                let header: Vec<u8> = pre_prepare_rq
                    .get_header()
                    .wrapped(ErrorKind::CommunicationSerialize)?
                    .to_vec();

                let request = pre_prepare_rq
                    .get_request()
                    .wrapped(ErrorKind::CommunicationSerialize)?;

                let parsed_request = messages_capnp_impl::deserialize_request_message::<D>(request)?;

                rqs.push(StoredMessage::new(
                    Header::deserialize_from(&header[..])?,
                    parsed_request,
                ));
            }
            ConsensusMessageKind::PrePrepare(rqs)
        }
        consensus_messages_capnp::consensus::WhichReader::PrePrepare(Err(err)) => {
            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
        consensus_messages_capnp::consensus::WhichReader::Prepare(Ok(digest)) => {
            ConsensusMessageKind::Prepare(Digest::from_bytes(&digest[..])?)
        }
        consensus_messages_capnp::consensus::WhichReader::Prepare(Err(err)) => {
            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
        consensus_messages_capnp::consensus::WhichReader::Commit(Ok(digest)) => {
            ConsensusMessageKind::Commit(Digest::from_bytes(&digest[..])?)
        }
        consensus_messages_capnp::consensus::WhichReader::Commit(Err(err)) => {
            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
    };

    Ok(ConsensusMessage::new(seq_no, view_no, kind))
}