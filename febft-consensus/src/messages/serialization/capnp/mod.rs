use std::io::{Read, Write};
use febft_common::error::*;
use febft_execution::serialize::SharedData;
use febft_capnp::{consensus_messages_capnp, messages_capnp};
use febft_common::crypto::hash::Digest;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_communication::serialize::Buf;
use febft_messages::serialization::capnp as messages_capnp_impl;
use crate::messages::{ConsensusMessage, ConsensusMessageKind, ObserveEventKind, ObserverMessage, PBFTProtocolMessage};
use crate::sync::view::ViewInfo;

pub(super) fn serialize_protocol_message<W: Write, D: SharedData>(w:&mut W, msg: &PBFTProtocolMessage<D>) -> Result<()> {
    let allocator = capnp::message::HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let mut writer: consensus_messages_capnp::protocol_message::Builder = root.init_root();

    match msg {
        PBFTProtocolMessage::Consensus(consensus_msg) => {
            let consensus = writer.reborrow().init_consensus_message();

            serialize_consensus_message::<D>(consensus, consensus_msg)?;
        }
        PBFTProtocolMessage::FwdConsensus(fwd_consensus_msg) => {
            unreachable!()
        }
        PBFTProtocolMessage::Cst(cst_msg) => {
            unreachable!()
        }
        PBFTProtocolMessage::ViewChange(view_change_msg) => {
            unreachable!()
        }
        PBFTProtocolMessage::ObserverMessage(observer_message) => {
            let capnp_observer = writer.reborrow().init_observer_message();

            let mut obs_message_type = capnp_observer.init_message_type();

            match observer_message {
                ObserverMessage::ObserverRegister => {
                    obs_message_type.set_observer_register(());
                }
                ObserverMessage::ObserverRegisterResponse(response) => {
                    obs_message_type.set_observer_register_response(*response);
                }
                ObserverMessage::ObserverUnregister => {
                    obs_message_type.set_observer_unregister(());
                }
                ObserverMessage::ObservedValue(observed_value) => {
                    let observer_value_msg = obs_message_type.init_observed_value();

                    let mut value = observer_value_msg.init_value();

                    match observed_value {
                        ObserveEventKind::CheckpointStart(start) => {
                            value.set_checkpoint_start((*start).into());
                        }
                        ObserveEventKind::CheckpointEnd(end) => {
                            value.set_checkpoint_end((*end).into());
                        }
                        ObserveEventKind::Consensus(seq_no) => {
                            value.set_consensus((*seq_no).into());
                        }
                        ObserveEventKind::NormalPhase((view, seq)) => {
                            let mut normal_phase = value.init_normal_phase();

                            let mut view_info = normal_phase.reborrow().init_view();

                            view_info.set_view_num(view.sequence_number().into());
                            view_info.set_n(view.params().n() as u32);
                            view_info.set_f(view.params().f() as u32);

                            normal_phase.set_seq_num((*seq).into());
                        }
                        ObserveEventKind::ViewChangePhase => {
                            value.set_view_change(());
                        }
                        ObserveEventKind::CollabStateTransfer => {
                            value.set_collab_state_transfer(());
                        }
                        ObserveEventKind::Prepare(seq_no) => {
                            value.set_prepare((*seq_no).into());
                        }
                        ObserveEventKind::Commit(seq_no) => {
                            value.set_commit((*seq_no).into());
                        }
                        ObserveEventKind::Ready(seq) => {
                            value.set_ready((*seq).into());
                        }
                        ObserveEventKind::Executed(seq) => {
                            value.set_executed((*seq).into());
                        }
                    }
                }
            }
        }
    }

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

pub(super) fn deserialize_protocol_message<R: Read, D: SharedData>(r: R) -> Result<PBFTProtocolMessage<D>> {
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let message: consensus_messages_capnp::protocol_message::Reader = reader.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get protocol msg root")?;

    let which = message.which().wrapped(ErrorKind::CommunicationSerialize)?;

    return match which {
        consensus_messages_capnp::protocol_message::WhichReader::ConsensusMessage(Ok(consensus_msg)) => {
            Ok(PBFTProtocolMessage::Consensus(deserialize_consensus_message::<D>(consensus_msg)?))
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
        consensus_messages_capnp::protocol_message::WhichReader::ObserverMessage(Ok(observer_msg)) => {
            let message_type = observer_msg.get_message_type();

            let type_which = message_type
                .which()
                .wrapped(ErrorKind::CommunicationSerialize)?;

            let observer_msg = match type_which {
                consensus_messages_capnp::observer_message::message_type::ObserverRegister(()) => {
                    Ok(ObserverMessage::ObserverRegister)
                }
                consensus_messages_capnp::observer_message::message_type::ObserverUnregister(()) => {
                    Ok(ObserverMessage::ObserverUnregister)
                }
                consensus_messages_capnp::observer_message::message_type::ObserverRegisterResponse(
                    result,
                ) => Ok(ObserverMessage::ObserverRegisterResponse(result)),
                consensus_messages_capnp::observer_message::message_type::ObservedValue(Ok(obs_req)) => {
                    let which = obs_req
                        .get_value()
                        .which()
                        .wrapped(ErrorKind::CommunicationSerialize)?;

                    let observed_value = match which {
                        consensus_messages_capnp::observed_value::value::CheckpointStart(start) => {
                            Ok(ObserveEventKind::CheckpointStart(start.into()))
                        }
                        consensus_messages_capnp::observed_value::value::CheckpointEnd(end) => {
                            Ok(ObserveEventKind::CheckpointEnd(end.into()))
                        }
                        consensus_messages_capnp::observed_value::value::Consensus(seq) => {
                            Ok(ObserveEventKind::Consensus(seq.into()))
                        }
                        consensus_messages_capnp::observed_value::value::NormalPhase(Ok(phase)) => {
                            let view = phase.get_view().unwrap();

                            let view_seq: SeqNo = view.get_view_num().into();
                            let n: usize = view.get_n() as usize;
                            let f: usize = view.get_f() as usize;

                            let seq_num: SeqNo = phase.get_seq_num().into();

                            let view_info = ViewInfo::new(view_seq, n, f).unwrap();

                            Ok(ObserveEventKind::NormalPhase((view_info, seq_num)))
                        }
                        consensus_messages_capnp::observed_value::value::NormalPhase(Err(err)) => {
                            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
                        }
                        consensus_messages_capnp::observed_value::value::ViewChange(()) => {
                            Ok(ObserveEventKind::ViewChangePhase)
                        }
                        consensus_messages_capnp::observed_value::value::CollabStateTransfer(()) => {
                            Ok(ObserveEventKind::CollabStateTransfer)
                        }
                        consensus_messages_capnp::observed_value::value::Prepare(seq) => {
                            Ok(ObserveEventKind::Prepare(seq.into()))
                        }
                        consensus_messages_capnp::observed_value::value::Commit(seq) => {
                            Ok(ObserveEventKind::Commit(seq.into()))
                        }
                        consensus_messages_capnp::observed_value::value::Ready(seq) => {
                            Ok(ObserveEventKind::Ready(seq.into()))
                        }
                        consensus_messages_capnp::observed_value::value::Executed(seq) => {
                            Ok(ObserveEventKind::Executed(seq.into()))
                        }
                    }?;

                    Ok(ObserverMessage::ObservedValue(observed_value))
                }
                consensus_messages_capnp::observer_message::message_type::ObservedValue(Err(err)) => {
                    Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
                }
            }?;

            Ok(PBFTProtocolMessage::ObserverMessage(observer_msg))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ObserverMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
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
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get consensus msg root (from reader)")?;

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