use std::io::Read;
use std::io::Write;
use bytes::BytesMut;
use capnp::message::ReaderOptions;
use atlas_capnp::{consensus_messages_capnp, messages_capnp};
use atlas_common::crypto::hash::Digest;

use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, PingMessage, StoredMessage};
use atlas_core::serialize::capnp::deserialize_request;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind,  ObserveEventKind, ObserverMessage, PBFTMessage, RequestMessage, ViewChangeMessage};

use crate::bft::msg_log::persistent::ProofInfo;
use crate::bft::sync::view::ViewInfo;

use super::{Buf, SharedData};

/// This module is meant to handle the serialization of the SMR messages, to allow for the users of this library to only
/// Have to serialize (and declare) their request, reply and state, instead of also having to do so for all message
/// types of the SMR protocol.

const DEFAULT_SERIALIZE_BUFFER_SIZE: usize = 1024;

pub fn serialize_message<D>(mut pbft_message: consensus_messages_capnp::protocol_message::Builder,
                            m: &PBFTMessage<D::Request>) -> Result<()> where D: SharedData {
    match m {
        PBFTMessage::Consensus(consensus_msg) => {
            let mut consensus_builder = pbft_message.init_consensus_message();

            serialize_consensus_message::<D>(consensus_builder, consensus_msg)?;
        }
        PBFTMessage::ViewChange(view_change) => {
            let mut view_builder = pbft_message.init_view_change_message();

            serialize_view_change::<D>(view_builder, view_change)?;
        }
        PBFTMessage::FwdConsensus(_) => {}
        PBFTMessage::ObserverMessage(msg) => {
            let obs_msg = pbft_message.init_observer_message();

            serialize_observer_message(obs_msg, msg)?;
        }
    }

    Ok(())
}

pub fn deserialize_message<D>(pbft_reader: consensus_messages_capnp::protocol_message::Reader) -> Result<PBFTMessage<D::Request>> where
    D: SharedData {
    let which = pbft_reader.which().wrapped(ErrorKind::CommunicationSerialize)?;

    match which {
        consensus_messages_capnp::protocol_message::WhichReader::ConsensusMessage(Ok(cons_msg)) => {
            Ok(PBFTMessage::Consensus(deserialize_consensus_message::<D>(cons_msg)?))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ViewChangeMessage(Ok(view_change)) => {
            Ok(PBFTMessage::ViewChange(deserialize_view_change::<D>(view_change)?))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ObserverMessage(Ok(obs_msg)) => {
            Ok(PBFTMessage::ObserverMessage(deserialize_observer_message(obs_msg)?))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ViewChangeMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ObserverMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        consensus_messages_capnp::protocol_message::WhichReader::ConsensusMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
    }
}

fn deserialize_consensus_message<D>(
    consensus_msg: consensus_messages_capnp::consensus::Reader,
) -> Result<ConsensusMessage<D::Request>>
    where
        D: SharedData,
{
    let seq_no: SeqNo = consensus_msg.get_seq_no().into();
    let view: SeqNo = consensus_msg.get_view().into();

    let consensus_type = consensus_msg
        .which()
        .wrapped(ErrorKind::CommunicationSerialize)?;

    let consensus_kind = match consensus_type {
        consensus_messages_capnp::consensus::Which::PrePrepare(Ok(pre_prepare)) => {
            let mut rqs = Vec::new();

            for pre_prepare_rq in pre_prepare.iter() {
                let header: Vec<u8> = pre_prepare_rq
                    .get_header()
                    .wrapped(ErrorKind::CommunicationSerialize)?
                    .to_vec();

                let request = pre_prepare_rq
                    .get_request()
                    .wrapped(ErrorKind::CommunicationSerialize)?;

                let parsed_request = deserialize_request::<D>(request)?;

                rqs.push(StoredMessage::new(
                    Header::deserialize_from(&header[..])?,
                    parsed_request,
                ));
            }

            Ok(ConsensusMessageKind::PrePrepare(rqs))
        }
        consensus_messages_capnp::consensus::Which::PrePrepare(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
        consensus_messages_capnp::consensus::Which::Prepare(Ok(data)) => {
            let digest = Digest::from_bytes(&data[..])?;

            Ok(ConsensusMessageKind::Prepare(digest))
        }
        consensus_messages_capnp::consensus::Which::Prepare(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
        consensus_messages_capnp::consensus::Which::Commit(Ok(data)) => {
            let digest = Digest::from_bytes(&data[..])?;

            Ok(ConsensusMessageKind::Commit(digest))
        }
        consensus_messages_capnp::consensus::Which::Commit(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
    }?;

    Ok(ConsensusMessage::new(seq_no, view, consensus_kind))
}

/// Deserialize a persistent consensus message from the log
pub fn deserialize_consensus<R, D>(r: R) -> Result<ConsensusMessage<D::Request>>
    where
        R: Read,
        D: SharedData,
{
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let consensus_msg: consensus_messages_capnp::consensus::Reader = reader.get_root().wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get system msg root",
    )?;

    deserialize_consensus_message::<D>(consensus_msg)
}

fn serialize_consensus_message<S>(
    mut consensus: atlas_capnp::consensus_messages_capnp::consensus::Builder,
    m: &ConsensusMessage<S::Request>,
) -> Result<()>
    where
        S: SharedData,
{
    consensus.set_seq_no(m.sequence_number().into());
    consensus.set_view(m.view().into());

    match m.kind() {
        ConsensusMessageKind::PrePrepare(requests) => {
            let mut header = [0; Header::LENGTH];
            let mut pre_prepare_requests = consensus.reborrow().init_pre_prepare(requests.len() as u32);

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

                    let stored_req = stored.message();

                    request.set_session_id(stored_req.session_id().into());
                    request.set_operation_id(stored_req.sequence_number().into());

                    let mut rq = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

                    S::serialize_request(&mut rq, stored_req.operation())?;

                    let rq = Buf::from(rq);

                    request.set_request(rq.as_ref());
                }
            }
        }
        ConsensusMessageKind::Prepare(digest) => consensus.set_prepare(digest.as_ref()),
        ConsensusMessageKind::Commit(digest) => consensus.set_commit(digest.as_ref()),
    }

    Ok(())
}

pub fn serialize_consensus<W, S>(w: &mut W, message: &ConsensusMessage<S::Request>) -> Result<()>
    where
        W: Write,
        S: SharedData,
{
    let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

    let mut consensus_msg: consensus_messages_capnp::consensus::Builder = root.init_root();

    serialize_consensus_message::<S>(consensus_msg, message)?;

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

fn serialize_view_change<D>(mut view_change: consensus_messages_capnp::view_change::Builder,
                            msg: &ViewChangeMessage<D::Request>) -> Result<()> where D: SharedData {
    Ok(())
}

fn deserialize_view_change<D>(view_change: consensus_messages_capnp::view_change::Reader)
                              -> Result<ViewChangeMessage<D::Request>>
    where D: SharedData {
    Err(Error::simple(ErrorKind::CommunicationSerialize))
}

fn serialize_observer_message(mut obs_message: consensus_messages_capnp::observer_message::Builder,
                              msg: &ObserverMessage) -> Result<()> {
    let mut obs_message_type = obs_message.init_message_type();

    match msg {
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

    Ok(())
}

fn deserialize_observer_message(observer_msg: consensus_messages_capnp::observer_message::Reader) -> Result<ObserverMessage> {
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

    Ok(observer_msg)
}