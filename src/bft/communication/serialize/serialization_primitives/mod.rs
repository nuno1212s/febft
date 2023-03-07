use std::io::Read;
use std::io::Write;
use bytes::BytesMut;
use capnp::message::ReaderOptions;

use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, Header, ObserveEventKind, ObserverMessage, PingMessage, ReplyMessage, RequestMessage, StoredMessage, SystemMessage};

use crate::bft::crypto::hash::Digest;
use crate::bft::error::*;
use crate::bft::msg_log::persistent::ProofInfo;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::sync::view::ViewInfo;

use super::{Buf, SharedData};

/// This module is meant to handle the serialization of the SMR messages, to allow for the users of this library to only
/// Have to serialize (and declare) their request, reply and state, instead of also having to do so for all message
/// types of the SMR protocol.

const DEFAULT_SERIALIZE_BUFFER_SIZE: usize = 1024;

/// Serialize a wire message into the writer `W`.
pub fn serialize_message<W, S>(
    w: &mut W,
    m: &SystemMessage<S::State, S::Request, S::Reply>,
) -> Result<()>
    where
        W: Write,
        S: SharedData + ?Sized,
{
    let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

    let mut sys_msg: messages_capnp::system::Builder = root.init_root();

    match m {
        SystemMessage::Request(req) => {
            let mut request = sys_msg.reborrow().init_request();

            request.set_session_id(req.session_id().into());
            request.set_operation_id(req.sequence_number().into());

            let mut rq = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

            S::serialize_request(&mut rq, req.operation())?;

            let rq = Buf::from(rq);

            request.set_request(rq.as_ref());
        }
        SystemMessage::UnOrderedRequest(req) => {
            let mut request = sys_msg.reborrow().init_unordered_request();

            request.set_session_id(req.session_id().into());
            request.set_operation_id(req.sequence_number().into());

            let mut rq = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

            S::serialize_request(&mut rq, req.operation())?;

            let rq = Buf::from(rq);

            request.set_request(rq.as_ref());
        }
        SystemMessage::Reply(reply) => {
            let mut reply_obj = sys_msg.init_reply();

            reply_obj.set_session_id(reply.session_id().into());
            reply_obj.set_operation_id(reply.sequence_number().into());

            let mut rq = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

            S::serialize_reply(&mut rq, reply.payload())?;

            let bytes = Buf::from(rq);

            reply_obj.set_reply(bytes.as_ref());
        }
        SystemMessage::UnOrderedReply(reply) => {
            let mut reply_obj = sys_msg.init_unordered_reply();

            reply_obj.set_session_id(reply.session_id().into());
            reply_obj.set_operation_id(reply.sequence_number().into());

            let mut rq = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

            S::serialize_reply(&mut rq, reply.payload())?;

            let rq = Buf::from(rq);

            reply_obj.set_reply(rq.as_ref());
        }
        SystemMessage::Consensus(m) => {
            let mut consensus = sys_msg.init_consensus();

            serialize_consensus_message::<S>(&mut consensus, m)?;
        }
        SystemMessage::ObserverMessage(observer_message) => {
            let capnp_observer = sys_msg.init_observer_message();

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
        SystemMessage::Ping(message) => {
            let mut builder = sys_msg.init_ping();

            builder.set_request(message.is_request());
        }
        _ => return Err("Unsupported system message").wrapped(ErrorKind::CommunicationSerialize),
    }

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )?;

    Ok(())
}

fn deserialize_request<S>(
    req: messages_capnp::request::Reader,
) -> Result<RequestMessage<S::Request>>
    where
        S: SharedData + ?Sized,
{
    let session_id: SeqNo = req.get_session_id().into();
    let op_id: SeqNo = req.get_operation_id().into();

    let request = req
        .get_request()
        .wrapped(ErrorKind::CommunicationSerialize)?;

    Ok(RequestMessage::new(
        session_id,
        op_id,
        S::deserialize_request(request)?,
    ))
}

fn deserialize_unordered_request<S>(
    req: messages_capnp::unordered_request::Reader,
) -> Result<RequestMessage<S::Request>>
    where
        S: SharedData + ?Sized,
{
    let session_id: SeqNo = req.get_session_id().into();
    let op_id: SeqNo = req.get_operation_id().into();

    let request = req
        .get_request()
        .wrapped(ErrorKind::CommunicationSerialize)?;

    Ok(RequestMessage::new(
        session_id,
        op_id,
        S::deserialize_request(request)?,
    ))
}

fn deserialize_reply<S>(req: messages_capnp::reply::Reader) -> Result<ReplyMessage<S::Reply>>
    where
        S: SharedData + ?Sized,
{
    let session_id: SeqNo = req.get_session_id().into();
    let op_id: SeqNo = req.get_operation_id().into();

    let reply = req.get_reply().wrapped(ErrorKind::CommunicationSerialize)?;

    Ok(ReplyMessage::new(
        session_id,
        op_id,
        S::deserialize_reply(reply)?,
    ))
}

fn deserialize_unordered_reply<S>(
    req: messages_capnp::unordered_reply::Reader,
) -> Result<ReplyMessage<S::Reply>>
    where
        S: SharedData + ?Sized,
{
    let session_id: SeqNo = req.get_session_id().into();
    let op_id: SeqNo = req.get_operation_id().into();

    let reply = req.get_reply().wrapped(ErrorKind::CommunicationSerialize)?;

    Ok(ReplyMessage::new(
        session_id,
        op_id,
        S::deserialize_reply(reply)?,
    ))
}

fn deserialize_consensus_message<S>(
    consensus_msg: messages_capnp::consensus::Reader,
) -> Result<ConsensusMessage<S::Request>>
    where
        S: SharedData + ?Sized,
{
    let seq_no: SeqNo = consensus_msg.get_seq_no().into();
    let view: SeqNo = consensus_msg.get_view().into();

    let consensus_type = consensus_msg
        .which()
        .wrapped(ErrorKind::CommunicationSerialize)?;

    let consensus_kind = match consensus_type {
        messages_capnp::consensus::Which::PrePrepare(Ok(pre_prepare)) => {
            let mut rqs = Vec::new();

            for pre_prepare_rq in pre_prepare.iter() {
                let header: Vec<u8> = pre_prepare_rq
                    .get_header()
                    .wrapped(ErrorKind::CommunicationSerialize)?
                    .to_vec();

                let request = pre_prepare_rq
                    .get_request()
                    .wrapped(ErrorKind::CommunicationSerialize)?;

                let parsed_request = deserialize_request::<S>(request)?;

                rqs.push(StoredMessage::new(
                    Header::deserialize_from(&header[..])?,
                    parsed_request,
                ));
            }

            Ok(ConsensusMessageKind::PrePrepare(rqs))
        }
        messages_capnp::consensus::Which::PrePrepare(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::consensus::Which::Prepare(Ok(data)) => {
            let digest = Digest::from_bytes(&data[..])?;

            Ok(ConsensusMessageKind::Prepare(digest))
        }
        messages_capnp::consensus::Which::Prepare(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::consensus::Which::Commit(Ok(data)) => {
            let digest = Digest::from_bytes(&data[..])?;

            Ok(ConsensusMessageKind::Commit(digest))
        }
        messages_capnp::consensus::Which::Commit(Err(err)) => {
            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
        }
    }?;

    Ok(ConsensusMessage::new(seq_no, view, consensus_kind))
}

/// Deserialize a persistent consensus message from the log
pub fn deserialize_consensus<R, S>(r: R) -> Result<ConsensusMessage<S::Request>>
    where
        R: Read,
        S: SharedData + ?Sized,
{
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let consensus_msg: messages_capnp::consensus::Reader = reader.get_root().wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get system msg root",
    )?;

    deserialize_consensus_message::<S>(consensus_msg)
}

/// Deserialize a wire message from a reader `R`.
pub fn deserialize_message<R, S>(r: R) -> Result<SystemMessage<S::State, S::Request, S::Reply>>
    where
        R: Read,
        S: SharedData + ?Sized,
{
    let mut options = ReaderOptions::new();

    options.traversal_limit_in_words(None);

    let reader = capnp::serialize::read_message(r, options).wrapped_msg(

        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let sys_msg: messages_capnp::system::Reader = reader.get_root().wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get system msg root",
    )?;

    let sys_which = sys_msg
        .which()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to match msg")?;

    match sys_which {
        messages_capnp::system::Which::Request(Ok(req)) => {
            Ok(SystemMessage::Request(deserialize_request::<S>(req)?))
        }
        messages_capnp::system::Which::Request(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::UnorderedRequest(Ok(req)) => Ok(
            SystemMessage::UnOrderedRequest(deserialize_unordered_request::<S>(req)?),
        ),
        messages_capnp::system::Which::UnorderedRequest(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::Reply(Ok(req)) => {
            Ok(SystemMessage::Reply(deserialize_reply::<S>(req)?))
        }
        messages_capnp::system::Which::Reply(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::UnorderedReply(Ok(req)) => Ok(
            SystemMessage::UnOrderedReply(deserialize_unordered_reply::<S>(req)?),
        ),
        messages_capnp::system::Which::UnorderedReply(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::Consensus(Ok(consensus_msg)) => Ok(
            SystemMessage::Consensus(deserialize_consensus_message::<S>(consensus_msg)?),
        ),
        messages_capnp::system::Which::Consensus(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::ObserverMessage(Ok(obs_req)) => {
            let message_type = obs_req.get_message_type();

            let type_which = message_type
                .which()
                .wrapped(ErrorKind::CommunicationSerialize)?;

            let observer_msg = match type_which {
                messages_capnp::observer_message::message_type::ObserverRegister(()) => {
                    Ok(ObserverMessage::ObserverRegister)
                }
                messages_capnp::observer_message::message_type::ObserverUnregister(()) => {
                    Ok(ObserverMessage::ObserverUnregister)
                }
                messages_capnp::observer_message::message_type::ObserverRegisterResponse(
                    result,
                ) => Ok(ObserverMessage::ObserverRegisterResponse(result)),
                messages_capnp::observer_message::message_type::ObservedValue(Ok(obs_req)) => {
                    let which = obs_req
                        .get_value()
                        .which()
                        .wrapped(ErrorKind::CommunicationSerialize)?;

                    let observed_value = match which {
                        messages_capnp::observed_value::value::CheckpointStart(start) => {
                            Ok(ObserveEventKind::CheckpointStart(start.into()))
                        }
                        messages_capnp::observed_value::value::CheckpointEnd(end) => {
                            Ok(ObserveEventKind::CheckpointEnd(end.into()))
                        }
                        messages_capnp::observed_value::value::Consensus(seq) => {
                            Ok(ObserveEventKind::Consensus(seq.into()))
                        }
                        messages_capnp::observed_value::value::NormalPhase(Ok(phase)) => {
                            let view = phase.get_view().unwrap();

                            let view_seq: SeqNo = view.get_view_num().into();
                            let n: usize = view.get_n() as usize;
                            let f: usize = view.get_f() as usize;

                            let seq_num: SeqNo = phase.get_seq_num().into();

                            let view_info = ViewInfo::new(view_seq, n, f).unwrap();

                            Ok(ObserveEventKind::NormalPhase((view_info, seq_num)))
                        }
                        messages_capnp::observed_value::value::NormalPhase(Err(err)) => {
                            Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
                        }
                        messages_capnp::observed_value::value::ViewChange(()) => {
                            Ok(ObserveEventKind::ViewChangePhase)
                        }
                        messages_capnp::observed_value::value::CollabStateTransfer(()) => {
                            Ok(ObserveEventKind::CollabStateTransfer)
                        }
                        messages_capnp::observed_value::value::Prepare(seq) => {
                            Ok(ObserveEventKind::Prepare(seq.into()))
                        }
                        messages_capnp::observed_value::value::Commit(seq) => {
                            Ok(ObserveEventKind::Commit(seq.into()))
                        }
                        messages_capnp::observed_value::value::Ready(seq) => {
                            Ok(ObserveEventKind::Ready(seq.into()))
                        }
                        messages_capnp::observed_value::value::Executed(seq) => {
                            Ok(ObserveEventKind::Executed(seq.into()))
                        }
                    }?;

                    Ok(ObserverMessage::ObservedValue(observed_value))
                }
                messages_capnp::observer_message::message_type::ObservedValue(Err(err)) => {
                    Err(format!("{:?}", err)).wrapped(ErrorKind::CommunicationSerialize)
                }
            }?;

            Ok(SystemMessage::ObserverMessage(observer_msg))
        }
        messages_capnp::system::Which::ObserverMessage(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
        messages_capnp::system::Which::Ping(Ok(ping_message)) => {
            Ok(SystemMessage::Ping(PingMessage::new(ping_message.get_request())))
        }
        messages_capnp::system::Which::Ping(Err(err)) => {
            Err(format!("{:?}", err).as_str()).wrapped(ErrorKind::CommunicationSerialize)
        }
    }
}

fn serialize_consensus_message<S>(
    consensus: &mut messages_capnp::consensus::Builder,
    m: &ConsensusMessage<S::Request>,
) -> Result<()>
    where
        S: SharedData + ?Sized,
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
        S: SharedData + ?Sized,
{
    let mut root = capnp::message::Builder::new(capnp::message::HeapAllocator::new());

    let mut consensus_msg: messages_capnp::consensus::Builder = root.init_root();

    serialize_consensus_message::<S>(&mut consensus_msg, message)?;

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

mod messages_capnp {
    #![allow(unused)]
    include!(concat!(
    env!("OUT_DIR"),
    "/messages_capnp.rs"
    ));
}
