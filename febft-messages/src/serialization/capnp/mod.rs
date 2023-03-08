use std::io::{Read, Write};
use std::ops::Deref;
use crate::messages::{ProtocolMessage, ReplyMessage, RequestMessage, SystemMessage};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::executable::{Request, Service};
use febft_capnp::{messages_capnp, service_messages_capnp};
use febft_capnp::messages_capnp::system::WhichReader;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::NetworkMessageContent;
use febft_execution::serialize::SharedData;
use crate::serialization::ProtocolData;

pub(super) fn serialize_message_capnp<D, P>(
    message: messages_capnp::system::Builder,
    msg: &SystemMessage<D, P>,
) -> Result<()> where D: SharedData, P: ProtocolData {
    match msg {
        SystemMessage::OrderedRequest(req) => {
            let mut rq_builder = message.init_request();

            serialize_request_message::<D>(rq_builder.reborrow(), req)?;
        }
        SystemMessage::OrderedReply(rep) => {
            let mut reply_builder = message.init_reply();

            serialize_reply_message::<D>(reply_builder.reborrow(), rep)?;
        }
        SystemMessage::UnorderedRequest(req) => {
            let mut rq_builder = message.init_unordered_request();

            serialize_request_message::<D>(rq_builder.reborrow(), req)?;
        }
        SystemMessage::UnorderedReply(rep) => {
            let mut reply_builder = message.init_reply();

            serialize_reply_message::<D>(reply_builder.reborrow(), rep)?;
        }
        SystemMessage::ForwardedRequests(fwd_reqs) => {}
        SystemMessage::Protocol(p_data) => {
            let protocol_builder: febft_capnp::consensus_messages_capnp::protocol_message::Builder = message.init_protocol();

            P::serialize_capnp(protocol_builder, p_data.deref())?;
        }
    }

    Ok(())
}

pub(super) fn deserialize_message_capnp<D, P>(
    message: messages_capnp::system::Reader
) -> Result<SystemMessage<D, P>>
    where D: SharedData,
          P: ProtocolData {
    let sys_msg_type = message.which()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read which for sys msg")?;

    return match sys_msg_type {
        WhichReader::Request(Ok(req)) => {
            Ok(SystemMessage::OrderedRequest(deserialize_request_message::<D>(req)?))
        }
        WhichReader::Request(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        WhichReader::Reply(Ok(reply)) => {
            Ok(SystemMessage::OrderedReply(deserialize_reply_message::<D>(reply)?))
        }
        WhichReader::Reply(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        WhichReader::UnorderedRequest(Ok(req)) => {
            Ok(SystemMessage::UnorderedRequest(deserialize_request_message::<D>(req)?))
        }
        WhichReader::UnorderedRequest(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        WhichReader::UnorderedReply(Ok(reply)) => {
            Ok(SystemMessage::UnorderedReply(deserialize_reply_message::<D>(reply)?))
        }
        WhichReader::UnorderedReply(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        WhichReader::Protocol(Ok(reader)) => {
            Ok(SystemMessage::Protocol(ProtocolMessage::new(P::deserialize_capnp(reader)?)))
        }
        WhichReader::Protocol(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
    };
}

pub(super) fn serialize_message<W, S, P>(w: &mut W, msg: &SystemMessage<S, P>) -> Result<()>
    where W: Write, S: SharedData, P: ProtocolData {
    let allocator = capnp::message::HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let mut message: messages_capnp::system::Builder = root.init_root();

    serialize_message_capnp(message, msg)?;

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

pub(super) fn deserialize_message<R: Read, S: SharedData, P: ProtocolData>(r: R) -> Result<SystemMessage<S, P>> {
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let message: messages_capnp::system::Reader = reader.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get system msg root")?;

    deserialize_message_capnp(message)
}

pub fn serialize_request_message<D>(mut msg_builder: service_messages_capnp::request::Builder,
                                    rq_msg: &RequestMessage<D::Request>)
                                    -> Result<()>
    where D: SharedData {
    msg_builder.set_operation_id(u32::from(rq_msg.sequence_number()));
    msg_builder.set_session_id(u32::from(rq_msg.session_id()));

    let mut vec = Vec::with_capacity(4096);

    D::serialize_request(&mut vec, rq_msg.operation())?;

    msg_builder.set_request(&vec[..]);

    Ok(())
}

pub fn deserialize_request_message<S>(msg_reader: service_messages_capnp::request::Reader) -> Result<RequestMessage<S::Request>>
    where S: SharedData {
    let seq_num: SeqNo = SeqNo::from(msg_reader.get_operation_id());
    let session_id: SeqNo = SeqNo::from(msg_reader.get_session_id());

    let request = S::deserialize_request(msg_reader.get_request().wrapped(ErrorKind::CommunicationSerialize)?)?;

    Ok(RequestMessage::new(session_id, seq_num, request))
}

pub fn serialize_reply_message<S>(mut msg_builder: service_messages_capnp::reply::Builder, reply_msg: &ReplyMessage<S::Reply>) -> Result<()>
    where S: SharedData {
    msg_builder.set_operation_id(u32::from(reply_msg.sequence_number()));
    msg_builder.set_session_id(u32::from(reply_msg.session_id()));

    let mut vec = Vec::with_capacity(4096);

    S::serialize_reply(&mut vec, reply_msg.payload())?;

    msg_builder.set_reply(&vec[..]);

    Ok(())
}

pub fn deserialize_reply_message<S>(msg_reader: service_messages_capnp::reply::Reader) -> Result<ReplyMessage<S::Reply>>
    where S: SharedData {
    let seq_num: SeqNo = SeqNo::from(msg_reader.get_operation_id());
    let session_id: SeqNo = SeqNo::from(msg_reader.get_session_id());

    let request = S::deserialize_reply(msg_reader.get_reply().wrapped(ErrorKind::CommunicationSerialize)?)?;

    Ok(ReplyMessage::new(session_id, seq_num, request))
}
