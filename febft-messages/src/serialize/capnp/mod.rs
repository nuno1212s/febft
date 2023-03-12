use febft_capnp::{messages_capnp, service_messages_capnp};
use febft_execution::serialize::SharedData;
use febft_common::error::*;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_communication::serialize::{Buf, Serializable};
use crate::messages::{ForwardedProtocolMessage, ForwardedRequestsMessage, Protocol, ReplyMessage, RequestMessage, SystemMessage};
use crate::serialize::{OrderingProtocol, System};

const DEFAULT_SERIALIZE_BUFFER_SIZE: usize = 1024;

pub type Message<D, P> = <System<D, P> as Serializable>::Message;

pub(super) fn serialize_message<D, P>(builder: messages_capnp::system::Builder, msg: &Message<D, P>) -> Result<()> where D: SharedData, P: OrderingProtocol {
    match msg {
        SystemMessage::OrderedRequest(req) => {
            let rq_builder = builder.init_request();

            serialize_request::<D>(rq_builder, req)?;
        }
        SystemMessage::UnorderedRequest(req) => {
            let unordered_rq_builder = builder.init_unordered_request();

            serialize_request::<D>(unordered_rq_builder, req)?;
        }
        SystemMessage::OrderedReply(rep) => {
            let reply_builder = builder.init_reply();

            serialize_reply::<D>(reply_builder, rep)?;
        }
        SystemMessage::UnorderedReply(rep) => {
            let unordered_reply_builder = builder.init_unordered_reply();

            serialize_reply::<D>(unordered_reply_builder, rep)?;
        }
        SystemMessage::ProtocolMessage(protocol) => {
            let protocol_message_builder = builder.init_protocol();

            P::serialize_capnp(protocol_message_builder, protocol.payload())?;
        }
        SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
            let fwd_protocol_builder = builder.init_fwd_protocol();

            serialize_fwd_protocol_message::<P>(fwd_protocol_builder, fwd_protocol)?;
        }
        SystemMessage::ForwardedRequestMessage(fwd_req) => {
            let mut fwd_rqs_builder = builder.init_fwd_requests(fwd_req.requests().len() as u32);

            let mut header = [0; Header::LENGTH];

            for (i, stored) in fwd_req.requests().iter().enumerate() {
                let mut forwarded = fwd_rqs_builder.reborrow().get(i as u32);

                // set header
                {
                    stored.header().serialize_into(&mut header[..]).unwrap();
                    forwarded.set_header(&header[..]);
                }

                // set request
                {
                    let mut request = forwarded.reborrow().init_request();

                    serialize_request::<D>(request, stored.message())?;
                }
            }
        }
    }

    Ok(())
}

pub(super) fn deserialize_message<D, P>(reader: messages_capnp::system::Reader) -> Result<Message<D, P>>
    where D: SharedData, P: OrderingProtocol {
    let which = reader.which().wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to read which type of message for the system message")?;

    return match which {
        messages_capnp::system::WhichReader::Request(Ok(req)) => {
            Ok(SystemMessage::OrderedRequest(deserialize_request::<D>(req)?))
        }
        messages_capnp::system::WhichReader::Reply(Ok(rep)) => {
            Ok(SystemMessage::OrderedReply(deserialize_reply::<D>(rep)?))
        }
        messages_capnp::system::WhichReader::UnorderedRequest(Ok(req)) => {
            Ok(SystemMessage::UnorderedRequest(deserialize_request::<D>(req)?))
        }
        messages_capnp::system::WhichReader::UnorderedReply(Ok(rep)) => {
            Ok(SystemMessage::UnorderedReply(deserialize_reply::<D>(rep)?))
        }
        messages_capnp::system::WhichReader::Protocol(Ok(protocol)) => {
            Ok(SystemMessage::ProtocolMessage(Protocol::new(P::deserialize_capnp(protocol)?)))
        }
        messages_capnp::system::WhichReader::FwdProtocol(Ok(fwd_protocol)) => {
            Ok(SystemMessage::ForwardedProtocolMessage(deserialize_fwd_protocol_message::<P>(fwd_protocol)?))
        }
        messages_capnp::system::WhichReader::FwdRequests(Ok(reqs)) => {
            let mut rqs = Vec::new();

            for fwd_req in reqs.iter() {
                let header: Vec<u8> = fwd_req
                    .get_header()
                    .wrapped(ErrorKind::CommunicationSerialize)?
                    .to_vec();

                let request = fwd_req
                    .get_request()
                    .wrapped(ErrorKind::CommunicationSerialize)?;

                let parsed_request = deserialize_request::<D>(request)?;

                rqs.push(StoredMessage::new(
                    Header::deserialize_from(&header[..])?,
                    parsed_request,
                ));
            }

            Ok(SystemMessage::ForwardedRequestMessage(ForwardedRequestsMessage::new(rqs)))
        }
        messages_capnp::system::WhichReader::Request(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::Reply(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::UnorderedRequest(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::UnorderedReply(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::FwdRequests(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::Protocol(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        messages_capnp::system::WhichReader::FwdProtocol(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
    };
}

pub fn serialize_request<D>(mut builder: service_messages_capnp::request::Builder, msg: &RequestMessage<D::Request>) -> Result<()> where D: SharedData {
    builder.set_operation_id(u32::from(msg.sequence_number()));
    builder.set_session_id(u32::from(msg.session_id()));

    let mut rq_data = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

    D::serialize_request(&mut rq_data, msg.operation())?;

    builder.set_request(&rq_data[..]);

    Ok(())
}

pub fn deserialize_request<D>(reader: service_messages_capnp::request::Reader) -> Result<RequestMessage<D::Request>>
    where D: SharedData {
    let seq_no = SeqNo::from(reader.get_operation_id());
    let session = SeqNo::from(reader.get_session_id());

    let operation = D::deserialize_request(reader.get_request().wrapped(ErrorKind::CommunicationSerialize)?)?;

    Ok(RequestMessage::new(session, seq_no, operation))
}

pub fn serialize_reply<D>(mut builder: service_messages_capnp::reply::Builder, msg: &ReplyMessage<D::Reply>) -> Result<()> where D: SharedData {
    builder.set_operation_id(u32::from(msg.sequence_number()));
    builder.set_session_id(u32::from(msg.session_id()));

    let mut rq_data = Vec::with_capacity(DEFAULT_SERIALIZE_BUFFER_SIZE);

    D::serialize_reply(&mut rq_data, msg.payload())?;

    builder.set_reply(&rq_data[..]);

    Ok(())
}

pub fn deserialize_reply<D>(reader: service_messages_capnp::reply::Reader) -> Result<ReplyMessage<D::Reply>>
    where D: SharedData {
    let seq_no = SeqNo::from(reader.get_operation_id());
    let session = SeqNo::from(reader.get_session_id());

    let operation = D::deserialize_reply(reader.get_reply().wrapped(ErrorKind::CommunicationSerialize)?)?;

    Ok(ReplyMessage::new(session, seq_no, operation))
}

pub fn serialize_fwd_protocol_message<P>(mut builder: messages_capnp::forwarded_protocol::Builder, msg: &ForwardedProtocolMessage<P::ProtocolMessage>) -> Result<()> where P: OrderingProtocol {
    let mut header = [0; Header::LENGTH];

    msg.header().serialize_into(&mut header[..])?;

    builder.set_header(&header);

    let protocol_builder = builder.init_message();

    P::serialize_capnp(protocol_builder, msg.message().message().payload())?;

    Ok(())
}

pub fn deserialize_fwd_protocol_message<P>(reader: messages_capnp::forwarded_protocol::Reader) -> Result<ForwardedProtocolMessage<P::ProtocolMessage>> where P: OrderingProtocol {
    let header = Header::deserialize_from(reader.get_header().wrapped(ErrorKind::CommunicationSerialize)?)?;

    let protocol_msg = P::deserialize_capnp(reader.get_message().wrapped(ErrorKind::CommunicationSerialize)?)?;

    Ok(ForwardedProtocolMessage::new(StoredMessage::new(header, Protocol::new(protocol_msg))))
}
