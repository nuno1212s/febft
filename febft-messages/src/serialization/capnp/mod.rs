use std::io::Write;
use crate::messages::{RequestMessage, SystemMessage};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::executable::Service;
use febft_capnp::messages_capnp;
use febft_common::ordering::Orderable;
use febft_execution::serialize::SharedData;

pub(super) fn serialize_message<W, S, P>(w: &mut W, msg: &SystemMessage<S, P>) -> Result<()>
    where W: Write, S: Service, P: Serializable {
    let allocator = capnp::message::HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let message : messages_capnp::system::Builder = root.init_root();

    match msg {
        SystemMessage::OrderedRequest(req) => {
            let mut rq_builder = message.init_request();

            serialize_request_message::<S::Data>(rq_builder.reborrow(), req)?;
        }
        SystemMessage::OrderedReply(rep) => {

        }
        SystemMessage::UnorderedRequest(req) => {
            let mut rq_builder = message.init_unordered_request();

            serialize_request_message::<S::Data>(rq_builder.reborrow(), req)?;

        }
        SystemMessage::UnorderedReply(rep) => {

        }
        SystemMessage::ForwardedRequests(fwd_reqs) => {

        }
        SystemMessage::Protocol(p_data) => {

        }
    }

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}

fn serialize_request_message<S>(mut msg_builder: messages_capnp::request::Builder, rq_msg: &RequestMessage<S::Request>)
-> Result<()>
    where S: SharedData {

    msg_builder.set_operation_id(u32::from(rq_msg.sequence_number()));
    msg_builder.set_session_id(u32::from(rq_msg.session_id()));

    let mut vec = Vec::new();

    S::serialize_request(&mut vec, rq_msg.operation())?;

    msg_builder.set_request(&vec[..]);

    Ok(())
}