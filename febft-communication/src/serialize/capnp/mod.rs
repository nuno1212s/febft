use std::cell::{RefCell};
use std::io::{Read, Write};
use capnp::message::{HeapAllocator, ScratchSpaceHeapAllocator};
use crate::message::{NetworkMessageContent, PingMessage};
use crate::serialize::Serializable;
use febft_common::error::*;
use febft_common::mem_pool::*;
use febft_capnp::network_messages_capnp;
use febft_capnp::network_messages_capnp::network_message;

thread_local! {
    static CACHED_MEM : RefCell<ClaimedMemory> = RefCell::new(ClaimedMemory::new(2048))
}

pub(super) fn serialize_message<T, W>(m: &NetworkMessageContent<T::Message>, w: &mut W) -> Result<()> where T: Serializable, W: Write + AsRef<[u8]> {

    let allocator = HeapAllocator::new();

    let mut root = capnp::message::Builder::new(allocator);

    let mut message: network_message::Builder = root.init_root();

    match m {
        NetworkMessageContent::System(sys_msg) => {
            let builder: febft_capnp::messages_capnp::system::Builder = message.init_system_message();

            T::serialize_message_capnp(builder, sys_msg)?;
        }
        NetworkMessageContent::Ping(ping) => {
            let mut builder = message.init_ping_message();

            builder.set_request(ping.is_request());
        }
    }

    capnp::serialize::write_message(w, &root).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to serialize using capnp",
    )
}


pub(super) fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T::Message>> where T: Serializable, R: Read {
    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let message: network_message::Reader = reader.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to get system msg root")?;

    let which = message.which().wrapped(ErrorKind::CommunicationSerialize)?;

    return match which {
        network_message::WhichReader::SystemMessage(Ok(sub_reader)) => {
            Ok(NetworkMessageContent::System(T::deserialize_message_capnp(sub_reader)?))
        }
        network_message::WhichReader::SystemMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
        network_message::WhichReader::PingMessage(Ok(sub_reader)) => {
            Ok(NetworkMessageContent::Ping(PingMessage::new(sub_reader.get_request())))
        }
        network_message::WhichReader::PingMessage(Err(err)) => {
            Err(Error::wrapped(ErrorKind::CommunicationSerialize, err))
        }
    };

}