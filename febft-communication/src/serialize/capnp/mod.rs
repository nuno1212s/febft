use std::cell::{RefCell};
use std::io::{Read, Write};
use capnp::message::{HeapAllocator, ScratchSpaceHeapAllocator};
use crate::message::{NetworkMessageContent, PingMessage};
use crate::serialize::Serializable;
use febft_common::error::*;
use febft_common::mem_pool::*;
use febft_capnp::network_messages_capnp;

thread_local! {
    static CACHED_MEM : RefCell<ClaimedMemory> = RefCell::new(ClaimedMemory::new(2048))
}

pub(super) fn serialize_message<T, W>(m: &NetworkMessageContent<T>, w: &mut W) -> Result<()> where T: Serializable, W: Write + AsRef<[u8]> {
    CACHED_MEM.with(|mem| {

        let memory_cache = (*mem).borrow();

        let buffer : MemoryGrant = memory_cache.take_mem();

        {
            let mut borrow = buffer.buffer().borrow_mut();

            //let alloc = ScratchSpaceHeapAllocator::new(&mut *borrow);

            let alloc = HeapAllocator::new();

            let mut builder = capnp::message::Builder::new(alloc);

            let mut root: network_messages_capnp::network_message::Builder = builder.init_root();

            match m {
                NetworkMessageContent::System(sys_msg) => {
                    let mut vec = Vec::with_capacity(512);

                    T::serialize(&mut vec, sys_msg)?;

                    root.reborrow().set_system_message(&vec[..]);
                }
                NetworkMessageContent::Ping(ping) => {
                    let mut ping_msg = root.reborrow().init_ping_message();

                    ping_msg.set_request(ping.is_request());
                }
            }

            capnp::serialize::write_message(w, &builder).wrapped_msg(
                ErrorKind::CommunicationSerialize,
                "Failed to serialize using capnp",
            )?;
        }

        Ok(())
    })
}


pub(super) fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T>> where T: Serializable, R: Read {

    let reader = capnp::serialize::read_message(r, Default::default()).wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get capnp reader",
    )?;

    let network_msg: network_messages_capnp::network_message::Reader = reader.get_root().wrapped_msg(
        ErrorKind::CommunicationSerialize,
        "Failed to get network msg root",
    )?;

    let msg = match network_msg.which().unwrap() {
        network_messages_capnp::network_message::WhichReader::SystemMessage(Ok(data)) => {
            NetworkMessageContent::System(T::deserialize_message(data)?)
        }
        network_messages_capnp::network_message::WhichReader::SystemMessage(Err(err)) => {
            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
        network_messages_capnp::network_message::WhichReader::PingMessage(Ok(ping)) => {
            NetworkMessageContent::Ping(PingMessage::new(ping.get_request()))
        }
        network_messages_capnp::network_message::WhichReader::PingMessage(Err(err)) => {
            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
    };

    Ok(msg)
}