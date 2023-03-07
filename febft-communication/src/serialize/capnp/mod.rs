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

pub(super) fn serialize_message<T, W>(m: &NetworkMessageContent<T::Message>, w: &mut W) -> Result<()> where T: Serializable, W: Write + AsRef<[u8]> {
    T::serialize_message_capnp(w, m)
}


pub(super) fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T::Message>> where T: Serializable, R: Read {
    T::deserialize_message_capnp(r)
}