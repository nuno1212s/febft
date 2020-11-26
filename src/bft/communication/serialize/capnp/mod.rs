use std::default::Default;

use capnp::serialize;
use capnp::message::HeapAllocator;
use bytes::{Buf, BufMut};

use crate::bft::error::*;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub fn serialize_to_replica<B: BufMut>(buf: B, m: ReplicaMessage) -> Result<B> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    let mut message_builder: message::replica_message::Builder = root.init_root();
    match m {
        ReplicaMessage::Dummy(data) => message_builder.set_dummy(&data),
    };
    let mut writer = buf.writer();
    serialize::write_message(&mut writer, &root)
        .map(|()| writer.into_inner())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub fn deserialize_from_replica<B: Buf>(buf: B) -> Result<ReplicaMessage> {
    let root = capnp::serialize::read_message(buf.reader(), Default::default())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;
    let message_reader: message::replica_message::Reader = root.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get message root")?;
    let dummy = message_reader.get_dummy()
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get dummy")?;
    Ok(ReplicaMessage::Dummy(dummy.into()))
}

mod message {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/message_capnp.rs"));
}
