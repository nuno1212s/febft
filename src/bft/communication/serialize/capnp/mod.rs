use std::default::Default;
use std::mem::MaybeUninit;

use capnp::serialize;
use capnp::message::HeapAllocator;
use bytes::{Buf, BufMut};

use crate::bft::error::*;
use crate::bft::communication::message::{
    CURRENT_VERSION,
    Header,
    ReplicaMessage,
    ReplicaMessagePayload,
};

// guarantee this is the same width as a u32
#[repr(packed)]
struct MagicVersion {
    magic: [u8; 3],
    version: u8,
}

const MAGIC_VERSION: MagicVersion = MagicVersion {
    magic: *b"BFT",
    version: CURRENT_VERSION,
};

pub fn serialize_message<B: BufMut>(buf: B, m: ReplicaMessage) -> Result<B> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    let mut message_builder: message_capnp::replica_message::Builder = root.init_root();
    let mut header = message_builder.reborrow().init_header();
    header.set_magic_version(unsafe { std::mem::transmute(MAGIC_VERSION) });
    //header.set_reserved0(0);
    //header.set_reserved1(0);
    //header.set_reserved2(0);
    //header.set_reserved3(0);
    //header.set_reserved4(0);
    //header.set_reserved5(0);
    //header.set_reserved6(0);
    match m.payload() {
        ReplicaMessagePayload::Dummy(ref data) => message_builder.set_payload(data),
    };
    let mut writer = buf.writer();
    serialize::write_message(&mut writer, &root)
        .map(|()| writer.into_inner())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub fn deserialize_message<B: Buf>(buf: B) -> Result<ReplicaMessage> {
    let root = capnp::serialize::read_message(buf.reader(), Default::default())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;
    let message_reader: message_capnp::replica_message::Reader = root.get_root()
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get message root")?;
    let header = message_reader.get_header()
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get header")?;
    let mv: MagicVersion = unsafe { std::mem::transmute(header.get_magic_version()) };
    let header = Header {
        magic: mv.magic,
        version: mv.version,
        _reserved: MaybeUninit::uninit(),
    };
    if !header.is_valid() {
        return Err("Invalid header").wrapped(ErrorKind::CommunicationSerializeCapnp)
    }
    let payload = message_reader.get_payload()
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get payload")
        .map(|data| ReplicaMessagePayload::Dummy(data.into()))?;
    Ok(ReplicaMessage { header, payload })
}

mod message_capnp {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/message_capnp.rs"));
}
