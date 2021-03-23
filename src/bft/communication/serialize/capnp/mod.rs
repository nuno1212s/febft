use std::default::Default;

use capnp::serialize;
use capnp::message::HeapAllocator;
use bytes::{Buf, BufMut};

use crate::bft::error::*;

pub trait FromCapnp {
    fn from_capnp(reader: &capnp::message::Reader) -> Result<SystemMessage<O>>;
}

pub trait ToCapnp {
    fn to_capnp(m: SystemMessage<Self>, root: &mut capnp::message::Builder) -> Result<()>;
}

pub fn serialize_message<O: ToCapnp, B: BufMut>(buf: B, m: SystemMessage<O>) -> Result<B> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    O::to_capnp(m, &mut root)?;
    let mut writer = buf.writer();
    serialize::write_message(&mut writer, &root)
        .map(|()| writer.into_inner())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub fn deserialize_message<O: FromCapnp, B: Buf>(buf: B) -> Result<SystemMessage<O>> {
    let reader = serialize::read_message(buf.reader(), Default::default())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;
    O::from_capnp(&reader)
}
