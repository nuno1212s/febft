use std::default::Default;

use bytes::{Buf, BufMut};
use capnp::serialize;
use capnp::message::{
    Reader,
    Builder,
    Allocator,
    HeapAllocator,
    ReaderSegments,
};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

// FIXME: maybe use `capnp::message::ScratchSpaceHeapAllocator` instead of
// `capnp::message::HeapAllocator`; this requires some wrapper type for
// allocating messages, which is slightly annoying, but ultimately better
// for performance reasons
//
// e.g. `Serializer::new(scratch).serialize(...)`
//
// each task would have its own `Serializer` instance

/// Deserialize a wire message from a Cap'n'Proto segment reader.
pub trait FromCapnp: Sized {
    fn from_capnp<S>(reader: &Reader<S>) -> Result<SystemMessage<Self>>
    where
        S: ReaderSegments;
}

/// Serialize a wire message using a Cap'n'Proto segment builder.
pub trait ToCapnp: Sized {
    fn to_capnp<A>(m: SystemMessage<Self>, root: &mut Builder<A>) -> Result<()>
    where
        A: Allocator;
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
