use std::default::Default;
use std::io::{Read, Write};

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

pub fn serialize_message<O: ToCapnp, W: Write>(mut w: W, m: SystemMessage<O>) -> Result<W> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    O::to_capnp(m, &mut root)?;
    serialize::write_message(&mut w, &root)
        .map(|_| w)
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub fn deserialize_message<O: FromCapnp, R: Read>(r: R) -> Result<SystemMessage<O>> {
    let reader = serialize::read_message(r, Default::default())
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;
    O::from_capnp(&reader)
}

#[cfg(test)]
impl ToCapnp for () {
    fn to_capnp<A>(m: SystemMessage<()>, root: &mut Builder<A>) -> Result<()>
    where
        A: Allocator
    {
        // TODO: create schema files to test this
        unimplemented!()
    }
}
