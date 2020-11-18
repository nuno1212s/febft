use std::io;

use capnp::message::HeapAllocator;

use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub struct Serializer {
    allocator: HeapAllocator,
}

impl Serializer {
    pub fn new() -> Self {
        Serializer { allocator: HeapAllocator::new() }
    }

    pub async fn serialize_to_replica(&mut self, s: &mut Socket, m: ReplicaMessage) -> io::Result<()> {
        unimplemented!();
    }

    pub async fn deserialize_from_replica(&mut self, s: &mut Socket) -> io::Result<ReplicaMessage> {
        unimplemented!();
    }
}

mod message {
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/message_capnp.rs"));
}
