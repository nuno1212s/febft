use std::io;
use std::default::Default;

use capnp::message::HeapAllocator;

use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub async fn serialize_to_replica(s: &mut Socket, m: ReplicaMessage) -> io::Result<()> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    let mut message_builder: message::replica_message::Builder = root.init_root();
    match m {
        ReplicaMessage::Dummy(data) => message_builder.set_dummy(&data),
    };
    capnp_futures::serialize::write_message(s, root)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to serialize using capnp: {}", e)))
}

pub async fn deserialize_from_replica(s: &mut Socket) -> io::Result<ReplicaMessage> {
    let root = capnp_futures::serialize::read_message(s, Default::default())
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to deserialize using capnp: {}", e)))?;

    match root {
        Some(root) => {
            let message_reader: message::replica_message::Reader = root.get_root()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to get root: {}", e)))?;
            let dummy = message_reader.get_dummy()
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to get dummy: {}", e)))?;
            Ok(ReplicaMessage::Dummy(dummy.into()))
        },
        None => Err(io::Error::new(io::ErrorKind::Other, "No message read from socket.")),
    }
}

mod message {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/message_capnp.rs"));
}
