use std::default::Default;

use capnp::message::HeapAllocator;

use crate::bft::error::*;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub async fn serialize_to_replica(s: &mut Socket, m: ReplicaMessage) -> Result<()> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    let mut message_builder: message::replica_message::Builder = root.init_root();
    match m {
        ReplicaMessage::Dummy(data) => message_builder.set_dummy(&data),
    };
    capnp_futures::serialize::write_message(s, root)
        .await
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

pub async fn deserialize_from_replica(s: &mut Socket) -> Result<ReplicaMessage> {
    let root = capnp_futures::serialize::read_message(s, Default::default())
        .await
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to deserialize using capnp")?;

    match root {
        Some(root) => {
            let message_reader: message::replica_message::Reader = root.get_root()
                .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get message root")?;
            let dummy = message_reader.get_dummy()
                .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to get dummy")?;
            Ok(ReplicaMessage::Dummy(dummy.into()))
        },
        None => Err("No message read from socket").wrapped(ErrorKind::CommunicationSerializeCapnp),
    }
}

mod message {
    #![allow(unused)]
    include!(concat!(env!("OUT_DIR"), "/src/bft/communication/serialize/capnp/message_capnp.rs"));
}
