use std::default::Default;

use capnp::message::HeapAllocator;
use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::bft::error::*;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub struct Serializer<W> {
    inner: W,
}

pub struct Deserializer<R> {
    inner: R,
}

impl<W: Unpin + AsyncWrite> Serializer<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    pub async fn to_replica(&mut self, m: ReplicaMessage) -> Result<()> {
        serialize_to_replica(&mut self.inner, m).await?;
        self.inner.flush()
            .await
            .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to flush socket")?;
        Ok(())
    }
}

impl<R: Unpin + AsyncRead> Deserializer<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub async fn from_replica(&mut self) -> Result<ReplicaMessage> {
        deserialize_from_replica(&mut self.inner).await
    }
}

#[inline]
async fn serialize_to_replica<W: Unpin + AsyncWrite>(w: W, m: ReplicaMessage) -> Result<()> {
    let mut root = capnp::message::Builder::new(HeapAllocator::new());
    let mut message_builder: message::replica_message::Builder = root.init_root();
    match m {
        ReplicaMessage::Dummy(data) => message_builder.set_dummy(&data),
    };
    capnp_futures::serialize::write_message(w, root)
        .await
        .wrapped_msg(ErrorKind::CommunicationSerializeCapnp, "Failed to serialize using capnp")
}

#[inline]
async fn deserialize_from_replica<R: Unpin + AsyncRead>(r: R) -> Result<ReplicaMessage> {
    let root = capnp_futures::serialize::read_message(r, Default::default())
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
