#[cfg(feature = "serialize_serde_cbor")]
pub mod cbor;

#[cfg(feature = "serialize_serde_bincode")]
pub mod bincode;

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::io::{AsyncRead, AsyncWrite};
use futures_codec::{FramedRead, FramedWrite, Encoder, Decoder};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

#[derive(Serialize, Deserialize)]
pub enum Message {
    R(ReplicaMessage),
    C(ClientMessage),
}

pub struct Serializer<W, E> {
    inner: FramedWrite<W, E>,
}

pub struct Deserializer<R, D> {
    inner: FramedRead<R, D>,
}

impl<W, E> Serializer<W, E>
where
    W: Unpin + AsyncWrite,
    E: Encoder<Item = Message>,
{
    pub fn new(writer: W, encoder: E) -> Self {
        Self { inner: FramedWrite::new(writer, encoder) }
    }

    pub async fn to_replica(&mut self, m: ReplicaMessage) -> Result<()> {
        self.inner.send(Message::R(m)).await
            .simple_msg(ErrorKind::CommunicationSerializeSerde, "Serialize to replica failed")
    }
}

impl<R, D> Deserializer<R, D>
where
    R: Unpin + AsyncRead,
    D: Decoder<Item = Message>,
{
    pub fn new(reader: R, decoder: D) -> Self {
        Self { inner: FramedRead::new(reader, decoder) }
    }

    pub async fn from_replica(&mut self) -> Result<ReplicaMessage> {
        let message = self.inner.next().await
            .ok_or(Error::simple(ErrorKind::CommunicationSerializeSerde))
            .wrapped_msg(ErrorKind::CommunicationSerializeSerde, "Deserialize from replica failed")?;
        match message {
            Ok(Message::R(m)) => Ok(m),
            Ok(Message::C(_)) => {
                Err("Unexpected message kind received")
                    .wrapped(ErrorKind::CommunicationSerializeSerde)
            },
            _ => {
                Err("Encountered an error while deserializing")
                    .wrapped(ErrorKind::CommunicationSerializeSerde)
            },
        }
    }
}
