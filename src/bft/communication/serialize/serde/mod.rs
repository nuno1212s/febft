pub mod cbor;

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

impl<W: Unpin + AsyncWrite, E: Encoder> Serializer<W, E> {
    pub fn new(writer: W, encoder: E) -> Self {
        Self { inner: FramedWrite::new(writer, encoder) }
    }

    pub async fn to_replica(&mut self, m: ReplicaMessage) -> Result<()> {
        self.inner.send(Message::R(m)).await
            .wrapped_msg(ErrorKind::CommunicationSerializeSerde, "Serialize to replica failed")
    }
}

impl<R: Unpin + AsyncRead, D: Decoder> Deserializer<R, D> {
    pub fn new(reader: R, decoder: D) -> Self {
        Self { inner: FramedRead::new(reader, decoder) }
    }

    pub async fn from_replica(&mut self) -> Result<ReplicaMessage> {
        let message = self.inner.next().await
            .wrapped_msg(ErrorKind::CommunicationSerializeSerde, "Deserialize from replica failed")?;
        match message {
            Message::R(m) => Ok(m),
            _ => panic!("Unexpected result!"),
        }
    }
}
