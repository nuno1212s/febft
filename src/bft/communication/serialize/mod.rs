#[cfg(feature = "serialize_capnp")]
mod capnp;

use futures::io::{AsyncRead, AsyncWrite};

use crate::bft::error::*;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub struct Serializer<W> {
    #[cfg(feature = "serialize_capnp")]
    inner: capnp::Serializer<W>,
}

pub struct Deserializer<R> {
    #[cfg(feature = "serialize_capnp")]
    inner: capnp::Deserializer<R>,
}

impl<W: Unpin + AsyncWrite> Serializer<W> {
    pub fn new(writer: W) -> Self {
        let inner = {
            #[cfg(feature = "serialize_capnp")]
            capnp::Serializer::new(writer)
        };
        Serializer { inner }
    }

    pub async fn to_replica(&mut self, m: ReplicaMessage) -> Result<()> {
        self.inner.to_replica(m).await
    }
}

impl<R: Unpin + AsyncRead> Deserializer<R> {
    pub fn new(reader: R) -> Self {
        let inner = {
            #[cfg(feature = "serialize_capnp")]
            capnp::Deserializer::new(reader)
        };
        Deserializer { inner }
    }

    pub async fn from_replica(&mut self) -> Result<ReplicaMessage> {
        self.inner.from_replica().await
    }
}
