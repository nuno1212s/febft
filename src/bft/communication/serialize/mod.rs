#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::io;

use futures::io::AsyncWriteExt;

use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub struct Serializer {
    #[cfg(feature = "serialize_capnp")]
    inner: capnp::Serializer,
}

impl Serializer {
    pub fn new() -> Self {
        let inner = {
            #[cfg(feature = "serialize_capnp")]
            capnp::Serializer::new()
        };
        Serializer { inner }
    }

    pub async fn serialize_to_replica(&mut self, s: &mut Socket, m: ReplicaMessage) -> io::Result<()> {
        self.inner.serialize_to_replica(s, m).await?;
        s.flush().await?;
        Ok(())
    }

    pub async fn deserialize_from_replica(&mut self, s: &mut Socket) -> io::Result<ReplicaMessage> {
        self.inner.deserialize_from_replica(s).await
    }
}
