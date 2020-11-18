#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::io;

use futures::io::AsyncWriteExt;

use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub async fn to_replica(s: &mut Socket, m: ReplicaMessage) -> io::Result<()> {
    #[cfg(feature = "serialize_capnp")]
    {
        capnp::serialize_to_replica(s, m).await?;
    }
    w.flush().await?;
    Ok(())
}
