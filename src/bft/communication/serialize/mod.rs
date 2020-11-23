#[cfg(feature = "serialize_capnp")]
mod capnp;

use futures::io::AsyncWriteExt;

use crate::bft::error::*;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub async fn serialize_to_replica(s: &mut Socket, m: ReplicaMessage) -> Result<()> {
    let () = {
        #[cfg(feature = "serialize_capnp")]
        capnp::serialize_to_replica(s, m).await?
    };
    s.flush()
        .await
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to flush socket")?;
    Ok(())
}

pub async fn deserialize_from_replica(s: &mut Socket) -> Result<ReplicaMessage> {
    #[cfg(feature = "serialize_capnp")]
    capnp::deserialize_from_replica(s).await
}
