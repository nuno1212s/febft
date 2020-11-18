use std::io;

use crate::bft::communication::socket::Socket;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub async fn serialize_to_replica(s: &mut Socket, m: ReplicaMessage) -> io::Result<()> {
    unimplemented!();
}

pub async fn deserialize_from_replica(s: &mut Socket) -> io::Result<ReplicaMessage> {
    unimplemented!();
}
