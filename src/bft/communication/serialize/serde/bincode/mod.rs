use bytes::{Buf, BufMut};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::{ReplicaMessage, ClientMessage};

pub fn serialize_message<B: BufMut>(buf: B, m: ReplicaMessage) -> Result<B> {
    let mut w = buf.writer();
    bincode::serialize_into(&mut w, &m)
        .map(|()| w.into_inner())
        .wrapped(ErrorKind::CommunicationSerializeSerdeBincode)
}

pub fn deserialize_message<B: Buf>(buf: B) -> Result<ReplicaMessage> {
    let mut r = buf.reader();
    bincode::deserialize_from(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeBincode)
}
