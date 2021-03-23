use bytes::{Buf, BufMut};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

pub fn serialize_message<O, B>(buf: B, m: SystemMessage<O>) -> Result<B>
where
    O: Serialize,
    B: BufMut,
{
    let mut w = buf.writer();
    serde_cbor::to_writer(&mut w, &m)
        .map(|()| w.into_inner())
        .wrapped(ErrorKind::CommunicationSerializeSerdeCbor)
}

pub fn deserialize_message<'de, O, B>(buf: B) -> Result<SystemMessage<O>>
where
    O: Deserialize<'de>,
    B: Buf,
{
    let mut r = buf.reader();
    serde_cbor::from_reader(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeCbor)
}
