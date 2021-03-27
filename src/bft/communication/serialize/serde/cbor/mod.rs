use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

pub fn serialize_message<O, W>(mut w: W, m: SystemMessage<O>) -> Result<W>
where
    O: Serialize,
    W: Write,
{
    serde_cbor::to_writer(&mut w, &m)
        .map(|()| w.into_inner())
        .wrapped(ErrorKind::CommunicationSerializeSerdeCbor)
}

pub fn deserialize_message<O, R>(mut r: R) -> Result<SystemMessage<O>>
where
    O: for<'de> Deserialize<'de>,
    R: Read,
{
    serde_cbor::from_reader(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeCbor)
}
