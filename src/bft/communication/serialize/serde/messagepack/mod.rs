use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

pub fn serialize_message<O, R, W>(mut w: W, m: &SystemMessage<O, R>) -> Result<W>
where
    O: Serialize,
    R: Serialize,
    W: Write,
{
    let mut w = buf.writer();
    rmp_serde::encode::write(&mut w, m)
        .map(|_| w)
        .wrapped(ErrorKind::CommunicationSerializeSerdeMessagepack)
}

pub fn deserialize_message<O, R, Rd>(mut r: Rd) -> Result<SystemMessage<O, R>>
where
    O: for<'de> Deserialize<'de>,
    R: for<'de> Deserialize<'de>,
    Rd: Read,
{
    rmp_serde::decode::from_read(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeMessagepack)
}

