use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

pub fn serialize_message<O, P, W>(mut w: W, m: &SystemMessage<O, P>) -> Result<W>
where
    O: Serialize,
    P: Serialize,
    W: Write,
{
    let mut w = buf.writer();
    rmp_serde::encode::write(&mut w, m)
        .map(|_| w)
        .wrapped(ErrorKind::CommunicationSerializeSerdeMessagepack)
}

pub fn deserialize_message<O, P, R>(mut r: R) -> Result<SystemMessage<O, P>>
where
    O: for<'de> Deserialize<'de>,
    P: for<'de> Deserialize<'de>,
    R: Read,
{
    rmp_serde::decode::from_read(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeMessagepack)
}

