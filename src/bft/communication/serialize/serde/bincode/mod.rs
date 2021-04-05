use std::io::{Read, Write};
use serde::{Serialize, Deserialize};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

pub fn serialize_message<O, W>(mut w: W, m: &SystemMessage<O>) -> Result<W>
where
    O: Serialize,
    W: Write,
{
    bincode::serialize_into(&mut w, m)
        .map(|_| w)
        .wrapped(ErrorKind::CommunicationSerializeSerdeBincode)
}

pub fn deserialize_message<O, R>(mut r: R) -> Result<SystemMessage<O>>
where
    O: for<'de> Deserialize<'de>,
    R: Read,
{
    bincode::deserialize_from(&mut r)
        .wrapped(ErrorKind::CommunicationSerializeSerdeBincode)
}
