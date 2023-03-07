use std::io::{Read, Write};
use bincode::{Decode, Encode};
use bincode::config::Configuration;
use crate::message::NetworkMessageContent;
use febft_common::error::*;
use crate::serialize::Serializable;


pub fn serialize_message<T, W>(
    m: &NetworkMessageContent<T::Message>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsMut<[u8]>,
    T: Serializable {
    bincode::encode_into_slice(m,  w.as_mut(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize message")?;

    Ok(())
}

pub fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T::Message>> where T: Serializable, R: Read + AsRef<[u8]> {
    let (msg, _) = bincode::borrow_decode_from_slice(r.as_ref(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize message")?;

    Ok(msg)
}