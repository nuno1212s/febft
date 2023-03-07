use std::io::{Read, Write};
use crate::message::NetworkMessageContent;
use crate::serialize::Serializable;
use febft_common::error::*;

pub fn serialize_message<T, W>(
    m: &NetworkMessageContent<T::Message>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsMut<[u8]>,
    T: Serializable {
    bincode::serde::encode_into_slice(m,  w.as_mut(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize message")?;

    Ok(())
}

pub fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T::Message>> where T: Serializable, R: Read + AsRef<[u8]> {
    let (msg, space): (NetworkMessageContent<T::Message>, usize) = bincode::serde::decode_from_slice(r.as_ref(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize message")?;

    Ok(msg)
}