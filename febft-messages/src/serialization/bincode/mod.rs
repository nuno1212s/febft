use std::io::{Read, Write};
use febft_communication::message::NetworkMessageContent;
use febft_communication::serialize::Serializable;
use febft_common::error::*;

pub fn serialize_message<T, W>(
    m: &NetworkMessageContent<T>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsRef<[u8]>,
    T: Serializable {
    bincode::encode_into_slice(m, w, bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize with bincode")
}

pub fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T>> where T: Serializable, R: Read + AsRef<[u8]> {
    bincode::decode_from_slice(r, bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize with bincode")
}