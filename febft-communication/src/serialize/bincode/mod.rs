use std::io::{Read, Write};
use bincode::{Decode, Encode};
use bincode::config::Configuration;
use crate::message::NetworkMessageContent;
use febft_common::error::*;
use crate::serialize::Serializable;


pub fn serialize_message<T, W>(
    m: &NetworkMessageContent<T>,
    w: &mut W,
) -> Result<()> where
    W: Write + AsRef<[u8]>,
    T: Serializable {

    bincode::encode_into_slice(m, w.as_mut(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize with bincode")?;

    match m {
        NetworkMessageContent::System(m) => {
            bincode::encode_into_slice::<&T::Message, Configuration>(m, w.as_mut(), bincode::config::standard())
                .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize with bincode")?;
        }
        NetworkMessageContent::Ping(ping_msg) => {

        }
    }
    Ok(())
}

pub fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T>> where T: Serializable, R: Read + AsRef<[u8]> {
    let (d, _s) = bincode::decode_from_slice::<T::Message, Configuration>(r.as_ref(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize with bincode")?;

    Ok(NetworkMessageContent::System(d))
}