use std::io::{Read, Write};
use febft_communication::message::NetworkMessageContent;
use febft_communication::serialize::Serializable;
use febft_common::error::*;
use crate::messages::SystemMessage;

pub(super) fn serialize_message<W, D, P>(w: &mut W, m: &SystemMessage<D, P>) -> Result<()>
    where W: Write + AsMut<[u8]>,
          D: SharedData,
          P: ProtocolData {
    bincode::encode_into_slice(m, w.as_mut(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize message")?;

    Ok(())
}

pub fn deserialize_message<T, R>(
    r: R
) -> Result<NetworkMessageContent<T>> where T: Serializable, R: Read + AsRef<[u8]> {
    bincode::borrow_decode_from_slice(r, bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize with bincode")
}