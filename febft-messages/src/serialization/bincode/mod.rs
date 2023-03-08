use std::io::{Read, Write};
use febft_communication::message::NetworkMessageContent;
use febft_communication::serialize::Serializable;
use febft_common::error::*;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;
use crate::serialization::ProtocolData;

pub(super) fn serialize_message<W, D, P>(w: &mut W, m: &SystemMessage<D, P>) -> Result<()>
    where W: Write + AsMut<[u8]>,
          D: SharedData,
          P: ProtocolData {
    bincode::encode_into_std_write(m, w, bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize message")?;

    Ok(())
}

pub fn deserialize_message<R, D, P>(
    r: R
) -> Result<SystemMessage<D, P>> where R: Read + AsRef<[u8]>,
                                       D: SharedData,
                                       P: ProtocolData {
    let (msg, _) = bincode::borrow_decode_from_slice(r.as_ref(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to deserialize with bincode")?;


    Ok(msg)
}