use std::io::Write;
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;
use crate::serialization::ProtocolData;

pub(super) fn serialize_message<W, D, P>(w: &mut W, m: &SystemMessage<D, P>) -> Result<()>
    where W: Write + AsMut<[u8]>,
          D: SharedData,
          P: ProtocolData {
    bincode::serde::encode_into_slice(m, w.as_mut(), bincode::config::standard())
        .wrapped_msg(ErrorKind::CommunicationSerialize, "Failed to serialize message")?;

    Ok(())
}