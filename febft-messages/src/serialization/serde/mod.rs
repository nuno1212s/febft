use std::io::Write;
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::executable::Service;
use crate::messages::SystemMessage;

pub(super) fn serialize_message<W, S, P>(w: &mut W, msg: &SystemMessage<S, P>) -> Result<()>
    where W: Write + AsRef<[u8]>, S: Service, P: Serializable {
    todo!()
}