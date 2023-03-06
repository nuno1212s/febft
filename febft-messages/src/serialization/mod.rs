use std::io::{Read, Write};
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::executable::Service;
use crate::messages::SystemMessage;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

/*
#[cfg(feature = "serialize_serde")]
pub mod serde;

#[cfg(feature = "serialize_bincode")]
pub mod bincode;
*/

impl<S, P> Serializable for SystemMessage<S, P> where S: Service, P: Serializable {
    type Message = SystemMessage<S, P>;

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()> {
        #[cfg(feature = "serialize_capnp")]
        capnp::serialize_message(w, message)?;

        /*
        #[cfg(feature = "serialize_bincode")]
        bincode::serialize_message(message, w)?;
        */

        Ok(())
    }

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message> {
        #[cfg(feature="serialize_capnp")]
        capnp::deserialize_message(r)
    }
}
