use std::io::{Read, Write};
use ::serde::{Deserialize, Serialize};
use febft_common::error::*;
use febft_communication::message::NetworkMessageContent;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;

// #[cfg(feature = "serialize_capnp")]
// pub mod capnp;

#[cfg(feature = "serialize_serde")]
pub mod serde;

// #[cfg(feature = "serialize_bincode")]
// pub mod bincode;

pub trait ProtocolData {

    type Message : for<'a> Deserialize<'a> + Serialize + Send + Clone;

    fn serialize<W: Write>(w: &mut W, message: &Self::Message) -> Result<()>;

    fn deserialize<R: Read>(r: R) -> Result<Self::Message>;

    //fn serialize_full<W: Write, D: SharedData, P: Self> (w: &mut W, message: SystemMessage<D, P>) -> Result<()>;

}

impl<D, P> Serializable for SystemMessage<D, P> where D: SharedData, P: ProtocolData {
    type Message = SystemMessage<D, P>;

    fn serialize<W: Write + AsRef<[u8]> + AsMut<[u8]>>(w: &mut W, message: &Self::Message) -> Result<()> {
        // #[cfg(feature = "serialize_capnp")]
        // capnp::serialize_message(w, message)?;

        // #[cfg(feature = "serialize_bincode")]
        // bincode::serialize_message(message, w)?;

        #[cfg(feature = "serialize_serde")]
        serde::serialize_message::<W, D, P>(w, message)?;

        Ok(())
    }

    fn deserialize_message<R: Read>(r: R) -> Result<Self::Message> {
        // #[cfg(feature="serialize_capnp")]
        // capnp::deserialize_message(r)

        Err(Error::simple(ErrorKind::CommunicationSerialize))
    }

}
