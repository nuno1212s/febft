//! This module is responsible for serializing wire messages in `febft`.
//!
//! If using the [Cap'n'Proto](https://capnproto.org/capnp-tool.html) backend,
//! users are expected to implement their own schema. Users can opt to enable
//! the [serde](https://serde.rs/) backend instead, which has a much more flexible
//! API, but performs worse, in general, because it doesn't have a
//! zero-copy architecture, like Cap'n'Proto.
//!
//! Configuring one over the other is done with the following feature flags:
//!
//! - `serialize_capnp`
//! - `serialize_serde_BACKEND`, where `BACKEND` may be `bincode`, for instance.
//!   Consult the `Cargo.toml` file for more alternatives.

// TODO: serialize express mode for SystemMessage<Vec<u8>>
// or SystemMessage<SmallVec<[u8; _]>>
//
// steps:
// ======
// 1) new Vec<u8> buffer
// 2) write the following:
//   2.1) message tag (ConsensusMessage, RequestMessage, ...)
//   2.2) message payload length if is request or reply
//   2.2) message payload if is request or reply
//     2.2.1) just copy input buffer from SystemMessage
// 3) flush buffer to the wire
//
// this is useful for HTTP and other text/binary protocols

#[cfg(feature = "serialize_capnp")]
mod capnp;

#[cfg(feature = "serialize_serde")]
mod serde;

#[cfg(feature = "serialize_serde")]
use ::serde::{Serialize, Deserialize};

use std::io::{Read, Write};

use crate::bft::error::*;
use crate::bft::communication::message::SystemMessage;

#[cfg(feature = "serialize_capnp")]
pub use self::capnp::{ToCapnp, FromCapnp};

/// Serialize a wire message into the writer `W`.
///
/// Once the operation is finished, the writer is returned.
pub fn serialize_message<O, P, W>(w: W, m: &SystemMessage<O, P>) -> Result<W>
where
    O: Marshal<P>,
    W: Write,
{
    #[cfg(feature = "serialize_capnp")]
    { capnp::serialize_message(w, m) }

    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::serialize_message(w, m) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::serialize_message(w, m) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::serialize_message(w, m) }
}

/// Deserialize a wire message from a reader `R`.
pub fn deserialize_message<O, P, R>(r: R) -> Result<SystemMessage<O, P>>
where
    P: Unmarshal<O>,
    R: Read,
{
    #[cfg(feature = "serialize_capnp")]
    { capnp::deserialize_message(r) }

    #[cfg(feature = "serialize_serde_bincode")]
    { serde::bincode::deserialize_message(r) }

    #[cfg(feature = "serialize_serde_messagepack")]
    { serde::messagepack::deserialize_message(r) }

    #[cfg(feature = "serialize_serde_cbor")]
    { serde::cbor::deserialize_message(r) }
}

////////////////////////////////////////////////////////////
//
//    WARNING !! !! !!
//    ================
//
//    gore below :-)
//
////////////////////////////////////////////////////////////

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_serde")]
pub trait Marshal<P>
where
    Self: Serialize,
    P: Serialize,
{}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_capnp")]
pub trait Marshal<P>: ToCapnp<P = P> {}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_serde")]
pub trait Unmarshal<O>
where
    O: for<'de> Deserialize<'de>,
    Self: for<'de> Deserialize<'de>,
{}

/// Marker trait to abstract between different serialization
/// crates.
#[cfg(feature = "serialize_capnp")]
pub trait Unmarshal<O>: FromCapnp<O = O> {}

#[cfg(feature = "serialize_serde")]
impl<O, P> Marshal<P> for O
where
    O: Serialize,
    P: Serialize,
{}

#[cfg(feature = "serialize_capnp")]
impl<O, P> Marshal<P> for O
where
    O: ToCapnp<P = P>,
{}

#[cfg(feature = "serialize_serde")]
impl<O, P> Unmarshal<O> for P
where
    O: for<'de> Deserialize<'de>,
    P: for<'de> Deserialize<'de>,
{}

#[cfg(feature = "serialize_capnp")]
impl<O, P> Unmarshal<O> for P
where
    P: FromCapnp<O = O>,
{}

// XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX-XXX

#[cfg(test)]
mod tests {
    use super::{
        serialize_message,
        deserialize_message,
    };
    use crate::bft::communication::message::{
        SystemMessage,
        RequestMessage,
    };

    #[test]
    fn test_serialize() {
        let mut buf = Vec::new();

        let m1 = RequestMessage::new(());
        let m1 = SystemMessage::Request(m1);

        serialize_message(&mut buf, &m1).unwrap();
        let m2: SystemMessage<(), ()> = deserialize_message(&buf[..]).unwrap();

        match (m1, m2) {
            (SystemMessage::Request(_), SystemMessage::Request(_)) => (),
            _ => panic!("Deserialize failed"),
        }
    }
}
