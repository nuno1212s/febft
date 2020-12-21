use std::mem::MaybeUninit;
use std::default::Default;

// consider only major version changes, since
// our range of values is limited to 256
const CURRENT_VERSION: u8 = 0;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ReplicaMessagePayload {
    Dummy(Vec<u8>),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ClientMessagePayload {
    Dummy(Vec<u8>),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Header {
    // should be the string `BFT`
    magic: [u8; 3],
    // version number for the protocol
    version: u8,
    // reserved bytes, should be left alone
    // for now
    _reserved: MaybeUninit<[u32; 7]>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ReplicaMessage {
    header: Header,
    payload: ReplicaMessagePayload,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ClientMessage {
    header: Header,
    payload: ClientMessagePayload,
}

impl Header {
    pub(crate) fn is_valid(&self) -> bool {
        self.version == CURRENT_VERSION && &self.magic == b"BFT"
    }

    pub fn version(&self) -> u8 {
        self.version
    }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            magic: *b"BFT",
            version: CURRENT_VERSION,
            _reserved: MaybeUninit::uninit(),
        }
    }
}

macro_rules! impl_message {
    ($name:ident, $payload:ident) => {
        impl $name {
            pub fn header(&self) -> &Header {
                &self.header
            }

            pub fn payload(&self) -> &$payload {
                &self.payload
            }
        }
    }
}

impl_message!{ClientMessage, ClientMessagePayload}
impl_message!{ReplicaMessage, ReplicaMessagePayload}
