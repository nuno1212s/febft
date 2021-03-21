use std::mem::MaybeUninit;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::communication::socket::Socket;
use crate::bft::communication::NodeId;
use crate::bft::error::*;

pub(crate) const CURRENT_VERSION: u32 = 0;

/// A header that is sent before a message in transit in the wire,
/// therefore a fixed amount of `std::mem::size_of::<Header>()` bytes
/// are read before a message is read. Contains the protocol version,
/// message length, as well as other metadata.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Header {
    // the protocol version.
    pub(crate) version: u32,
    // origin of the message
    pub(crate) from: NodeId,
    // destiny of the message
    pub(crate) to: NodeId,
    // length of the payload
    pub(crate) length: u64,
    // sign(hash(version + from + to + length + serialize(payload)))
    pub(crate) signature: (),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Message {
    pub(crate) header: Header,
    pub(crate) payload: MessagePayload,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum MessagePayload {
    System(SystemMessage),
    ConnectedTx(NodeId, Socket),
    ConnectedRx(NodeId, Socket),
    Error(Error),
}

impl Header {
    pub(crate) fn is_valid(&self) -> bool {
        self.version == CURRENT_VERSION
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl Message {
    // TODO: add header elements, like source, destiny, etc
    pub fn new(payload: MessagePayload) -> Message {
        let header = Header {
            version: CURRENT_VERSION,
        };
        Message { header, payload }
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn payload(&self) -> &$payload {
        &self.payload
    }

    pub fn into_inner(self) -> (Header, $payload) {
        (self.header, self.payload)
    }
}

impl_message!{ClientMessage, ClientMessagePayload}
impl_message!{ReplicaMessage, ReplicaMessagePayload}
