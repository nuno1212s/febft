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

/// A message to be sent over the wire. The payload should be a serialized
/// SystemMessage, for correctness.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct WireMessage<'a> {
    pub(crate) header: Header,
    pub(crate) payload: &'a [u8],
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum Message {
    System(SystemMessage),
    ConnectedTx(NodeId, Socket),
    ConnectedRx(NodeId, Socket),
    Error(Error),
}

impl Header {
    pub fn version(&self) -> u32 {
        self.version
    }
}

impl<'a> WireMessage<'a> {
    /// Constructs a new message to be sent over the wire.
    pub fn new(from: NodeId, to: NodeId, payload: &'a [u8], signature: ()) -> Self {
        // TODO: sign the message
        let header = Header {
            version: CURRENT_VERSION,
            length: payload.len() as u64,
            signature,
            from,
            to,
        };
        Self { header, payload }
    }

    pub fn into_inner(self) -> (Header, &'a [u8]) {
        (self.header, self.payload)
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn payload(&self) -> &'a [u8] {
        &self.payload
    }

    pub fn is_valid(&self) -> bool {
        // TODO: verify signature, etc
        self.header.version == CURRENT_VERSION
    }
}
