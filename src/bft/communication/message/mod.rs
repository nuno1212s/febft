use std::mem::MaybeUninit;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::crypto::signature::Signature;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::NodeId;
use crate::bft::error::*;

/// A header that is sent before a message in transit in the wire.
///
/// A fixed amount of `Header::LENGTH` bytes are read before
/// a message is read. Contains the protocol version, message
/// length, as well as other metadata.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(C)]
pub struct Header {
    // the protocol version.
    pub(crate) version: u32,
    // origin of the message
    pub(crate) from: u32,
    // destiny of the message
    pub(crate) to: u32,
    // length of the payload
    pub(crate) length: u64,
    // sign(hash(version + from + to + length + serialize(payload)))
    pub(crate) signature: [u8; Signature::LENGTH],
}

/// A message to be sent over the wire. The payload should be a serialized
/// `SystemMessage`, for correctness.
#[derive(Debug)]
pub struct WireMessage<'a> {
    pub(crate) header: Header,
    pub(crate) payload: &'a [u8],
}

/// The `Message` type encompasses all the messages traded between different
/// asynchronous tasks in the system.
pub enum Message<O> {
    System(SystemMessage<O>),
    ConnectedTx(NodeId, Socket),
    ConnectedRx(NodeId, Socket),
    Error(Error),
}

/// A `SystemMessage` corresponds to a message regarding one of the SMR
/// sub-protocols.
///
/// This can be either a `Request` from a client, a `Consensus` message,
/// or even `ViewChange` messages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum SystemMessage<O> {
    Request(RequestMessage<O>),
    Consensus(ConsensusMessage),
}

/// Represents a request from a client.
///
/// The `O` type argument symbolizes the client operation to be performed
/// over the replicated state.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct RequestMessage<O> {
    id: NodeId,
    operation: O,
}

/// Represents a message from the consensus sub-protocol.
///
/// Different types of consensus messages are represented in the `ConsensusMessageKind`
/// type.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ConsensusMessage {
    seq: i32,
    from: NodeId,
    kind: ConsensusMessageKind,
}

/// Represents one of many different consensus stages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ConsensusMessageKind {
    PrePrepare((/* hash digest type here */)),
    Prepare,
    Commit,
}

// FIXME: perhaps use references for serializing and deserializing,
// to save a stack allocation? probably overkill
impl Header {
    /// The size of the memory representation of the `Header` in bytes.
    pub const LENGTH: usize = std::mem::size_of::<Self>();

    unsafe fn serialize_into_unchecked(self, buf: &mut [u8]) {
        #[cfg(target_endian = "big")]
        {
            self.version = self.version.to_le();
            self.from = self.from.to_le();
            self.to = self.to.to_le();
            self.length = self.length.to_le();
        }
        let hdr: [u8; Self::LENGTH] = std::mem::transmute(self);
        (&mut buf[..Self::LENGTH]).copy_from_slice(&hdr[..]);
    }

    /// Serialize a `Header` into a byte buffer of appropriate size.
    pub fn serialize_into(self, buf: &mut [u8]) -> Result<()> {
        if buf.len() < Self::LENGTH {
            return Err("Buffer is too short to serialize into")
                .wrapped(ErrorKind::CommunicationMessage);
        }
        Ok(unsafe { self.serialize_into_unchecked(buf) })
    }

    unsafe fn deserialize_from_unchecked(buf: &[u8]) -> Self {
        let mut hdr: [u8; Self::LENGTH] = {
            let hdr = MaybeUninit::uninit();
            hdr.assume_init()
        };
        (&mut hdr[..]).copy_from_slice(&buf[..Self::LENGTH]);
        #[cfg(target_endian = "big")]
        {
            hdr.version = hdr.version.to_be();
            hdr.from = hdr.from.to_be();
            hdr.to = hdr.to.to_le();
            hdr.length = hdr.length.to_be();
        }
        std::mem::transmute(hdr)
    }

    /// Deserialize a `Header` from a byte buffer of appropriate size.
    pub fn deserialize_from(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::LENGTH {
            return Err("Buffer is too short to deserialize from")
                .wrapped(ErrorKind::CommunicationMessage);
        }
        Ok(unsafe { Self::deserialize_from_unchecked(buf) })
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl<'a> WireMessage<'a> {
    /// The current version of the wire protocol.
    pub const CURRENT_VERSION: u32 = 0;

    /// Constructs a new message to be sent over the wire.
    pub fn new(from: NodeId, to: NodeId, payload: &'a [u8], sig: Signature) -> Self {
        let signature = unsafe {
            let mut s: MaybeUninit<[u8; Signature::LENGTH]> =
                MaybeUninit::uninit();
            (*s.as_mut_ptr())
                .copy_from_slice(sig.as_ref());
            s.assume_init()
        };
        let (from, to): (u32, u32) = (from.into(), to.into());
        let header = Header {
            version: Self::CURRENT_VERSION,
            length: payload.len() as u64,
            signature,
            from,
            to,
        };
        Self { header, payload }
    }

    /// Retrieve the inner `Header` and payload byte buffer stored
    /// inside the `WireMessage`.
    pub fn into_inner(self) -> (Header, &'a [u8]) {
        (self.header, self.payload)
    }

    /// Returns a reference to the `Header` of the `WireMessage`.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns a reference to the payload bytes of the `WireMessage`.
    pub fn payload(&self) -> &'a [u8] {
        &self.payload
    }

    /// Checks for the correctness of the `WireMessage`. This implies
    /// checking signatures and other metadata.
    pub fn is_valid(&self) -> bool {
        // TODO: verify signature, etc
        self.header.version == Self::CURRENT_VERSION
    }
}

#[cfg(test)]
mod tests {
    use crate::bft::communication::message::{WireMessage, Header};
    use crate::bft::crypto::signature::Signature;
    use crate::bft::communication::NodeId;

    #[test]
    fn test_header_serialize() {
        let signature = Signature::from_bytes(&[0; Signature::LENGTH][..])
            .expect("Invalid signature length");
        let (old_header, _) = WireMessage::new(
            NodeId::from(0),
            NodeId::from(3),
            b"I am a cool payload!",
            signature,
        ).into_inner();
        let mut buf = [0; Header::LENGTH];
        old_header.serialize_into(&mut buf[..])
            .expect("Serialize failed");
        let new_header = Header::deserialize_from(&buf[..])
            .expect("Deserialize failed");
        assert_eq!(old_header, new_header);
    }
}
