use std::mem::MaybeUninit;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::crypto::signature::Signature;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::NodeId;
use crate::bft::error::*;

pub(crate) const CURRENT_VERSION: u32 = 0;

/// The size of the `Header` in bytes.
pub const HEADER_LENGTH: usize = std::mem::size_of::<Header>();

/// A header that is sent before a message in transit in the wire,
/// therefore a fixed amount of `std::mem::size_of::<Header>()` bytes
/// are read before a message is read. Contains the protocol version,
/// message length, as well as other metadata.
// TODO: https://doc.rust-lang.org/reference/conditional-compilation.html#target_endian
//       conditionally compile on big endian systems,
//       always serialize in little endian format;
//       make sure the signature length has a fixed size!
//       ring uses variable signature length, maybe add another
//       container type of e.g. 1024 bits
#[derive(Debug, Copy, Clone)]
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
pub struct WireMessage<'a> {
    pub(crate) header: Header,
    pub(crate) payload: &'a [u8],
}

pub enum Message<O> {
    System(SystemMessage<O>),
    ConnectedTx(NodeId, Socket),
    ConnectedRx(NodeId, Socket),
    Error(Error),
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum SystemMessage<O> {
    Request(RequestMessage<O>),
    Consensus(ConsensusMessage),
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct RequestMessage<O> {
    id: NodeId,
    operation: O,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ConsensusMessage {
    seq: i32,
    from: NodeId,
    kind: ConsensusMessageKind,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ConsensusMessageKind {
    PrePrepare((/* hash digest type here */)),
    Prepare,
    Commit,
}

impl Header {
    fn serialize_into_unchecked(self, buf: &mut [u8]) {
        #[cfg(target_endian = "big")]
        {
            self.version = self.version.to_le();
            self.from = self.from.to_le();
            self.to = self.to.to_le();
            self.length = self.length.to_le();
        }
        let hdr: [u8; HEADER_LENGTH] = unsafe {
            std::mem::transmute(self)
        };
        buf.copy_from_slice(&hdr[..]);
    }

    pub fn serialize_into(self, buf: &mut [u8]) -> Result<()> {
        if buf.len() < HEADER_LENGTH {
            return Err("Buffer is too short to serialize into")
                .wrapped(ErrorKind::CommunicationMessage);
        }
        Ok(self.serialize_into_unchecked(buf))
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

impl<'a> WireMessage<'a> {
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
