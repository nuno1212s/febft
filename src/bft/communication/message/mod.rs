//! This module contains types associated with messages traded
//! between the system processes.

use std::io;
use std::mem::MaybeUninit;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use smallvec::{
    SmallVec,
    Array,
};
use async_tls::{
    server::TlsStream as TlsStreamSrv,
    client::TlsStream as TlsStreamCli,
};
use futures::io::{
    AsyncWriteExt,
    AsyncWrite,
};

use crate::bft::crypto::signature::{
    Signature,
    PublicKey,
    KeyPair,
};
use crate::bft::crypto::hash::{
    Context,
    Digest,
};
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
    // sign(hash(le(version) + le(from) + le(to) + le(length) + serialize(payload)))
    pub(crate) signature: [u8; Signature::LENGTH],
}

/// A message to be sent over the wire. The payload should be a serialized
/// `SystemMessage`, for correctness.
#[derive(Debug)]
pub struct WireMessage<'a> {
    pub(crate) header: Header,
    pub(crate) payload: &'a [u8],
}

/// A generic `WireMessage`, for different `AsRef<[u8]>` types.
#[derive(Clone, Debug)]
pub struct OwnedWireMessage<T> {
    pub(crate) header: Header,
    pub(crate) payload: T,
}

/// The `Message` type encompasses all the messages traded between different
/// asynchronous tasks in the system.
///
pub enum Message<O, P> {
    /// Client requests and process sub-protocol messages.
    System(Header, SystemMessage<O, P>),
    /// A client with id `NodeId` has finished connecting to the socket `Socket`.
    /// This socket should only perform write operations.
    ConnectedTx(NodeId, TlsStreamCli<Socket>),
    /// A client with id `NodeId` has finished connecting to the socket `Socket`.
    /// This socket should only perform read operations.
    ConnectedRx(NodeId, TlsStreamSrv<Socket>),
    /// Send half of node with id `NodeId` has disconnected.
    DisconnectedTx(NodeId),
    /// Receive half of node with id `Some(NodeId)` has disconnected.
    ///
    /// The id is only equal to `None` during a `Node` bootstrap process.
    DisconnectedRx(Option<NodeId>),
    /// The request of a client with id `NodeId` has finished executing.
    ///
    /// The payload delivered to the client is `P`.
    ExecutionFinished(NodeId, Signature, P),
}

/// A `SystemMessage` corresponds to a message regarding one of the SMR
/// sub-protocols.
///
/// This can be either a `Request` from a client, a `Consensus` message,
/// or even `ViewChange` messages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum SystemMessage<O, P> {
    Request(RequestMessage<O>),
    Reply(ReplyMessage<P>),
    Consensus(ConsensusMessage),
}

/// Represents a request from a client.
///
/// The `O` type argument symbolizes the client operation to be performed
/// over the replicated state.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct RequestMessage<O> {
    operation: O,
}

/// Represents a reply to a client.
///
/// The `P` type argument symbolizes the response payload.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ReplyMessage<P> {
    payload: P,
}

/// Represents a message from the consensus sub-protocol.
///
/// Different types of consensus messages are represented in the `ConsensusMessageKind`
/// type.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone)]
pub struct ConsensusMessage {
    seq: i32,
    kind: ConsensusMessageKind,
}

/// Represents one of many different consensus stages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Copy, Clone)]
pub enum ConsensusMessageKind {
    /// Pre-prepare a request, according to the BFT protocol.
    /// The `Signature` represens the signed hash of the
    /// serialized request payload and respective header.
    PrePrepare(Signature),
    /// Prepare a request.
    Prepare,
    /// Commit a request, signaling the system is almost ready
    /// to execute it.
    Commit,
}

impl<O> RequestMessage<O> {
    /// Creates a new `RequestMessage`.
    pub fn new(operation: O) -> Self {
        Self { operation }
    }

    /// Returns a reference to the operation of type `O`.
    pub fn operation(&self) -> &O {
        &self.operation
    }

    /// Unwraps this `RequestMessage`.
    pub fn into_inner(self) -> O {
        self.operation
    }
}

impl<P> ReplyMessage<P> {
    /// Creates a new `ReplyMessage`.
    pub fn new(payload: P) -> Self {
        Self { payload }
    }

    /// Returns a reference to the payload of type `P`.
    pub fn payload(&self) -> &P {
        &self.payload
    }
}

impl ConsensusMessage {
    /// Creates a new `ConsensusMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: i32, kind: ConsensusMessageKind) -> Self {
        Self { seq, kind }
    }

    /// Returns the sequence number of this consensus message.
    pub fn sequence_number(&self) -> i32 {
        self.seq
    }

    /// Returns a reference to the consensus message kind.
    pub fn kind(&self) -> &ConsensusMessageKind {
        &self.kind
    }
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

    /// Reports the current version of the wire protocol,
    /// i.e. `WireMessage::CURRENT_VERSION`.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The originating `NodeId`.
    pub fn from(&self) -> NodeId {
        self.from.into()
    }

    /// The destiny `NodeId`.
    pub fn to(&self) -> NodeId {
        self.to.into()
    }

    /// The length of the payload associated with this `Header`.
    pub fn payload_length(&self) -> usize {
        self.length as usize
    }

    /// The signature of this `Header` and associated payload.
    pub fn signature(&self) -> Signature {
        Signature::from_bytes(&self.signature[..]).unwrap()
    }
}

impl From<WireMessage<'_>> for OwnedWireMessage<Box<[u8]>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: Vec::from(wm.payload).into_boxed_slice(),
        }
    }
}

impl From<WireMessage<'_>> for OwnedWireMessage<Vec<u8>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: Vec::from(wm.payload),
        }
    }
}

impl<T: Array<Item = u8>> From<WireMessage<'_>> for OwnedWireMessage<SmallVec<T>> {
    fn from(wm: WireMessage<'_>) -> Self {
        OwnedWireMessage {
            header: wm.header,
            payload: SmallVec::from(wm.payload),
        }
    }
}

impl<T: AsRef<[u8]>> OwnedWireMessage<T> {
    /// Returns a reference to a `WireMessage`.
    pub fn borrowed<'a>(&'a self) -> WireMessage<'a> {
        WireMessage {
            header: self.header,
            payload: self.payload.as_ref(),
        }
    }
}

impl<'a> WireMessage<'a> {
    /// The current version of the wire protocol.
    pub const CURRENT_VERSION: u32 = 0;

    /// Wraps a `Header` and a byte array payload into a `WireMessage`.
    pub fn from_parts(header: Header, payload: &'a [u8]) -> Result<Self> {
        let wm = Self { header, payload };
        if !wm.is_valid(None) {
            return Err(Error::simple(ErrorKind::CommunicationMessage));
        }
        Ok(wm)
    }

    /// Constructs a new message to be sent over the wire.
    pub fn new(from: NodeId, to: NodeId, payload: &'a [u8], sk: Option<&KeyPair>) -> Self {
        let signature = sk
            .map(|sk| {
                let signature = Self::sign_parts(
                    sk,
                    from.into(),
                    to.into(),
                    payload,
                );
                // safety: signatures have repr(transparent)
                unsafe { std::mem::transmute(signature) }
            })
            .unwrap_or([0; Signature::LENGTH]);
        let (from, to) = (from.into(), to.into());
        let header = Header {
            version: Self::CURRENT_VERSION,
            length: payload.len() as u64,
            signature,
            from,
            to,
        };
        Self { header, payload }
    }

    fn digest_parts(from: u32, to: u32, payload: &[u8]) -> Digest {
        // sign(hash(le(version) + le(from) + le(to) + le(length) + serialize(payload)))
        let mut ctx = Context::new();

        let buf = Self::CURRENT_VERSION.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = from.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = to.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = (payload.len() as u64).to_le_bytes();
        ctx.update(&buf[..]);

        if payload.len() > 0 {
            ctx.update(payload);
        }
        ctx.finish()
    }

    fn sign_parts(sk: &KeyPair, from: u32, to: u32, payload: &[u8]) -> Signature {
        let digest = Self::digest_parts(from, to, payload);
        // XXX: unwrap() should always work, much like heap allocs
        // should always work
        sk.sign(digest.as_ref()).unwrap()
    }

    fn verify_parts(
        pk: &PublicKey,
        sig: &Signature,
        from: u32,
        to: u32,
        payload: &[u8],
    ) -> Result<()> {
        let digest = Self::digest_parts(from, to, payload);
        pk.verify(digest.as_ref(), sig)
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
    /// checking its signature, if a `PublicKey` is provided.
    pub fn is_valid(&self, public_key: Option<&PublicKey>) -> bool {
        let preliminary_check_failed =
            self.header.version != WireMessage::CURRENT_VERSION
            || self.header.length != self.payload.len() as u64;
        if preliminary_check_failed {
            return false;
        }
        public_key
            .map(|pk| {
                // unwrap() should be safe because of the `Header`
                let signature = Signature::from_bytes(&self.header.signature[..])
                    .unwrap();
                Self::verify_parts(
                    pk,
                    &signature,
                    self.header.from,
                    self.header.to,
                    self.payload,
                ).is_ok()
            })
            .unwrap_or(true)
    }

    /// Serialize a `WireMessage` into an async writer.
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, mut w: W) -> io::Result<()> {
        let mut buf = [0; Header::LENGTH];
        self.header.serialize_into(&mut buf[..]).unwrap();

        // FIXME: switch to vectored writes?
        w.write_all(&buf[..]).await?;
        if self.payload.len() > 0 {
            w.write_all(&self.payload).await?;
        }

        Ok(())
    }

    /// Converts this `WireMessage` into an owned one.
    pub fn with_owned_buffer<T: AsRef<[u8]>>(self, buf: T) -> Option<OwnedWireMessage<T>> {
        let buf_p = buf.as_ref()[0] as *const u8;
        let payload_p = &self.payload[0] as *const u8;

        // both point to the same memory region, safe
        if buf_p == payload_p {
            Some(OwnedWireMessage {
                header: self.header,
                payload: buf,
            })
        } else {
            None
        }
    }
}

impl<O, P> Message<O, P> {
    /// Returns the `Header` of this message, if it is
    /// a `SystemMessage`.
    pub fn header(&self) -> Result<&Header> {
        match self {
            Message::System(ref h, _) =>
                Ok(h),
            Message::ConnectedTx(_, _) =>
                Err("Expected System found ConnectedTx")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::ConnectedRx(_, _) =>
                Err("Expected System found ConnectedRx")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::DisconnectedTx(_) =>
                Err("Expected System found DisconnectedTx")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::DisconnectedRx(_) =>
                Err("Expected System found DisconnectedRx")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::ExecutionFinished(_, _, _) =>
                Err("Expected System found ExecutionFinished")
                    .wrapped(ErrorKind::CommunicationMessage),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bft::communication::message::{WireMessage, Header};
    use crate::bft::crypto::signature::Signature;

    #[test]
    fn test_header_serialize() {
        let old_header = Header {
            version: WireMessage::CURRENT_VERSION,
            signature: [0; Signature::LENGTH],
            from: 0,
            to: 3,
            length: 0,
        };
        let mut buf = [0; Header::LENGTH];
        old_header.serialize_into(&mut buf[..])
            .expect("Serialize failed");
        let new_header = Header::deserialize_from(&buf[..])
            .expect("Deserialize failed");
        assert_eq!(old_header, new_header);
    }
}
