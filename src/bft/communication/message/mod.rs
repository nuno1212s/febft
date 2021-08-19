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
use crate::bft::ordering::{
    SeqNo,
    Orderable,
};
use crate::bft::consensus::log::CollectData;
use crate::bft::executable::UpdateBatchReplies;
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::socket::Socket;
use crate::bft::communication::NodeId;
use crate::bft::timeouts::TimeoutKind;
use crate::bft::sync::LeaderCollects;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;

// convenience type
pub type StoredSerializedSystemMessage<D> = StoredMessage<
    SerializedMessage<
        SystemMessage<
            <D as SharedData>::State,
            <D as SharedData>::Request,
            <D as SharedData>::Reply
        >
    >
>;

pub struct SerializedMessage<M> {
    original: M,
    raw: Vec<u8>,
}

impl<M> SerializedMessage<M> {
    pub fn new(original: M, raw: Vec<u8>) -> Self {
        Self { original, raw }
    }

    pub fn original(&self) -> &M {
        &self.original
    }

    pub fn raw(&self) -> &Vec<u8> {
        &self.raw
    }

    pub fn into_inner(self) -> (M, Vec<u8>) {
        (self.original, self.raw)
    }
}

/// Contains a system message as well as its respective header.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct StoredMessage<M> {
    header: Header,
    message: M,
}

impl<M> StoredMessage<M> {
    /// Constructs a new `StoredMessage`.
    pub fn new(header: Header, message: M) -> Self {
        Self { header, message }
    }

    /// Returns the stored message's header.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns the stored system message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Return the inner types of this `StoredMessage`.
    pub fn into_inner(self) -> (Header, M) {
        (self.header, self.message)
    }
}

/*
impl<S, O, P> StoredMessage<SystemMessage<S, O, P>> {
    /// Convert the inner `SystemMessage` into a `ConsensusMessage`,
    /// if possible, else return the original message.
    pub fn into_consensus(self) -> Either<Self, StoredMessage<ConsensusMessage>> {
        let (header, message) = self.into_inner();
        match message {
            SystemMessage::Consensus(message) => {
                Right(StoredMessage::new(header, message))
            },
            message => {
                Left(StoredMessage::new(header, message))
            },
        }
    }
}
*/

/// A header that is sent before a message in transit in the wire.
///
/// A fixed amount of `Header::LENGTH` bytes are read before
/// a message is read. Contains the protocol version, message
/// length, as well as other metadata.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(C, packed)]
pub struct Header {
    // manually align memory for cross platform compat
    pub(crate) _align: u32,
    // the protocol version
    pub(crate) version: u32,
    // origin of the message
    pub(crate) from: u32,
    // destination of the message
    pub(crate) to: u32,
    // a random number
    pub(crate) nonce: u64,
    // length of the payload
    pub(crate) length: u64,
    // the digest of the serialized payload
    pub(crate) digest: [u8; Digest::LENGTH],
    // sign(hash(le(version) + le(from) + le(to) + le(nonce) + le(length) + hash(serialize(payload))))
    pub(crate) signature: [u8; Signature::LENGTH],
}

#[cfg(feature = "serialize_serde")]
impl serde::Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // TODO: improve this, to avoid allocating a `Vec`
        let mut bytes = vec![0; Self::LENGTH];
        let hdr: &[u8; Self::LENGTH] = unsafe { std::mem::transmute(self) };
        bytes.copy_from_slice(&hdr[..]);
        serde_bytes::serialize(&bytes, serializer)
    }
}

#[cfg(feature = "serialize_serde")]
impl<'de> serde::Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Header, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        let mut hdr: [u8; Self::LENGTH] = [0; Self::LENGTH];
        hdr.copy_from_slice(&bytes);
        Ok(unsafe { std::mem::transmute(hdr) })
    }
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
pub enum Message<S, O, P> {
    /// Client requests and process sub-protocol messages.
    System(Header, SystemMessage<S, O, P>),
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
    /// A batch of client requests has finished executing.
    ///
    /// The type of the payload delivered to the clients is `P`.
    ExecutionFinished(UpdateBatchReplies<P>),
    /// Same as `Message::ExecutionFinished`, but includes a snapshot of
    /// the application state.
    ///
    /// This is useful for local checkpoints.
    ExecutionFinishedWithAppstate(UpdateBatchReplies<P>, S),
    /// We received a timeout from the timeouts layer.
    Timeout(TimeoutKind),
}

/// A `SystemMessage` corresponds to a message regarding one of the SMR
/// sub-protocols.
///
/// This can be either a `Request` from a client, a `Consensus` message,
/// or even `ViewChange` messages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum SystemMessage<S, O, P> {
    // TODO: ReadRequest,
    Request(RequestMessage<O>),
    Reply(ReplyMessage<P>),
    Consensus(ConsensusMessage),
    Cst(CstMessage<S, O>),
    ViewChange(ViewChangeMessage<O>),
    ForwardedRequests(ForwardedRequestsMessage<O>),
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ForwardedRequestsMessage<O> {
    inner: Vec<StoredMessage<RequestMessage<O>>>,
}

impl<O> ForwardedRequestsMessage<O> {
    /// Creates a new `ForwardedRequestsMessage`, containing the given client requests.
    pub fn new(inner: Vec<StoredMessage<RequestMessage<O>>>) -> Self {
        Self { inner }
    }

    /// Returns the client requests contained in this `ForwardedRequestsMessage`.
    pub fn into_inner(self) -> Vec<StoredMessage<RequestMessage<O>>> {
        self.inner
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ViewChangeMessage<O> {
    view: SeqNo,
    kind: ViewChangeMessageKind<O>,
}

impl<O> Orderable for ViewChangeMessage<O> {
    /// Returns the sequence number of the view this message refers to.
    fn sequence_number(&self) -> SeqNo {
        self.view
    }
}

impl<O> ViewChangeMessage<O> {
    /// Creates a new `ViewChangeMessage`, pertaining to the view
    /// with sequence number `view`, and of the kind `kind`.
    pub fn new(view: SeqNo, kind: ViewChangeMessageKind<O>) -> Self {
        Self { view, kind }
    }

    /// Returns a reference to the view change message kind.
    pub fn kind(&self) -> &ViewChangeMessageKind<O> {
        &self.kind
    }

    /// Returns an owned view change message kind.
    pub fn into_kind(self) -> ViewChangeMessageKind<O> {
        self.kind
    }

    /// Takes the collects embedded in this view change message, if they are available.
    pub fn take_collects(&mut self) -> Option<LeaderCollects<O>> {
        let kind = std::mem::replace(
            &mut self.kind,
            ViewChangeMessageKind::Sync(LeaderCollects::empty()),
        );
        match kind {
            ViewChangeMessageKind::Sync(collects) => Some(collects),
            _ => {
                self.kind = kind;
                None
            },
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ViewChangeMessageKind<O> {
    Stop(Vec<StoredMessage<RequestMessage<O>>>),
    StopData(CollectData),
    Sync(LeaderCollects<O>),
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct CstMessage<S, O> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: CstMessageKind<S, O>,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum CstMessageKind<S, O> {
    RequestLatestConsensusSeq,
    ReplyLatestConsensusSeq(SeqNo),
    RequestState,
    ReplyState(RecoveryState<S, O>),
}

impl<S, O> Orderable for CstMessage<S, O> {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S, O> CstMessage<S, O> {
    /// Creates a new `CstMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, kind: CstMessageKind<S, O>) -> Self {
        Self { seq, kind }
    }

    /// Returns a reference to the state transfer message kind.
    pub fn kind(&self) -> &CstMessageKind<S, O> {
        &self.kind
    }

    /// Takes the recovery state embedded in this cst message, if it is available.
    pub fn take_state(&mut self) -> Option<RecoveryState<S, O>> {
        let kind = std::mem::replace(&mut self.kind, CstMessageKind::RequestState);
        match kind {
            CstMessageKind::ReplyState(state) => Some(state),
            _ => {
                self.kind = kind;
                None
            },
        }
    }
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
    digest: Digest,
    payload: P,
}

/// Represents a message from the consensus sub-protocol.
///
/// Different types of consensus messages are represented in the `ConsensusMessageKind`
/// type.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ConsensusMessage {
    seq: SeqNo,
    view: SeqNo,
    kind: ConsensusMessageKind,
}

/// Represents one of many different consensus stages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ConsensusMessageKind {
    /// Pre-prepare a request, according to the BFT consensus protocol.
    ///
    /// The value `Vec<Digest>` contains a batch of hash digests of the
    /// serialized client requests to be proposed.
    PrePrepare(Vec<Digest>),
    /// Prepare a batch of requests.
    ///
    /// The `Digest` represents the hash of the serialized `PRE-PREPARE`,
    /// where the batch of requests were proposed.
    Prepare(Digest),
    /// Commit a batch of requests, signaling the system is ready
    /// to execute them.
    ///
    /// The `Digest` represents the hash of the serialized `PRE-PREPARE`,
    /// where the batch of requests were proposed.
    Commit(Digest),
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
    pub fn new(digest: Digest, payload: P) -> Self {
        Self { digest, payload }
    }

    /// Returns a reference to the payload of type `P`.
    pub fn payload(&self) -> &P {
        &self.payload
    }

    /// The hash digest of the request associated with
    /// this reply.
    pub fn digest(&self) -> &Digest {
        &self.digest
    }

    /// Unwraps this `ReplyMessage`.
    pub fn into_inner(self) -> (Digest, P) {
        (self.digest, self.payload)
    }
}

impl Orderable for ConsensusMessage {
    /// Returns the sequence number of this consensus message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl ConsensusMessage {
    /// Creates a new `ConsensusMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, view: SeqNo, kind: ConsensusMessageKind) -> Self {
        Self { seq, view, kind }
    }

    /// Returns a reference to the consensus message kind.
    pub fn kind(&self) -> &ConsensusMessageKind {
        &self.kind
    }

    /// Checks if a consensus message refers to the digest of the
    /// proposed value.
    ///
    /// Evidently, this predicate is not defined for `PRE-PREPARE` messages.
    pub fn has_proposed_digest(&self, digest: &Digest) -> Option<bool> {
        match self.kind {
            ConsensusMessageKind::PrePrepare(_) => None,
            ConsensusMessageKind::Prepare(d) | ConsensusMessageKind::Commit(d) => {
                Some(&d == digest)
            },
        }
    }

    /// Returns the sequence number of the view this consensus message belongs to.
    pub fn view(&self) -> SeqNo {
        self.view
    }

    /// Takes the proposed client requests embedded in this consensus message,
    /// if they are available.
    pub fn take_proposed_requests(&mut self) -> Option<Vec<Digest>> {
        let kind = std::mem::replace(
            &mut self.kind,
            ConsensusMessageKind::PrePrepare(Vec::new()),
        );
        match kind {
            ConsensusMessageKind::PrePrepare(v) => Some(v),
            _ => {
                self.kind = kind;
                None
            },
        }
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
            self.nonce = self.nonce.to_le();
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
            hdr.nonce = hdr.nonce.to_be();
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

    /// The destination `NodeId`.
    pub fn to(&self) -> NodeId {
        self.to.into()
    }

    /// The length of the payload associated with this `Header`.
    pub fn payload_length(&self) -> usize {
        self.length as usize
    }

    /// The signature of this `Header` and associated payload.
    pub fn signature(&self) -> &Signature {
        unsafe { std::mem::transmute(&self.signature) }
    }

    /// The digest of the associated payload serialized data.
    pub fn digest(&self) -> &Digest {
        unsafe { std::mem::transmute(&self.digest) }
    }

    /// Hashes the digest of the associated message's payload
    /// with this header's nonce.
    ///
    /// This is useful for attaining a unique identifier for
    /// a particular client request.
    pub fn unique_digest(&self) -> Digest {
        self.digest().entropy(self.nonce.to_le_bytes())
    }

    /// Returns the nonce associated with this `Header`.
    pub fn nonce(&self) -> u64 {
        self.nonce
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
    pub fn new(
        from: NodeId,
        to: NodeId,
        payload: &'a [u8],
        nonce: u64,
        digest: Option<Digest>,
        sk: Option<&KeyPair>,
    ) -> Self {
        let digest = digest
            // safety: digests have repr(transparent)
            .map(|d| unsafe {std::mem::transmute(d) })
            // if payload length is 0
            .unwrap_or([0; Digest::LENGTH]);
        let signature = sk
            .map(|sk| {
                let signature = Self::sign_parts(
                    sk,
                    from.into(),
                    to.into(),
                    nonce,
                    &digest[..],
                );
                // safety: signatures have repr(transparent)
                unsafe { std::mem::transmute(signature) }
            })
            .unwrap_or([0; Signature::LENGTH]);
        let (from, to) = (from.into(), to.into());
        let header = Header {
            _align: 0,
            version: Self::CURRENT_VERSION,
            length: payload.len() as u64,
            signature,
            digest,
            nonce,
            from,
            to,
        };
        Self { header, payload }
    }

    fn digest_parts(from: u32, to: u32, nonce: u64, payload: &[u8]) -> Digest {
        let mut ctx = Context::new();

        let buf = Self::CURRENT_VERSION.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = from.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = to.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = nonce.to_le_bytes();
        ctx.update(&buf[..]);

        let buf = (payload.len() as u64).to_le_bytes();
        ctx.update(&buf[..]);

        ctx.update(payload);
        ctx.finish()
    }

    fn sign_parts(
        sk: &KeyPair,
        from: u32,
        to: u32,
        nonce: u64,
        payload: &[u8],
    ) -> Signature {
        let digest = Self::digest_parts(from, to, nonce, payload);
        // NOTE: unwrap() should always work, much like heap allocs
        // should always work
        sk.sign(digest.as_ref()).unwrap()
    }

    fn verify_parts(
        pk: &PublicKey,
        sig: &Signature,
        from: u32,
        to: u32,
        nonce: u64,
        payload: &[u8],
    ) -> Result<()> {
        let digest = Self::digest_parts(from, to, nonce, payload);
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
                Self::verify_parts(
                    pk,
                    self.header.signature(),
                    self.header.from,
                    self.header.to,
                    self.header.nonce,
                    &self.header.digest[..],
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
        w.flush().await?;

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

impl<S, O, P> Message<S, O, P> {
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
            Message::ExecutionFinished(_) =>
                Err("Expected System found ExecutionFinished")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::ExecutionFinishedWithAppstate(_, _) =>
                Err("Expected System found ExecutionFinishedWithAppstate")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::Timeout(_) =>
                Err("Expected System found Timeout")
                    .wrapped(ErrorKind::CommunicationMessage),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bft::communication::message::{WireMessage, Header};
    use crate::bft::crypto::signature::Signature;
    use crate::bft::crypto::hash::Digest;

    #[test]
    fn test_header_serialize() {
        let old_header = Header {
            _align: 0,
            version: WireMessage::CURRENT_VERSION,
            signature: [0; Signature::LENGTH],
            digest: [0; Digest::LENGTH],
            nonce: 0,
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
