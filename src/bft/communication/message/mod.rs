//! This module contains types associated with messages traded
//! between the system processes.

use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Write;
use std::mem::MaybeUninit;
use bytes::Bytes;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

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

use crate::bft::communication::serialize::{Buf, SharedData};
use crate::bft::communication::NodeId;
use crate::bft::timeouts::{Timeout, TimeoutKind};
use crate::bft::sync::LeaderCollects;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;
use crate::bft::msg_log::decisions::CollectData;
use crate::bft::sync::view::ViewInfo;

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
    raw: Buf,
}

impl<M> SerializedMessage<M> {
    pub fn new(original: M, raw: Buf) -> Self {
        Self { original, raw }
    }

    pub fn original(&self) -> &M {
        &self.original
    }

    pub fn raw(&self) -> &Buf {
        &self.raw
    }

    pub fn into_inner(self) -> (M, Buf) {
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
    // sign(hash(le(from) + le(to) + le(nonce) + hash(serialize(payload))))
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
pub struct WireMessage {
    pub(crate) header: Header,
    pub(crate) payload: Bytes,
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
pub enum Message<S, O, P> where S: Send, O: Send, P: Send {
    /// Client requests and process sub-protocol messages.
    System(Header, SystemMessage<S, O, P>),
    /// Same as `Message::ExecutionFinished`, but includes a snapshot of
    /// the application state.
    ///
    /// This is useful for local checkpoints.
    ExecutionFinishedWithAppstate((SeqNo, S)),
    /// We received a timeout from the timeouts layer.
    Timeout(Timeout),
}

impl<S, O, P> Debug for Message<S, O, P> where S: Send, O: Send, P: Send {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::System(header, msg) => {
                write!(f, "System message {:?} ({:?})", msg, header.digest())
            }
            Message::ExecutionFinishedWithAppstate(_) => {
                write!(f, "Execution finished")
            }
            Message::Timeout(_) => {
                write!(f, "timeout")
            }
        }
    }
}

/// A `SystemMessage` corresponds to a message regarding one of the SMR
/// sub-protocols.
///
/// This can be either a `Request` from a client, a `Consensus` message,
/// or even `ViewChange` messages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum SystemMessage<S, O, P> {
    //Requests that do not need to be logged nor ordered. Notice that
    //The actual request instance is not different from the ordered requests,
    //It is up to the developer of the particular services to use
    //These operations responsibly
    UnOrderedRequest(RequestMessage<O>),
    UnOrderedReply(ReplyMessage<P>),
    Request(RequestMessage<O>),
    Reply(ReplyMessage<P>),
    Consensus(ConsensusMessage<O>),
    FwdConsensus(FwdConsensusMessage<O>),
    //Collaborative state transfer messages
    Cst(CstMessage<S, O>),
    ViewChange(ViewChangeMessage<O>),
    ForwardedRequests(ForwardedRequestsMessage<O>),
    //Observer related messages
    ObserverMessage(ObserverMessage),
    //Ping messages
    Ping(PingMessage),
}

impl<S, O, P> Debug for SystemMessage<S, O, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemMessage::UnOrderedRequest(_req) => {
                write!(f, "Unordered request")
            }
            SystemMessage::UnOrderedReply(_) => {
                write!(f, "Unordered reply")
            }
            SystemMessage::Request(_rq) => {
                write!(f, "Request")
            }
            SystemMessage::Reply(_re) => {
                write!(f, "Reply")
            }
            SystemMessage::Consensus(cs) => {
                match cs.kind() {
                    ConsensusMessageKind::PrePrepare(list) => {
                        write!(f, "Consensus PrePrepare with {}", list.len())
                    }
                    ConsensusMessageKind::Prepare(prepare) => {
                        write!(f, "Consensus prepare {:?}", prepare)
                    }
                    ConsensusMessageKind::Commit(commit) => {
                        write!(f, "Consensus commit {:?}", commit)
                    }
                }
            }
            SystemMessage::Cst(_cst) => {
                write!(f, "Cst")
            }
            SystemMessage::ViewChange(_vchange) => {
                write!(f, "view change")
            }
            SystemMessage::ForwardedRequests(_fr) => {
                write!(f, "forwarded requests")
            }
            SystemMessage::ObserverMessage(_message) => {
                write!(f, "observer message")
            }
            SystemMessage::FwdConsensus(_) => {
                write!(f, "Fwd consensus message")
            }
            SystemMessage::Ping(_) => {
                write!(f, "Ping message")
            }
        }
    }
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
    pub fn take_collects(self) -> Option<LeaderCollects<O>> {
        match self.kind {
            ViewChangeMessageKind::Sync(collects) => Some(collects),
            _ => {
                None
            }
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ViewChangeMessageKind<O> {
    Stop(Vec<StoredMessage<RequestMessage<O>>>),
    StopData(CollectData<O>),
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
            }
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
    session_id: SeqNo,
    operation_id: SeqNo,
    operation: O,
}

/// Represents a reply to a client.
///
/// The `P` type argument symbolizes the response payload.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ReplyMessage<P> {
    session_id: SeqNo,
    operation_id: SeqNo,
    payload: P,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct FwdConsensusMessage<O> {
    header: Header,
    consensus_msg: ConsensusMessage<O>,
}

impl<O> FwdConsensusMessage<O> {
    pub fn new(header: Header, msg: ConsensusMessage<O>) -> Self {
        Self {
            header,
            consensus_msg: msg,
        }
    }

    pub fn header(&self) -> &Header { &self.header }

    pub fn consensus(&self) -> &ConsensusMessage<O> {
        &self.consensus_msg
    }

    pub fn into_inner(self) -> (Header, ConsensusMessage<O>) {
        (self.header, self.consensus_msg)
    }
}

/// Represents a message from the consensus sub-protocol.
///
/// Different types of consensus messages are represented in the `ConsensusMessageKind`
/// type.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ConsensusMessage<O> {
    seq: SeqNo,
    view: SeqNo,
    kind: ConsensusMessageKind<O>,
}

impl<O> Debug for ConsensusMessage<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            ConsensusMessageKind::PrePrepare(_) => {
                write!(f, "Pre prepare message")
            }
            ConsensusMessageKind::Prepare(_) => {
                write!(f, "Prepare message")
            }
            ConsensusMessageKind::Commit(_) => {
                write!(f, "Commit message")
            }
        }
    }
}

/// Represents one of many different consensus stages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ConsensusMessageKind<O> {
    /// Pre-prepare a request, according to the BFT consensus protocol.
    /// Sent by a single leader
    ///
    /// The value `Vec<Digest>` contains a batch of hash digests of the
    /// serialized client requests to be proposed.
    PrePrepare(Vec<StoredMessage<RequestMessage<O>>>),
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

impl<O> Orderable for RequestMessage<O> {
    fn sequence_number(&self) -> SeqNo {
        self.operation_id
    }
}

impl<O> RequestMessage<O> {
    /// Creates a new `RequestMessage`.
    pub fn new(sess: SeqNo, id: SeqNo, operation: O) -> Self {
        Self { operation, operation_id: id, session_id: sess }
    }

    /// Returns a reference to the operation of type `O`.
    pub fn operation(&self) -> &O {
        &self.operation
    }

    pub fn session_id(&self) -> SeqNo {
        self.session_id
    }

    /// Unwraps this `RequestMessage`.
    pub fn into_inner_operation(self) -> O {
        self.operation
    }
}

impl<P> Orderable for ReplyMessage<P> {
    fn sequence_number(&self) -> SeqNo {
        self.operation_id
    }
}

impl<P> ReplyMessage<P> {
    /// Creates a new `ReplyMessage`.
    pub fn new(sess: SeqNo, id: SeqNo, payload: P) -> Self {
        Self { payload, operation_id: id, session_id: sess }
    }

    /// Returns a reference to the payload of type `P`.
    pub fn payload(&self) -> &P {
        &self.payload
    }

    pub fn session_id(&self) -> SeqNo {
        self.session_id
    }

    /// Unwraps this `ReplyMessage`.
    pub fn into_inner(self) -> (SeqNo, SeqNo, P) {
        (self.session_id, self.operation_id, self.payload)
    }
}

impl<O> Orderable for ConsensusMessage<O> {
    /// Returns the sequence number of this consensus message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<O> ConsensusMessage<O> {
    /// Creates a new `ConsensusMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, view: SeqNo, kind: ConsensusMessageKind<O>) -> Self {
        Self { seq, view, kind }
    }

    /// Returns a reference to the consensus message kind.
    pub fn kind(&self) -> &ConsensusMessageKind<O> {
        &self.kind
    }

    pub fn into_kind(self) -> ConsensusMessageKind<O> {
        self.kind
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
            }
        }
    }

    /// Returns the sequence number of the view this consensus message belongs to.
    pub fn view(&self) -> SeqNo {
        self.view
    }

    /// Takes the proposed client requests embedded in this consensus message,
    /// if they are available.
    pub fn take_proposed_requests(&mut self) -> Option<Vec<StoredMessage<RequestMessage<O>>>> {
        let kind = std::mem::replace(
            &mut self.kind,
            ConsensusMessageKind::PrePrepare(Vec::new()),
        );
        match kind {
            ConsensusMessageKind::PrePrepare(v) => Some(v),
            _ => {
                self.kind = kind;
                None
            }
        }
    }
}

///Observer related messages
///@{
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ObserverMessage {
    ///Observer client related messages
    ///Register the client that sent this as an observer
    ObserverRegister,
    //Response to the register request of an observer
    ObserverRegisterResponse(bool),
    ObserverUnregister,
    ///A status update sent to an observer client as an observer
    ObservedValue(ObserveEventKind),
}

///The kinds of events that can be reported by the replicas to observers
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ObserveEventKind {
    ///Report a checkpoint start type event
    /// The provided SeqNo is the last seq number of requests executed before the checkpoint
    CheckpointStart(SeqNo),
    ///Report a checkpoint end type event
    /// The provided SeqNo is the current seq number that is going to be used
    CheckpointEnd(SeqNo),
    ///Report that the system is ready for another round of consensus
    ///
    /// The param is the seq no of the next consensus round
    Ready(SeqNo),
    ///Report that the given replica has received a preprepare request
    ///And it's now going to enter into it's prepare phase
    /// 
    ///  param is the seq no of the received preprepare request, and therefore
    /// of the current consensus instance
    Prepare(SeqNo),
    ///Report that the given replica has received all required prepare messages
    ///And is now going to enter consensus phase
    /// 
    /// param is the seq no of the current consensus instance
    Commit(SeqNo),
    ///Report that the given replica has received all required commit messages
    /// and has sent the request for execution as the consensus has been finished
    ///
    /// The provided SeqNo is the sequence number of the last executed operation
    Consensus(SeqNo),
    ///Report that the previous consensus has been executed and written to the drive
    ///
    /// param is the seq number of the consensus instance that was executed
    Executed(SeqNo),
    ///Report that the replica is now in the normal
    ///phase of the algorithm
    ///
    /// The provided info is the info about the view and the current sequence number
    NormalPhase((ViewInfo, SeqNo)),
    ///Report that the replica has entered the view change phase
    /// The provided SeqNo is the seq number of the new view and the current seq no
    ViewChangePhase,
    /// Report that the replica is now in the collaborative state
    /// transfer state
    CollabStateTransfer,
}

impl Debug for ObserveEventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObserveEventKind::CheckpointStart(_) => {
                write!(f, "Checkpoint start event")
            }
            ObserveEventKind::CheckpointEnd(_) => {
                write!(f, "Checkpoint end event")
            }
            ObserveEventKind::Consensus(_) => {
                write!(f, "Consensus event")
            }
            ObserveEventKind::NormalPhase(_) => {
                write!(f, "Normal phase")
            }
            ObserveEventKind::ViewChangePhase => {
                write!(f, "View change phase")
            }
            ObserveEventKind::CollabStateTransfer => {
                write!(f, "Collab state transfer")
            }
            ObserveEventKind::Prepare(_) => {
                write!(f, "Prepare state entered")
            }
            ObserveEventKind::Commit(_) => {
                write!(f, "Commit state entered")
            }
            ObserveEventKind::Ready(seq) => {
                write!(f, "Ready to receive next consensus {:?}", seq)
            }
            ObserveEventKind::Executed(seq) => {
                write!(f, "Executed the consensus instance {:?}", seq)
            }
        }
    }
}

///}@

///
/// Ping messages
/// @{

///Contains a boolean representing if this is a request.
///If it is a ping request, should be set to true,
///ping responses should be false
///
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct PingMessage {
    request: bool,
}

impl PingMessage {
    pub fn new(is_request: bool) -> Self {
        Self {
            request: is_request
        }
    }

    pub fn is_request(&self) -> bool {
        self.request
    }
}

///}@

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

/*
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

impl<T: Array<Item=u8>> From<WireMessage<'_>> for OwnedWireMessage<SmallVec<T>> {
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
            payload: self.payload,
        }
    }
}
*/

impl WireMessage {
    /// The current version of the wire protocol.
    pub const CURRENT_VERSION: u32 = 0;

    /// Wraps a `Header` and a byte array payload into a `WireMessage`.
    pub fn from_parts(header: Header, payload: Buf) -> Result<Self> {
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
        payload: Buf,
        nonce: u64,
        digest: Option<Digest>,
        sk: Option<&KeyPair>,
    ) -> Self {
        let digest = digest
            // safety: digests have repr(transparent)
            .map(|d| unsafe { std::mem::transmute(d) })
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

    ///Sign a given message, with the following passed parameters
    /// From is the node that sent the message
    /// to is the destination node
    /// nonce is the none
    /// and the payload is what we actually want to sign (in this case we will
    /// sign the digest of the message, instead of the actual entire payload
    /// since that would be quite slow)
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

    ///Verify the signature of a given message, which contains
    /// the following parameters
    /// From is the node that sent the message
    /// to is the destination node
    /// nonce is the none
    /// and the payload is what we actually want to sign (in this case we will
    /// sign the digest of the message, instead of the actual entire payload
    /// since that would be quite slow)
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
    pub fn into_inner(self) -> (Header, Buf) {
        (self.header, self.payload)
    }

    /// Returns a reference to the `Header` of the `WireMessage`.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns a reference to the payload bytes of the `WireMessage`.
    pub fn payload(&self) -> &[u8] {
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
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, mut w: W, flush: bool) -> io::Result<()> {
        let mut buf = [0; Header::LENGTH];
        self.header.serialize_into(&mut buf[..]).unwrap();

        // FIXME: switch to vectored writes?
        w.write_all(&buf[..]).await?;
        if self.payload.len() > 0 {
            w.write_all(&self.payload[..]).await?;
        }
        if flush {
            w.flush().await?;
        }

        Ok(())
    }

    /// Serialize a `WireMessage` into an async writer.
    pub fn write_to_sync<W: Write>(&self, mut w: W, flush: bool) -> io::Result<()> {
        let mut buf = [0; Header::LENGTH];
        self.header.serialize_into(&mut buf[..]).unwrap();

        // FIXME: switch to vectored writes?
        w.write_all(&buf[..]);

        if self.payload.len() > 0 {
            w.write_all(&self.payload[..]);
        }

        if flush {
            w.flush();
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

impl<S: Send, O: Send, P: Send> Message<S, O, P> {
    /// Returns the `Header` of this message, if it is
    /// a `SystemMessage`.
    pub fn header(&self) -> Result<&Header> {
        match self {
            Message::System(ref h, _) =>
                Ok(h),
            Message::ExecutionFinishedWithAppstate(_) =>
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
