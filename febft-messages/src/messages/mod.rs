use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use febft_common::ordering::{Orderable, SeqNo};
use febft_common::error::*;
use febft_communication::message::{Header, NetworkMessage, StoredMessage};
use febft_execution::serialize::SharedData;
use crate::serialize::{OrderingProtocolMessage, ServiceMsg};

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use crate::state_transfer::Checkpoint;
use crate::timeouts::{TimedOut, Timeout};


/// The `Message` type encompasses all the messages traded between different
/// asynchronous tasks in the system.
///
pub enum Message<D> where D: SharedData {
    /// Same as `Message::ExecutionFinished`, but includes a snapshot of
    /// the application state.
    ///
    /// This is useful for local checkpoints.
    ExecutionFinishedWithAppstate((SeqNo, D::State)),
    DigestedAppState(Arc<ReadOnly<Checkpoint<D::State>>>),
    /// We received a timeout from the timeouts layer.
    Timeout(TimedOut),
}

impl<D> Debug for Message<D> where D: SharedData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::ExecutionFinishedWithAppstate(_) => {
                write!(f, "Execution finished")
            }
            Message::Timeout(_) => {
                write!(f, "timeout")
            }
            Message::DigestedAppState(_) => {
                write!(f, "DigestedAppState")
            }
        }
    }
}

impl<D: SharedData> Message<D> {
    /// Returns the `Header` of this message, if it is
    /// a `SystemMessage`.
    pub fn header(&self) -> Result<&Header> {
        match self {
            Message::ExecutionFinishedWithAppstate(_) =>
                Err("Expected System found ExecutionFinishedWithAppstate")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::Timeout(_) =>
                Err("Expected System found Timeout")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::DigestedAppState(_) => {
                Err("Expected System found DigestedAppState")
                    .wrapped(ErrorKind::CommunicationMessage)
            }
        }
    }
}


#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum SystemMessage<D: SharedData, P, ST> {
    ///An ordered request
    OrderedRequest(RequestMessage<D::Request>),
    ///An unordered request
    UnorderedRequest(RequestMessage<D::Request>),
    ///A reply to an ordered request
    OrderedReply(ReplyMessage<D::Reply>),
    ///A reply to an unordered request
    UnorderedReply(ReplyMessage<D::Reply>),
    ///Requests forwarded from other peers
    ForwardedRequestMessage(ForwardedRequestsMessage<D::Request>),
    ///A message related to the protocol
    ProtocolMessage(Protocol<P>),
    ///A protocol message that has been forwarded by another peer
    ForwardedProtocolMessage(ForwardedProtocolMessage<P>),
    ///A state transfer protocol message
    StateTransferMessage(StateTransfer<ST>),
}

impl<D, P, ST> SystemMessage<D, P, ST> where D: SharedData {
    pub fn from_protocol_message(msg: P) -> Self {
        SystemMessage::ProtocolMessage(Protocol::new(msg))
    }

    pub fn from_state_transfer_message(msg: ST) -> Self {
        SystemMessage::StateTransferMessage(StateTransfer::new(msg))
    }

    pub fn from_fwd_protocol_message(msg: StoredMessage<Protocol<P>>) -> Self {
        SystemMessage::ForwardedProtocolMessage(ForwardedProtocolMessage::new(msg))
    }

    pub fn into_protocol_message(self) -> P {
        match self {
            SystemMessage::ProtocolMessage(prot) => {
                prot.into_inner()
            }
            _ => {
                unreachable!()
            }
        }
    }

    pub fn into_state_tranfer_message(self) -> ST {
        match self {
            SystemMessage::StateTransferMessage(s) => {
                s.into_inner()
            }
            _ => { unreachable!() }
        }
    }
}

impl<D, P, ST> Clone for SystemMessage<D, P, ST> where D: SharedData, P: Clone, ST: Clone {
    fn clone(&self) -> Self {
        match self {
            SystemMessage::OrderedRequest(req) => {
                SystemMessage::OrderedRequest(req.clone())
            }
            SystemMessage::UnorderedRequest(req) => {
                SystemMessage::UnorderedRequest(req.clone())
            }
            SystemMessage::OrderedReply(rep) => {
                SystemMessage::OrderedReply(rep.clone())
            }
            SystemMessage::UnorderedReply(rep) => {
                SystemMessage::UnorderedReply(rep.clone())
            }
            SystemMessage::ForwardedRequestMessage(fwd_req) => {
                SystemMessage::ForwardedRequestMessage(fwd_req.clone())
            }
            SystemMessage::ProtocolMessage(prot) => {
                SystemMessage::ProtocolMessage(prot.clone())
            }
            SystemMessage::ForwardedProtocolMessage(prot) => {
                SystemMessage::ForwardedProtocolMessage(prot.clone())
            }
            SystemMessage::StateTransferMessage(state_transfer) => {
                SystemMessage::StateTransferMessage(state_transfer.clone())
            }
        }
    }
}

impl<D, P, ST> Debug for SystemMessage<D, P, ST> where D: SharedData, P: Clone, ST: Clone {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemMessage::OrderedRequest(_) => {
                write!(f, "Ordered Request")
            }
            SystemMessage::UnorderedRequest(_) => {
                write!(f, "Unordered Request")
            }
            SystemMessage::OrderedReply(_) => {
                write!(f, "Ordered Reply")
            }
            SystemMessage::UnorderedReply(_) => {
                write!(f, "Unordered Reply")
            }
            SystemMessage::ForwardedRequestMessage(_) => {
                write!(f, "Forwarded Request")
            }
            SystemMessage::ProtocolMessage(_) => {
                write!(f, "Protocol Message")
            }
            SystemMessage::ForwardedProtocolMessage(_) => {
                write!(f, "Forwarded Protocol Message")
            }
            SystemMessage::StateTransferMessage(_) => {
                write!(f, "State Transfer Message")
            }
        }
    }
}

#[derive(Eq, PartialEq, Ord, Clone, PartialOrd, Debug)]
pub struct ClientRqInfo {
    //The UNIQUE digest of the request in question
    pub digest: Digest,

    // The sender, sequence number and session number
    pub sender: NodeId,
    pub seqno: SeqNo,
    pub session: SeqNo,
}

pub type StoredRequestMessage<O> = StoredMessage<RequestMessage<O>>;

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

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Protocol<P> {
    payload: P,
}

impl<P> Protocol<P> {
    pub fn new(payload: P) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &P { &self.payload }

    pub fn into_inner(self) -> P {
        self.payload
    }
}

impl<P> Deref for Protocol<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct StateTransfer<P> {
    payload: P,
}

impl<P> StateTransfer<P> {
    pub fn new(payload: P) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &P { &self.payload }

    pub fn into_inner(self) -> P {
        self.payload
    }
}

impl<P> Deref for StateTransfer<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

/// A message containing a number of forwarded requests
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ForwardedRequestsMessage<O> {
    inner: Vec<StoredRequestMessage<O>>,
}

impl<O> ForwardedRequestsMessage<O> {
    /// Creates a new `ForwardedRequestsMessage`, containing the given client requests.
    pub fn new(inner: Vec<StoredRequestMessage<O>>) -> Self {
        Self { inner }
    }

    pub fn requests(&self) -> &Vec<StoredRequestMessage<O>> { &self.inner }

    pub fn mut_requests(&mut self) -> &mut Vec<StoredRequestMessage<O>> { &mut self.inner }

    /// Returns the client requests contained in this `ForwardedRequestsMessage`.
    pub fn into_inner(self) -> Vec<StoredRequestMessage<O>> {
        self.inner
    }
}

/// A message containing a single forwarded consensus message
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ForwardedProtocolMessage<P> where {
    message: StoredMessage<Protocol<P>>,
}

impl<P> Deref for ForwardedProtocolMessage<P> {
    type Target = StoredMessage<Protocol<P>>;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl<P> ForwardedProtocolMessage<P> {
    pub fn new(message: StoredMessage<Protocol<P>>) -> Self {
        Self { message }
    }

    pub fn message(&self) -> &StoredMessage<Protocol<P>> { &self.message }

    pub fn into_inner(self) -> StoredMessage<Protocol<P>> {
        self.message
    }
}

impl<O> Debug for RequestMessage<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Session: {:?} Seq No: {:?}", self.session_id, self.sequence_number())
    }
}

impl Orderable for ClientRqInfo {
    fn sequence_number(&self) -> SeqNo {
        self.seqno
    }
}

impl ClientRqInfo {
    pub fn new(digest: Digest, sender: NodeId, seqno: SeqNo, session: SeqNo) -> Self {
        Self {
            digest,
            sender,
            seqno,
            session,
        }
    }
}

impl<O> From<StoredRequestMessage<O>> for ClientRqInfo {
    fn from(message: StoredRequestMessage<O>) -> Self {

        let digest = message.header().unique_digest();
        let sender = message.header().from();

        let session = message.message().session_number();
        let seq_no = message.message().sequence_number();

        Self {
            digest,
            sender,
            seqno: seq_no,
            session,
        }
    }
}

impl Hash for ClientRqInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.digest.hash(state);
    }
}