use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::StoredMessage;
use febft_execution::serialize::SharedData;
use crate::serialize::OrderingProtocol;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum SystemMessage<D: SharedData, P: OrderingProtocol> {
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
    ProtocolMessage(Protocol<P::ProtocolMessage>),
    ///A protocol message that has been forwarded by another peer
    ForwardedProtocolMessage(ForwardedProtocolMessage<P>),
}

impl<D, P> SystemMessage<D, P> where D: SharedData, P: OrderingProtocol {
    pub fn from_protocol_message(msg: P::ProtocolMessage) -> Self {
        SystemMessage::ProtocolMessage(Protocol::new(msg))
    }
}

impl<D, P> Clone for SystemMessage<D, P> where D: SharedData, P: OrderingProtocol {
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

/// A message containing a number of forwarded requests
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

    pub fn requests(&self) -> &Vec<StoredMessage<RequestMessage<O>>> { &self.inner }

    /// Returns the client requests contained in this `ForwardedRequestsMessage`.
    pub fn into_inner(self) -> Vec<StoredMessage<RequestMessage<O>>> {
        self.inner
    }
}

/// A message containing a single forwarded consensus message
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct ForwardedProtocolMessage<P> where P: OrderingProtocol {
    message: StoredMessage<Protocol<P::ProtocolMessage>>,
}

impl<P> ForwardedProtocolMessage<P> where P: OrderingProtocol {
    pub fn new(message: StoredMessage<Protocol<P::ProtocolMessage>>) -> Self {
        Self { message }
    }

    pub fn message(&self) -> &StoredMessage<Protocol<P::ProtocolMessage>> { &self.message }

    pub fn into_inner(self) -> StoredMessage<Protocol<P::ProtocolMessage>> {
        self.message
    }
}

impl<P> Clone for ForwardedProtocolMessage<P> where P: OrderingProtocol {
    fn clone(&self) -> Self {
        ForwardedProtocolMessage::new(self.message.clone())
    }
}