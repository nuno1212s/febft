use std::ops::Deref;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{NetworkMessageContent, StoredMessage};

#[cfg(feature = "serialize_bincode")]
use bincode::{Decode, Encode};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use febft_execution::serialize::SharedData;
use crate::serialization::ProtocolData;

/// A `SystemMessage` corresponds to a message regarding one of the SMR
/// sub-protocols or requests from the clients.
///
/// This can be either a `Request` from a client or any given Protocol message, to
/// be defined by the consensus crate.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
pub enum SystemMessage<D, P> where D: SharedData, P: ProtocolData {
    OrderedRequest(RequestMessage<D::Request>),
    OrderedReply(ReplyMessage<D::Reply>),
    UnorderedRequest(RequestMessage<D::Request>),
    UnorderedReply(ReplyMessage<D::Reply>),

    //Decide how this is going to be handled
    ForwardedRequests(ForwardedRequestsMessage<D::Request>),

    Protocol(ProtocolMessage<P::Message>),
}

impl<D, P> Clone for SystemMessage<D, P> where D: SharedData, P: ProtocolData {
    fn clone(&self) -> Self {
        match self {
            SystemMessage::OrderedRequest(req) => {
                SystemMessage::OrderedRequest(req.clone())
            }
            SystemMessage::OrderedReply(reply) => {
                SystemMessage::OrderedReply(reply.clone())
            }
            SystemMessage::UnorderedRequest(req) => {
                SystemMessage::UnorderedRequest(req.clone())
            }
            SystemMessage::UnorderedReply(reply) => {
                SystemMessage::UnorderedReply(reply.clone())
            }
            SystemMessage::ForwardedRequests(fwd_reqs) => {
                SystemMessage::ForwardedRequests(fwd_reqs.clone())
            }
            SystemMessage::Protocol(protocol) => {
                SystemMessage::Protocol(protocol.clone())
            }
        }
    }
}

impl<D, P> From<SystemMessage<D, P>> for NetworkMessageContent<SystemMessage<D, P>> where D: SharedData, P: ProtocolData {
    fn from(value: SystemMessage<D, P>) -> Self {
        NetworkMessageContent::System(value)
    }
}

impl<D, P> From<NetworkMessageContent<SystemMessage<D, P>>> for SystemMessage<D, P> where D: SharedData, P: ProtocolData {
    fn from(value: NetworkMessageContent<SystemMessage<D, P>>) -> Self {
        match value {
            NetworkMessageContent::System(sys) => {
                sys
            }
            NetworkMessageContent::Ping(_) => {
                panic!("Cannot unwrap a ping msg")
            }
        }
    }
}

/// A protocol message
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
#[derive(Clone)]
pub struct ProtocolMessage<P> {
    inner: P,
}

impl<P> ProtocolMessage<P> {
    /// Creates a new `ProtocolMessage`, containing the given protocol message.
    pub fn new(inner: P) -> Self {
        Self { inner }
    }

    /// Returns the inner Protocol Message, consuming the encapsulating struct
    pub fn into_inner(self) -> P {
        self.inner
    }
}

impl<P> Deref for ProtocolMessage<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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

/// Represents a request from a client.
///
/// The `O` type argument symbolizes the client operation to be performed
/// over the replicated state.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
