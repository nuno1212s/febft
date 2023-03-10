//! This module contains types associated with messages traded
//! between the system processes.

use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Write;
use std::mem::MaybeUninit;
use bytes::Bytes;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use febft_common::error::*;

use futures::io::{
    AsyncWriteExt,
    AsyncWrite,
};
use febft_common::crypto::hash::{Context, Digest};
use febft_common::crypto::signature::{KeyPair, PublicKey, Signature};
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, PingMessage, StoredMessage};

use crate::bft::timeouts::{Timeout, TimeoutKind};
use crate::bft::sync::LeaderCollects;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{Reply, Request, State};
use crate::bft::message::serialize::{Buf, SharedData};
use crate::bft::msg_log::decisions::CollectData;
use crate::bft::sync::view::ViewInfo;

pub mod serialize;

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
