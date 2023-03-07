pub(crate) mod serialization;

use std::fmt::{Debug, Formatter};
use febft_common::crypto::hash::Digest;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_execution::executable::{Reply, Request, Service, State};
use febft_execution::serialize::SharedData;
use febft_messages::messages::{RequestMessage, SystemMessage};
use crate::cst::RecoveryState;
use crate::msg_log::decisions::CollectData;
use crate::sync::LeaderCollects;
use crate::sync::view::ViewInfo;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
#[cfg(feature = "serialize_bincode")]
use bincode::{Decode, Encode};

use crate::SysMsg;

/// A protocol message corresponds to a message regarding one of the SMR protocols
/// This can be a message to the consensus state machine, view change state machine or
/// consensus state transfer protocol
/// The S is the State type while the O is the request type
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
pub enum PBFTProtocolMessage<D> where D: SharedData {
    Consensus(ConsensusMessage<D::Request>),
    FwdConsensus(FwdConsensusMessage<D::Request>),

    Cst(CstMessage<D::State, D::Request>),
    ViewChange(ViewChangeMessage<D::Request>),

    ObserverMessage(ObserverMessage),
}

/// Manual clone implementation as the compiler is not smart enough to decompose the
/// Implementation above into it's Cloneable parts, so we are left with this
/// Manual but very effective (and safe) work around
impl<D:SharedData> Clone for PBFTProtocolMessage<D> {
    fn clone(&self) -> Self {
        match self {
            PBFTProtocolMessage::Consensus(consensus) => {
                PBFTProtocolMessage::Consensus(consensus.clone())
            }
            PBFTProtocolMessage::FwdConsensus(fwd_consensus) => {
                PBFTProtocolMessage::FwdConsensus(fwd_consensus.clone())
            }
            PBFTProtocolMessage::Cst(cst) => {
                PBFTProtocolMessage::Cst(cst.clone())
            }
            PBFTProtocolMessage::ViewChange(view_change) => {
                PBFTProtocolMessage::ViewChange(view_change.clone())
            }
            PBFTProtocolMessage::ObserverMessage(observer) => {
                PBFTProtocolMessage::ObserverMessage(observer.clone())
            }
        }
    }
}

/// Automatically encapsulate to a system message
impl<D: SharedData> From<PBFTProtocolMessage<D>> for SysMsg<D> {
    fn from(value: PBFTProtocolMessage<D>) -> Self {
        SystemMessage::Protocol(febft_messages::messages::ProtocolMessage::new(value))
    }
}

impl<D: SharedData> From<SystemMessage<D, PBFTProtocolMessage<D>>> for PBFTProtocolMessage<D> {
    fn from(value: SystemMessage<D, PBFTProtocolMessage<D>>) -> Self {
        match value {
            SystemMessage::Protocol(protocol) => {
                protocol.into_inner()
            }
            _ => unreachable!()
        }
    }
}

impl<D> Debug for PBFTProtocolMessage<D> where D: SharedData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PBFTProtocolMessage::Consensus(_) => {
                write!(f, "Consensus Message")
            }
            PBFTProtocolMessage::FwdConsensus(_) => {
                write!(f, "Forwarded Consensus Message")
            }
            PBFTProtocolMessage::Cst(_) => {
                write!(f, "CST Message")
            }
            PBFTProtocolMessage::ViewChange(_) => {
                write!(f, "View change Message")
            }
            PBFTProtocolMessage::ObserverMessage(_) => {
                write!(f, "Consensus Message")
            }
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
#[derive(Clone)]
pub enum ViewChangeMessageKind<O> {
    Stop(Vec<StoredMessage<RequestMessage<O>>>),
    StopData(CollectData<O>),
    Sync(LeaderCollects<O>),
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
#[derive(Clone)]
pub struct CstMessage<S, O> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: CstMessageKind<S, O>,
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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
#[cfg_attr(feature = "serialize_bincode", derive(Encode, Decode))]
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