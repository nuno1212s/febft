//! This module contains types associated with messages traded
//! between the system processes.

use std::fmt::{Debug, Formatter};
use std::io::Write;

use getset::Getters;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use atlas_common::crypto::hash::Digest;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{Header, StoredMessage};

use crate::bft::log::decisions::CollectData;
use crate::bft::sync::view::ViewInfo;
use crate::bft::sync::LeaderCollects;

pub mod serialize;

/// PBFT protocol messages
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum PBFTMessage<R> {
    /// Consensus message
    Consensus(ConsensusMessage<R>),
    /// View change messages
    ViewChange(ViewChangeMessage<R>),
    //Observer related messages
    ObserverMessage(ObserverMessage),
}

impl<R> Debug for PBFTMessage<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PBFTMessage::Consensus(_) => {
                write!(f, "Consensus ")
            }
            PBFTMessage::ViewChange(_) => {
                write!(f, "View change msg")
            }
            PBFTMessage::ObserverMessage(_) => {
                write!(f, "Observer msg")
            }
        }
    }
}

impl<R> Orderable for PBFTMessage<R> {
    fn sequence_number(&self) -> SeqNo {
        match self {
            PBFTMessage::Consensus(consensus) => consensus.sequence_number(),
            PBFTMessage::ViewChange(view) => view.sequence_number(),
            PBFTMessage::ObserverMessage(_obs) => SeqNo::ZERO,
        }
    }
}

impl<R> PBFTMessage<R> {
    pub fn consensus(&self) -> &ConsensusMessage<R> {
        match self {
            PBFTMessage::Consensus(msg) => msg,
            _ => panic!("Not a consensus message"),
        }
    }

    pub fn into_consensus(self) -> ConsensusMessage<R> {
        match self {
            PBFTMessage::Consensus(msg) => msg,
            _ => panic!("Not a consensus message"),
        }
    }

    pub fn view_change(&self) -> &ViewChangeMessage<R> {
        match self {
            PBFTMessage::ViewChange(msg) => msg,
            _ => panic!("Not a view change message"),
        }
    }

    pub fn into_view_change(self) -> ViewChangeMessage<R> {
        match self {
            PBFTMessage::ViewChange(msg) => msg,
            _ => panic!("Not a view change message"),
        }
    }

    pub fn observer_message(&self) -> &ObserverMessage {
        match self {
            PBFTMessage::ObserverMessage(msg) => msg,
            _ => panic!("Not an observer message"),
        }
    }

    pub fn into_observer_message(self) -> ObserverMessage {
        match self {
            PBFTMessage::ObserverMessage(msg) => msg,
            _ => panic!("Not an observer message"),
        }
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

impl<O> Debug for ViewChangeMessage<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "View {:?}, {:?}", self.view, self.kind)
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
            _ => None,
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum ViewChangeMessageKind<O> {
    /// A STOP message, broadcast when we want to call a view change due to requests getting timed out
    Stop(Vec<StoredMessage<O>>),
    /// A STOP message, broadcast when we want to call a view change due to us having received a Node Quorum Join message
    StopQuorumJoin(NodeId),
    // Each of the latest decisions from the sender, so the new leader can sync
    StopData(CollectData<O>),
    Sync(LeaderCollects<O>),
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
    nonce: u16,
    kind: ConsensusMessageKind<O>,
}

impl<O> Debug for ConsensusMessage<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Seq: {:?} View: {:?} ", self.seq, self.view)?;

        match &self.kind {
            ConsensusMessageKind::PrePrepare(d) => {
                write!(f, "Pre prepare message with {} rqs", d.len())
            }
            ConsensusMessageKind::Prepare(d) => {
                write!(f, "Prepare message {:?}", d)
            }
            ConsensusMessageKind::Commit(d) => {
                write!(f, "Commit message {:?}", d)
            }
        }
    }
}

/// Represents one of many different consensus stages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ConsensusMessageKind<O> {
    /// Pre-prepare a request, according to the BFT consensus protocol.
    /// Sent by a single leader
    ///
    /// The value `Vec<Digest>` contains a batch of hash digests of the
    /// serialized client requests to be proposed.
    PrePrepare(Vec<StoredMessage<O>>),
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

impl<O> Clone for ConsensusMessageKind<O>
where
    O: Clone,
{
    fn clone(&self) -> Self {
        match self {
            ConsensusMessageKind::PrePrepare(reqs) => {
                ConsensusMessageKind::PrePrepare(reqs.clone())
            }
            ConsensusMessageKind::Prepare(digest) => ConsensusMessageKind::Prepare(*digest),
            ConsensusMessageKind::Commit(digest) => ConsensusMessageKind::Commit(*digest),
        }
    }
}

impl<O> ConsensusMessage<O> {
    /// Creates a new `ConsensusMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, view: SeqNo, kind: ConsensusMessageKind<O>) -> Self {
        let nonce = fastrand::u16(0..u16::MAX);

        Self {
            seq,
            view,
            kind,
            nonce,
        }
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
    pub fn take_proposed_requests(&mut self) -> Option<Vec<StoredMessage<O>>> {
        let kind = std::mem::replace(&mut self.kind, ConsensusMessageKind::PrePrepare(Vec::new()));
        match kind {
            ConsensusMessageKind::PrePrepare(v) => Some(v),
            _ => {
                self.kind = kind;
                None
            }
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Getters)]
pub struct FwdConsensusMessage<O> {
    #[get = "pub"]
    header: Header,
    #[get = "pub"]
    consensus_msg: ConsensusMessage<O>,
}

impl<O> FwdConsensusMessage<O> {
    pub fn new(header: Header, msg: ConsensusMessage<O>) -> Self {
        Self {
            header,
            consensus_msg: msg,
        }
    }

    pub fn consensus(&self) -> &ConsensusMessage<O> {
        &self.consensus_msg
    }

    pub fn into_inner(self) -> (Header, ConsensusMessage<O>) {
        (self.header, self.consensus_msg)
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

impl<O> Debug for ViewChangeMessageKind<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewChangeMessageKind::Stop(_) => {
                write!(f, "Stop message")
            }
            ViewChangeMessageKind::StopData(_) => {
                write!(f, "Stop data message")
            }
            ViewChangeMessageKind::Sync(_) => {
                write!(f, "Sync message")
            }
            ViewChangeMessageKind::StopQuorumJoin(node) => {
                write!(f, "Stop quorum join message {:?}", node)
            }
        }
    }
}
