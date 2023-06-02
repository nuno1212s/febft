pub mod serialize;


use std::fmt::{Debug, Formatter};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::ordering::{Orderable, SeqNo};
use crate::RecoveryState;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct CstMessage<S, V, O, P> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: CstMessageKind<S, V, O, P>,
}

impl<S, V, O, P> Debug for CstMessage<S, V, O, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            CstMessageKind::RequestLatestConsensusSeq => {
                write!(f, "Request consensus ID")
            }
            CstMessageKind::ReplyLatestConsensusSeq(opt ) => {
                write!(f, "Reply consensus seq {:?}", opt.as_ref().map(|(seq, _)| *seq).unwrap_or(SeqNo::ZERO))
            }
            CstMessageKind::RequestState => {
                write!(f, "Request state message")
            }
            CstMessageKind::ReplyState(_) => {
                write!(f, "Reply with state message")
            }
        }

    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum CstMessageKind<S, V, O, P> {
    RequestLatestConsensusSeq,
    ReplyLatestConsensusSeq(Option<(SeqNo, P)>),
    RequestState,
    ReplyState(RecoveryState<S, V, O>),
}

impl<S, O, V, P> Orderable for CstMessage<S, V, O, P> {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S, O, V, P> CstMessage<S, V, O, P> {
    /// Creates a new `CstMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, kind: CstMessageKind<S, V, O, P>) -> Self {
        Self { seq, kind }
    }

    /// Returns a reference to the state transfer message kind.
    pub fn kind(&self) -> &CstMessageKind<S, V, O, P> {
        &self.kind
    }

    /// Takes the recovery state embedded in this cst message, if it is available.
    pub fn take_state(&mut self) -> Option<RecoveryState<S, V, O>> {
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