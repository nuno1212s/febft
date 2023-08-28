use std::fmt::{Debug, Formatter};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::crypto::hash::Digest;

use atlas_common::ordering::{Orderable, SeqNo};

use crate::RecoveryState;

pub mod serialize;


#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct CstMessage<S> {
    // NOTE: not the same sequence number used in the
    // consensus layer to order client requests!
    seq: SeqNo,
    kind: CstMessageKind<S>,
}

impl<S> Debug for CstMessage<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            CstMessageKind::RequestState => {
                write!(f, "Request state message")
            }
            CstMessageKind::ReplyState(_) => {
                write!(f, "Reply with state message")
            }
            CstMessageKind::RequestStateCid => {
                write!(f, "Request state cid message")
            }
            CstMessageKind::ReplyStateCid(opt) => {
                if let Some((seq, digest)) = opt {
                    write!(f, "Reply with state cid message {:?} {:?}", seq, digest)
                } else {
                    write!(f, "Reply with state cid message None")
                }
            }
        }
    }
}

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub enum CstMessageKind<S> {
    RequestStateCid,
    ReplyStateCid(Option<(SeqNo, Digest)>),
    RequestState,
    ReplyState(RecoveryState<S>),
}

impl<S> Orderable for CstMessage<S> {
    /// Returns the sequence number of this state transfer message.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S> CstMessage<S> {
    /// Creates a new `CstMessage` with sequence number `seq`,
    /// and of the kind `kind`.
    pub fn new(seq: SeqNo, kind: CstMessageKind<S>) -> Self {
        Self { seq, kind }
    }

    /// Returns a reference to the state transfer message kind.
    pub fn kind(&self) -> &CstMessageKind<S> {
        &self.kind
    }

    /// Takes the recovery state embedded in this cst message, if it is available.
    pub fn take_state(&mut self) -> Option<RecoveryState<S>> {
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