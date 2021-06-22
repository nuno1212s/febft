//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use crate::bft::communication::message::{
    Header,
    SystemMessage,
    ConsensusMessage,
    ConsensusMessageKind,
};

/// Represents the state of an on-going colloborative
/// state transfer protocol execution.
pub struct CollabStTransfer {
    phase: ProtoPhase,
    seq: SeqNo,
}

/// Status returned from asking the latest sequence number
/// the consensus protocol is currently ordering.
pub enum ConsensusSeqStatus {
    WaitingFetch,
    Fetched(SeqNo),
}

enum ProtoPhase {
    Init,
    ReceivingCid(usize),
    ReceivingAppState(usize),
}

impl CollabStTransfer {
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            seq: SeqNo::from(0),
        }
    }

    pub fn process_message(&mut self, header: Header, message: 

    pub fn ask_latest_consensus_seq_no(&mut self, node: &mut Node<S::Data>) 
}
