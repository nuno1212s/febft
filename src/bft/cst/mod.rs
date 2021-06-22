//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use crate::bft::ordering::SeqNo;
use crate::bft::communication::message::{
    Header,
    CstMessage,
    SystemMessage,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

enum ProtoPhase {
    Init,
    ReceivingCid(usize),
    ReceivingAppState(usize),
}

/// Represents the state of an on-going colloborative
/// state transfer protocol execution.
pub struct CollabStateTransfer {
    phase: ProtoPhase,
    seq: SeqNo,
}

/// Status returned from asking the latest sequence number
/// the consensus protocol is currently ordering.
pub enum ConsensusSeqStatus {
    WaitingFetch,
    Fetched(SeqNo),
}

impl CollabStateTransfer {
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            seq: SeqNo::from(0),
        }
    }

    pub fn process_message<S>(
        &mut self,
        header: Header,
        message: CstMessage<State<S>>,
        node: &mut Node<S::Data>,
    ) -> ConsensusStatus
    where
        S: Service + Send + 'static,
        State<S>: Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
    {
        unimplemented!()
    }

    pub fn ask_latest_consensus_seq_no(&mut self, node: &mut Node<S::Data>) {
        asd
    }
}
