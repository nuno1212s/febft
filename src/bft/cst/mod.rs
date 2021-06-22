//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use crate::bft::ordering::SeqNo;
use crate::bft::communication::Node;
use crate::bft::communication::message::{
    Header,
    CstMessage,
    //SystemMessage,
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

/// Status returned from processnig a state transfer message.
pub enum CstStatus {
    AwaitingReplies,
    SeqNo(SeqNo),
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
        message: CstMessage,
        node: &mut Node<S::Data>,
    ) -> CstStatus
    where
        S: Service + Send + 'static,
        State<S>: Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
    {
        unimplemented!()
    }

    pub fn request_latest_consensus_seq_no<S>(&mut self, node: &mut Node<S::Data>)
    where
        S: Service + Send + 'static,
        State<S>: Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
    {
        unimplemented!()
    }
}
