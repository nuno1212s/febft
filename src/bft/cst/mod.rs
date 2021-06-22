//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use crate::bft::ordering::SeqNo;
use crate::bft::communication::{
    Node,
    NodeId,
};
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
    largest_cid: SeqNo,
    largest_cid_node: NodeId,
    seq: SeqNo,
    // NOTE: remembers whose replies we have
    // received already, to avoid replays
    //voted: HashSet<NodeId>,
}

/// Status returned from processnig a state transfer message.
pub enum CstStatus {
    /// We are not running the CST protocol.
    ///
    /// Drop any attempt of processing a message in this condition.
    Nil,
    /// We are waiting for replies to form a BFT quorum.
    AwaitingReplies,
    /// We have received the largest consensus sequence number from
    /// the following node.
    SeqNo(NodeId, SeqNo),
}

impl CollabStateTransfer {
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            largest_cid: SeqNo::from(0),
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
        match self.phase {
            ProtoPhase::Init => {
                // drop all messages if we are not running
                // the cst protocol
                //
                // FIXME: is this correct?
                CstStatus::Nil
            },
            ProtoPhase::ReceivingCid(i) => {
                // drop old messages
                if message.sequence_number() != self.seq {
                    // FIXME: maybe return CstStatus::Finish?
                    return CstStatus::AwaitingReplies;
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                let i = i + 1;

                if i == view.params().quorum() {
                    unimplemented!()
                } else {
                    unimplemented!()
                }
            },
        }
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
