//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use crate::bft::ordering::SeqNo;
use crate::bft::core::server::ViewInfo;
use crate::bft::communication::{
    Node,
    NodeId,
};
use crate::bft::communication::message::{
    Header,
    CstMessage,
    CstMessageKind,
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
    latest_cid: SeqNo,
    latest_cid_node: NodeId,
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
    /// The CST protocol is currently running.
    Running,
    /// We have received the largest consensus sequence number from
    /// the following node.
    SeqNo(NodeId, SeqNo),
}

impl CollabStateTransfer {
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            latest_cid: SeqNo::from(0),
            latest_cid_node: NodeId::from(0u32),
            seq: SeqNo::from(0),
        }
    }

    pub fn process_message<S>(
        &mut self,
        header: Header,
        message: CstMessage,
        view: ViewInfo,
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
                match message.kind() {
                    CstMessageKind::RequestLatestConsensusSeq => {
                        // TODO: send consensus id
                        unimplemented!()
                    },
                    CstMessageKind::RequestApplicationState => {
                        // TODO: send app state
                        unimplemented!()
                    },
                    // we are not running cst, so drop any reply msgs
                    _ => (),
                }
                CstStatus::Nil
            },
            ProtoPhase::ReceivingCid(i) => {
                // drop old messages
                if message.sequence_number() != self.seq {
                    // FIXME: how to handle old or newer messages?
                    // BFT-SMaRt simply ignores messages with a
                    // value of `queryID` different from the current
                    // `queryID` a replica is tracking...
                    // we will do the same for now
                    return CstStatus::Running;
                }

                match message.kind() {
                    CstMessageKind::ReplyLatestConsensusSeq(seq) => {
                        let seq = *seq;
                        if seq > self.latest_cid {
                            self.latest_cid = seq;
                            self.latest_cid_node = header.from();
                        }
                    },
                    // drop invalid message kinds
                    _ => return CstStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                let i = i + 1;

                if i == view.params().quorum() {
                    unimplemented!()
                } else {
                    self.phase = ProtoPhase::ReceivingCid(i);
                    CstStatus::Running
                }
            },
            // TODO: implement receiving app state on a replica
            ProtoPhase::ReceivingAppState(i) => unimplemented!(),
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
