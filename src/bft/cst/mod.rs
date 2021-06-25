//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

use std::cmp::Ordering;

use crate::bft::log::Log;
use crate::bft::ordering::SeqNo;
use crate::bft::consensus::Consensus;
use crate::bft::core::server::ViewInfo;
use crate::bft::communication::{
    Node,
    //NodeId,
};
use crate::bft::communication::message::{
    Header,
    CstMessage,
    CstMessageKind,
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
    WaitingCheckpoint(Header, CstMessage),
    ReceivingCid(usize),
    ReceivingState(usize),
}

// TODO:
// - finish this struct
// - include request payload
pub struct ExecutionState<S: Service> {
    latest_cid: SeqNo,
    view: ViewInfo,
    checkpoint_state: State<S>,
    // used to replay log on recovering replicas;
    // the request batches have been concatenated,
    // for efficiency
    requests: Vec<Request<S>>,
    //pre_prepares: Vec<StoredConsensus>,
    //prepares: Vec<StoredConsensus>,
    //commits: Vec<StoredConsensus>,
}

/// Represents the state of an on-going colloborative
/// state transfer protocol execution.
pub struct CollabStateTransfer {
    phase: ProtoPhase,
    latest_cid: SeqNo,
    latest_cid_count: usize,
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
    /// We should request the latest cid from the view.
    RequestLatestCid,
    /// We should request the latest state from the view.
    RequestState,
    /// We have received and validated the largest consensus sequence
    /// number available.
    SeqNo(SeqNo),
    /// We have received and validated the state from
    /// a group of replicas.
    State( (/* TODO: app state type */) )
}

/// Represents progress in the CST state machine.
///
/// To clarify, the mention of state machine here has nothing to do with the
/// SMR protocol, but rather the implementation in code of the CST protocol.
pub enum CstProgress {
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, CstMessage),
}

macro_rules! getmessage {
    ($progress:expr, $status:expr) => {
        match $progress {
            CstProgress::Nil => return $status,
            CstProgress::Message(h, m) => (h, m),
        }
    };
    // message queued while waiting for exec layer to deliver app state
    ($phase:expr) => {{
        let phase = std::mem::replace($phase, ProtoPhase::Init);
        match phase {
            ProtoPhase::WaitingCheckpoint(h, m) => (h, m),
            _ => return CstStatus::Nil,
        }
    }};
}

impl CollabStateTransfer {
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            latest_cid: SeqNo::from(0u32),
            latest_cid_count: 0,
            seq: SeqNo::from(0),
        }
    }

    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_, _))
    }

    pub fn process_message<S>(
        &mut self,
        progress: CstProgress,
        view: ViewInfo,
        consensus: &Consensus<S>,
        log: &mut Log<Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> CstStatus
    where
        S: Service + Send + 'static,
        State<S>: Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
    {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_, _) => {
                let (header, message) = getmessage!(&mut self.phase);

                // TODO: send app state
                unimplemented!()
            },
            ProtoPhase::Init => {
                let (header, message) = getmessage!(progress, CstStatus::Nil);
                match message.kind() {
                    CstMessageKind::RequestLatestConsensusSeq => {
                        let kind = CstMessageKind::ReplyLatestConsensusSeq(
                            consensus.sequence_number(),
                        );
                        let reply = SystemMessage::Cst(CstMessage::new(
                            message.sequence_number(),
                            kind,
                        ));
                        node.send(reply, header.from());
                    },
                    CstMessageKind::RequestState => {
                        if !log.has_complete_checkpoint() {
                            self.phase = ProtoPhase::WaitingCheckpoint(header, message);
                            return CstStatus::Nil;
                        }

                        // TODO: send app state
                        unimplemented!()
                    },
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestReplicaState
                    _ => (),
                }
                CstStatus::Nil
            },
            ProtoPhase::ReceivingCid(i) => {
                let (header, message) = getmessage!(progress, CstStatus::RequestLatestCid);

                // drop cst messages with invalid seq no
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
                        match seq.cmp(&self.latest_cid) {
                            Ordering::Greater => {
                                self.latest_cid = *seq;
                                self.latest_cid_count = 1;
                            },
                            Ordering::Equal => {
                                self.latest_cid_count += 1;
                            },
                            Ordering::Less => (),
                        }
                    },
                    // drop invalid message kinds
                    _ => return CstStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                let i = i + 1;

                if i == view.params().quorum() {
                    self.phase = ProtoPhase::Init;
                    if self.latest_cid_count > view.params().f() {
                        // the latest cid was available in at least
                        // f+1 replicas
                        CstStatus::SeqNo(self.latest_cid)
                    } else {
                        CstStatus::RequestLatestCid
                    }
                } else {
                    self.phase = ProtoPhase::ReceivingCid(i);
                    CstStatus::Running
                }
            },
            ProtoPhase::ReceivingState(_i) => {
                let (header, message) = getmessage!(progress, CstStatus::RequestState);

                // TODO: implement receiving app state on a replica
                unimplemented!()
            },
        }
    }

    pub fn request_latest_consensus_seq_no<S>(&mut self, _node: &mut Node<S::Data>)
    where
        S: Service + Send + 'static,
        State<S>: Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
    {
        // reset state of latest seq no
        self.latest_cid = SeqNo::from(0u32);
        self.latest_cid_count = 0;

        // ...

        // update our cst seq no
        self.seq = self.seq.next();

        unimplemented!()
    }
}
