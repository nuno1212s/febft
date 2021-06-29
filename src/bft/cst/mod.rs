//! The collaborative state transfer algorithm.
//!
//! The implementation is based on the paper «On the Efﬁciency of
//! Durable State Machine Replication», by A. Bessani et al.

// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number

use std::cmp::Ordering;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::ordering::SeqNo;
use crate::bft::crypto::hash::Digest;
use crate::bft::consensus::Consensus;
use crate::bft::core::server::ViewInfo;
use crate::bft::log::{
    Log,
    Checkpoint,
    StoredMessage,
};
use crate::bft::communication::{
    Node,
    //NodeId,
};
use crate::bft::communication::message::{
    Header,
    CstMessage,
    SystemMessage,
    CstMessageKind,
    ConsensusMessage,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};
use crate::bft::collections::{
    self,
    HashMap,
};

enum ProtoPhase<S, O> {
    Init,
    WaitingCheckpoint(Header, CstMessage<S, O>),
    ReceivingCid(usize),
    ReceivingState(usize),
}

/// Contains state used by a recovering node.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct RecoveryState<S, O> {
    view: ViewInfo,
    checkpoint: Checkpoint<S>,
    // used to replay log on recovering replicas;
    // the request batches have been concatenated,
    // for memory efficiency
    requests: Vec<O>,
    pre_prepares: Vec<StoredMessage<ConsensusMessage>>,
    prepares: Vec<StoredMessage<ConsensusMessage>>,
    commits: Vec<StoredMessage<ConsensusMessage>>,
}

impl<S, O> RecoveryState<S, O> {
    /// Creates a new `RecoveryState`.
    pub fn new(
        view: ViewInfo,
        checkpoint: Checkpoint<S>,
        requests: Vec<O>,
        pre_prepares: Vec<StoredMessage<ConsensusMessage>>,
        prepares: Vec<StoredMessage<ConsensusMessage>>,
        commits: Vec<StoredMessage<ConsensusMessage>>,
    ) -> Self {
        Self {
            view,
            checkpoint,
            requests,
            pre_prepares,
            prepares,
            commits,
        }
    }

    /// Returns the view this `RecoveryState` is tracking.
    pub fn view(&self) -> ViewInfo {
        self.view
    }

    /// Returns the local checkpoint of this recovery state.
    pub fn checkpoint(&self) -> &Checkpoint<S> {
        &self.checkpoint
    }

    /// Returns the operations embedded in the requests sent by clients
    /// after the last checkpoint at the moment of the creation of this `RecoveryState`.
    pub fn requests(&self) -> &[O] {
        &self.requests[..]
    }

    /// Returns the list of `PRE-PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `RecoveryState`.
    pub fn pre_prepares(&self) -> &[StoredMessage<ConsensusMessage>] {
        &self.pre_prepares[..]
    }

    /// Returns the list of `PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `RecoveryState`.
    pub fn prepares(&self) -> &[StoredMessage<ConsensusMessage>] {
        &self.prepares[..]
    }

    /// Returns the list of `COMMIT` messages after the last checkpoint
    /// at the moment of the creation of this `RecoveryState`.
    pub fn commits(&self) -> &[StoredMessage<ConsensusMessage>] {
        &self.commits[..]
    }
}

struct ReceivedState<S, O> {
    count: usize,
    state: RecoveryState<S, O>,
}

/// Represents the state of an on-going colloborative
/// state transfer protocol execution.
pub struct CollabStateTransfer<S: Service> {
    latest_cid: SeqNo,
    cst_seq: SeqNo,
    latest_cid_count: usize,
    // NOTE: remembers whose replies we have
    // received already, to avoid replays
    //voted: HashSet<NodeId>,
    received_states: HashMap<Digest, ReceivedState<State<S>, Request<S>>>,
    phase: ProtoPhase<State<S>, Request<S>>,
}

/// Status returned from processnig a state transfer message.
pub enum CstStatus<S, O> {
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
    State(RecoveryState<S, O>)
}

/// Represents progress in the CST state machine.
///
/// To clarify, the mention of state machine here has nothing to do with the
/// SMR protocol, but rather the implementation in code of the CST protocol.
pub enum CstProgress<S, O> {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, CstMessage<S, O>),
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

// TODO: request timeouts
impl<S> CollabStateTransfer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + Clone + 'static,
    Reply<S>: Send + 'static,
{
    /// Craete a new instance of `CollabStateTransfer`.
    pub fn new() -> Self {
        Self {
            received_states: collections::hash_map(),
            phase: ProtoPhase::Init,
            latest_cid: SeqNo::from(0u32),
            latest_cid_count: 0,
            cst_seq: SeqNo::from(0),
        }
    }

    /// Checks if the CST layer is waiting for a local checkpoint to
    /// complete.
    ///
    /// This is used when a node is sending state to a peer.
    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_, _))
    }

    fn process_reply_state(
        &mut self,
        view: ViewInfo,
        header: Header,
        message: CstMessage<State<S>, Request<S>>,
        log: &Log<State<S>, Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) {
        let snapshot = match log.snapshot(view) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                self.phase = ProtoPhase::WaitingCheckpoint(header, message);
                return;
            },
        };
        let reply = SystemMessage::Cst(CstMessage::new(
            message.sequence_number(),
            CstMessageKind::ReplyState(snapshot),
        ));
        node.send(reply, header.from());
    }

    /// Advances the state of the CST state machine.
    pub fn process_message(
        &mut self,
        progress: CstProgress<State<S>, Request<S>>,
        view: ViewInfo,
        consensus: &Consensus<S>,
        log: &Log<State<S>, Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> CstStatus<State<S>, Request<S>> {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_, _) => {
                let (header, message) = getmessage!(&mut self.phase);
                self.process_reply_state(view, header, message, log, node);
                CstStatus::Nil
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
                        self.process_reply_state(view, header, message, log, node);
                    },
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestState
                    _ => (),
                }
                CstStatus::Nil
            },
            ProtoPhase::ReceivingCid(i) => {
                let (_header, message) = getmessage!(progress, CstStatus::RequestLatestCid);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.cst_seq {
                    // FIXME: how to handle old or newer messages?
                    // BFT-SMaRt simply ignores messages with a
                    // value of `queryID` different from the current
                    // `queryID` a replica is tracking...
                    // we will do the same for now
                    //
                    // TODO: implement timeouts to fix cases like this
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
                //
                // TODO: check for more than one reply from the same node
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
            ProtoPhase::ReceivingState(i) => {
                let (header, mut message) = getmessage!(progress, CstStatus::RequestState);

                // NOTE: check comment above, on ProtoPhase::ReceivingCid
                if message.sequence_number() != self.cst_seq {
                    return CstStatus::Running;
                }

                let state = match message.take_state() {
                    Some(state) => state,
                    // drop invalid message kinds
                    None => return CstStatus::Running,
                };

                let received_state = self.received_states
                    .entry(header.digest().clone())
                    .or_insert(ReceivedState { count: 0, state });

                received_state.count += 1;

                // check if we have gathered enough state
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                if i != view.params().quorum() {
                    self.phase = ProtoPhase::ReceivingState(i);
                    return CstStatus::Running;
                }

                // NOTE: clear saved states when we return;
                // this is important, because each state
                // may be several GBs in size

                // check if we have at least f+1 matching states
                let digest = {
                    let received_state = self.received_states
                        .iter()
                        .max_by_key(|(_, st)| st.count);
                    match received_state {
                        Some((digest, _)) => digest.clone(),
                        None => {
                            self.received_states.clear();
                            return CstStatus::RequestState;
                        },
                    }
                };
                let received_state = {
                    let received_state = self.received_states
                        .remove(&digest);
                    self.received_states.clear();
                    received_state
                };

                // return the state
                match received_state {
                    Some(ReceivedState { count, state }) if count > view.params().f() => {
                        CstStatus::State(state)
                    },
                    _ => CstStatus::RequestState,
                }
            },
        }
    }

    /// Used by a recovering node to retrieve the latest sequence number
    /// attributed to a client request by the consensus layer.
    pub fn request_latest_consensus_seq_no(&mut self, _node: &mut Node<S::Data>) {
        // reset state of latest seq no
        self.latest_cid = SeqNo::from(0u32);
        self.latest_cid_count = 0;

        // ...

        // update our cst seq no
        self.cst_seq = self.cst_seq.next();

        unimplemented!()
    }

    /// Used by a recovering node to retrieve the latest state.
    pub fn request_latest_state(&mut self, _node: &mut Node<S::Data>) {
        // reset hashmap of received states
        self.received_states.clear();

        // ...

        // update our cst seq no
        self.cst_seq = self.cst_seq.next();

        unimplemented!()
    }
}
