use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use lazy_static::lazy_static;
use tracing::{debug, error, info, warn};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use thiserror::Error;

use atlas_common::channel::sync::ChannelSyncTx;
use atlas_common::collections::HashMap;
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::{collections, Err};
use atlas_communication::message::{Header, StoredMessage};
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::ordering_protocol::{ExecutionResult};
use atlas_core::persistent_log::{OperationMode, PersistableStateTransferProtocol};
use atlas_core::timeouts::timeout::{ModTimeout, TimeoutModHandle, TimeoutableMod};
use atlas_core::timeouts::{TimeoutID};
use atlas_metrics::metrics::metric_duration;
use atlas_smr_application::state::monolithic_state::{InstallStateMessage, MonolithicState};
use atlas_smr_core::persistent_log::MonolithicStateLog;
use atlas_smr_core::state_transfer::monolithic_state::{
    MonolithicStateTransfer, MonolithicStateTransferInitializer,
};
use atlas_smr_core::state_transfer::networking::StateTransferSendNode;
use atlas_smr_core::state_transfer::{
    Checkpoint, CstM, STPollResult, STResult, STTimeoutResult, StateTransferProtocol,
};

use crate::config::StateTransferConfig;
use crate::message::serialize::CSTMsg;
use crate::message::{CstMessage, CstMessageKind};
use crate::metrics::STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID;

pub mod config;
pub mod message;
pub mod metrics;

lazy_static!(
    static ref MOD_NAME: Arc<str> = Arc::from("ST_TRANSFER");
);

/// The state of the checkpoint
pub enum CheckpointState<D> {
    // no checkpoint has been performed yet
    None,
    // we are calling this a partial checkpoint because we are
    // waiting for the application state from the execution layer
    Partial {
        // sequence number of the last executed request
        seq: SeqNo,
    },
    PartialWithEarlier {
        // sequence number of the last executed request
        seq: SeqNo,
        // save the earlier checkpoint, in case corruption takes place
        earlier: Arc<ReadOnly<Checkpoint<D>>>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Arc<ReadOnly<Checkpoint<D>>>),
}

enum ProtoPhase<S> {
    Init,
    WaitingCheckpoint(Vec<StoredMessage<CstMessage<S>>>),
    ReceivingCid(usize),
    ReceivingState(usize),
}

impl<S> Debug for ProtoPhase<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoPhase::Init => {
                write!(f, "Init Phase")
            }
            ProtoPhase::WaitingCheckpoint(header) => {
                write!(f, "Waiting for checkpoint {}", header.len())
            }
            ProtoPhase::ReceivingCid(size) => {
                write!(f, "Receiving CID phase {} responses", size)
            }
            ProtoPhase::ReceivingState(size) => {
                write!(f, "Receiving state phase {} responses", size)
            }
        }
    }
}

/// Contains state used by a recovering node.
///
/// Cloning this is better than it was because of the read only checkpoint,
/// and because decision log also got a lot easier to clone, but we still have
/// to be very careful thanks to the requests vector, which can be VERY large
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct RecoveryState<S> {
    pub checkpoint: Arc<ReadOnly<Checkpoint<S>>>,
}

impl<S> RecoveryState<S> {
    /// Creates a new `RecoveryState`.
    pub fn new(checkpoint: Arc<ReadOnly<Checkpoint<S>>>) -> Self {
        Self { checkpoint }
    }

    /// Returns the local checkpoint of this recovery state.
    pub fn checkpoint(&self) -> &Arc<ReadOnly<Checkpoint<S>>> {
        &self.checkpoint
    }
}

#[derive(Debug)]
struct ReceivedState<S> {
    count: usize,
    state: RecoveryState<S>,
}

#[derive(Debug)]
struct ReceivedStateCid {
    cid: SeqNo,
    count: usize,
}

// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
/// The collaborative state transfer algorithm.
///
/// The implementation is based on the paper «On the Efﬁciency of
/// Durable State Machine Replication», by A. Bessani et al.
pub struct CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
{
    curr_seq: SeqNo,
    current_checkpoint_state: CheckpointState<S>,
    base_timeout: Duration,
    curr_timeout: Duration,
    timeouts: TimeoutModHandle,
    // NOTE: remembers whose replies we have
    // received already, to avoid replays
    //voted: HashSet<NodeId>,
    node: Arc<NT>,
    received_states: HashMap<Digest, ReceivedState<S>>,
    received_state_ids: HashMap<Digest, ReceivedStateCid>,
    phase: ProtoPhase<S>,

    install_channel: ChannelSyncTx<InstallStateMessage<S>>,

    /// Persistent logging for the state transfer protocol.
    persistent_log: PL,
}

/// Status returned from processing a state transfer message.
pub enum CstStatus<S> {
    /// We are not running the CST protocol.
    ///
    /// Drop any attempt of processing a message in this condition.
    Nil,
    /// The CST protocol is currently running.
    Running,
    /// We should request the latest cid from the view.
    RequestStateCid,
    /// We have received and validated the largest state sequence
    /// number available.
    SeqNo(SeqNo),
    /// We should request the latest state from the view.
    RequestState,
    /// We have received and validated the state from
    /// a group of replicas.
    State(RecoveryState<S>),
}

impl<S> Debug for CstStatus<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CstStatus::Nil => {
                write!(f, "Nil")
            }
            CstStatus::Running => {
                write!(f, "Running")
            }
            CstStatus::RequestStateCid => {
                write!(f, "Request latest CID")
            }
            CstStatus::RequestState => {
                write!(f, "Request latest state")
            }
            CstStatus::SeqNo(seq) => {
                write!(f, "Received seq no {:?}", seq)
            }
            CstStatus::State(_) => {
                write!(f, "Received state")
            }
        }
    }
}

/// Represents progress in the CST state machine.
///
/// To clarify, the mention of state machine here has nothing to do with the
/// SMR protocol, but rather the implementation in code of the CST protocol.
pub enum CstProgress<S> {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, CstMessage<S>),
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

impl<S, NT, PL> TimeoutableMod<STTimeoutResult> for CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
    PL: MonolithicStateLog<S> + 'static,
    NT: StateTransferSendNode<CSTMsg<S>> + 'static,
{
    fn mod_name() -> Arc<str> {
        MOD_NAME.clone()
    }

    fn handle_timeout(&mut self, timeout: Vec<ModTimeout>) -> Result<STTimeoutResult> {
        for cst_seq in timeout {
            if let TimeoutID::SeqNoBased(seq_no) = cst_seq.id() {
                /*if self.cst_request_timed_out(*seq_no, ) {
                    return Ok(STTimeoutResult::RunCst);
                }*/
            }
        }

        Ok(STTimeoutResult::CstNotNeeded)
    }
}

impl<S, NT, PL> StateTransferProtocol<S> for CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
    PL: MonolithicStateLog<S> + 'static,
    NT: StateTransferSendNode<CSTMsg<S>> + 'static,
{
    type Serialization = CSTMsg<S>;

    fn request_latest_state<V>(&mut self, view: V) -> Result<()>
    where
        V: NetworkView,
    {
        self.request_latest_consensus_seq_no::<V>(view);

        Ok(())
    }

    fn poll(&mut self) -> Result<STPollResult<CstM<Self::Serialization>>> {
        Ok(STPollResult::ReceiveMsg)
    }

    fn handle_off_ctx_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<CstM<Self::Serialization>>,
    ) -> Result<()>
    where
        V: NetworkView,
    {
        let (header, message) = message.into_inner();

        debug!(
            "{:?} // Off context Message {:?} from {:?} with seq {:?}",
            self.node.id(),
            message,
            header.from(),
            message.sequence_number()
        );

        match &message.kind() {
            CstMessageKind::RequestStateCid => {
                self.process_request_seq(header, message);

                return Ok(());
            }
            CstMessageKind::RequestState => {
                self.process_request_state(header, message);

                return Ok(());
            }
            _ => {}
        }

        let status = self.process_message(view, CstProgress::Message(header, message));

        match status {
            CstStatus::Nil => (),
            // should not happen...
            _ => {
                return Err(anyhow!(format!(
                    "Invalid state reached while state transfer processing message! {:?}",
                    status
                )));
            }
        }

        Ok(())
    }

    fn process_message<V>(
        &mut self,
        view: V,
        message: StoredMessage<CstM<Self::Serialization>>,
    ) -> Result<STResult>
    where
        V: NetworkView,
    {
        let (header, message) = message.into_inner();

        debug!(
            "{:?} // Message {:?} from {:?} while in phase {:?}",
            self.node.id(),
            message,
            header.from(),
            self.phase
        );

        match &message.kind() {
            CstMessageKind::RequestStateCid => {
                self.process_request_seq(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            CstMessageKind::RequestState => {
                self.process_request_state(header, message);

                return Ok(STResult::StateTransferRunning);
            }
            _ => {}
        }

        // Notify timeouts that we have received this message
        self.timeouts.ack_received(
            TimeoutID::SeqNoBased(message.sequence_number()),
            header.from(),
        )?;

        let status = self.process_message(view.clone(), CstProgress::Message(header, message));

        match status {
            CstStatus::Running => (),
            CstStatus::State(state) => {
                let start = Instant::now();

                self.install_channel
                    .send(InstallStateMessage::new(state.checkpoint.state().clone()))
                    .unwrap();

                metric_duration(STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID, start.elapsed());

                return Ok(STResult::StateTransferFinished(
                    state.checkpoint.sequence_number(),
                ));
            }
            CstStatus::SeqNo(seq) => {
                if self.current_checkpoint_state.sequence_number() < seq {
                    debug!("{:?} // Requesting state {:?}", self.node.id(), seq);

                    self.request_latest_state(view);
                } else {
                    debug!(
                        "{:?} // Not installing sequence number nor requesting state {:?} {:?}",
                        self.node.id(),
                        self.current_checkpoint_state.sequence_number(),
                        seq
                    );

                    return Ok(STResult::StateTransferNotNeeded(seq));
                }
            }
            CstStatus::RequestStateCid => {
                self.request_latest_consensus_seq_no(view);
            }
            CstStatus::RequestState => {
                self.request_latest_state(view);
            }
            CstStatus::Nil => {
                // No actions are required for the CST
                // This can happen for example when we already received the a quorum of sequence number replies
                // And therefore we are already in the Init phase and we are still receiving replies
                // And have not yet processed the
            }
        }

        Ok(STResult::StateTransferRunning)
    }

    fn handle_app_state_requested(&mut self, seq: SeqNo) -> Result<ExecutionResult> {
        let earlier = std::mem::replace(&mut self.current_checkpoint_state, CheckpointState::None);

        self.current_checkpoint_state = match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => {
                CheckpointState::PartialWithEarlier { seq, earlier }
            }
            // FIXME: this may not be an invalid state after all; we may just be generating
            // checkpoints too fast for the execution layer to keep up, delivering the
            // hash digests of the appstate
            _ => {
                error!("Invalid checkpoint state detected");

                self.current_checkpoint_state = earlier;

                return Ok(ExecutionResult::Nil);
            }
        };

        Ok(ExecutionResult::BeginCheckpoint)
    }
}

impl<S, NT, PL> MonolithicStateTransfer<S> for CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
    PL: MonolithicStateLog<S> + 'static,
    NT: StateTransferSendNode<CSTMsg<S>> + 'static,
{
    type Config = StateTransferConfig;

    fn handle_state_received_from_app(
        &mut self,
        state: Arc<ReadOnly<Checkpoint<S>>>,
    ) -> Result<()> {
        self.finalize_checkpoint(state)?;

        if self.needs_checkpoint() {
            self.process_pending_state_requests();
        }

        Ok(())
    }
}

impl<S, NT, PL> MonolithicStateTransferInitializer<S, NT, PL> for CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
    PL: MonolithicStateLog<S> + 'static,
    NT: StateTransferSendNode<CSTMsg<S>> + 'static,
{
    fn initialize(
        config: Self::Config,
        timeouts: TimeoutModHandle,
        node: Arc<NT>,
        log: PL,
        executor_handle: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Result<Self>
    where
        Self: Sized,
        NT: StateTransferSendNode<Self::Serialization>,
        PL: MonolithicStateLog<S>,
    {
        let StateTransferConfig { timeout_duration } = config;

        Ok(Self::new(
            node,
            timeout_duration,
            timeouts,
            log,
            executor_handle,
        ))
    }
}

type Ser<ST, S> = <ST as StateTransferProtocol<S>>::Serialization;

// TODO: request timeouts
impl<S, NT, PL> CollabStateTransfer<S, NT, PL>
where
    S: MonolithicState + 'static,
    PL: MonolithicStateLog<S> + 'static,
    NT: StateTransferSendNode<CSTMsg<S>> + 'static,
{
    /// Create a new instance of `CollabStateTransfer`.
    pub fn new(
        node: Arc<NT>,
        base_timeout: Duration,
        timeouts: TimeoutModHandle,
        persistent_log: PL,
        install_channel: ChannelSyncTx<InstallStateMessage<S>>,
    ) -> Self {
        Self {
            current_checkpoint_state: CheckpointState::None,
            base_timeout,
            curr_timeout: base_timeout,
            timeouts,
            node,
            received_states: collections::hash_map(),
            received_state_ids: collections::hash_map(),
            phase: ProtoPhase::Init,
            curr_seq: SeqNo::ZERO,
            persistent_log,
            install_channel,
        }
    }

    /// Checks if the CST layer is waiting for a local checkpoint to
    /// complete.
    ///
    /// This is used when a node is sending state to a peer.
    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_))
    }

    fn process_request_seq(&mut self, header: Header, message: CstMessage<S>) {
        let seq = match &self.current_checkpoint_state {
            CheckpointState::PartialWithEarlier { seq: _, earlier } => {
                Some((earlier.sequence_number(), *earlier.digest()))
            }
            CheckpointState::Complete(seq) => Some((seq.sequence_number(), *seq.digest())),
            _ => None,
        };

        let kind = CstMessageKind::ReplyStateCid(seq);

        let reply = CstMessage::new(message.sequence_number(), kind);

        debug!(
            "{:?} // Replying to {:?} seq {:?} with seq no {:?}",
            self.node.id(),
            header.from(),
            message.sequence_number(),
            seq
        );

        let _ = self.node.send_signed(reply, header.from(), true);
    }

    /// Process the entire list of pending state transfer requests
    /// This will only reply to the latest request sent by each of the replicas
    fn process_pending_state_requests(&mut self) {
        let waiting = std::mem::replace(&mut self.phase, ProtoPhase::Init);

        if let ProtoPhase::WaitingCheckpoint(reqs) = waiting {
            let mut map: HashMap<NodeId, StoredMessage<CstMessage<S>>> = collections::hash_map();

            for request in reqs {
                // We only want to reply to the most recent requests from each of the nodes
                if let std::collections::hash_map::Entry::Vacant(e) =
                    map.entry(request.header().from())
                {
                    e.insert(request);
                } else {
                    map.entry(request.header().from()).and_modify(|x| {
                        if x.message().sequence_number() < request.message().sequence_number() {
                            //Dispose of the previous request
                            let _ = std::mem::replace(x, request);
                        }
                    });

                    continue;
                }
            }

            map.into_values().for_each(|req| {
                let (header, message) = req.into_inner();

                self.process_request_state(header, message);
            });
        }
    }

    fn process_request_state(&mut self, header: Header, message: CstMessage<S>) {
        match &mut self.phase {
            ProtoPhase::Init => {}
            ProtoPhase::WaitingCheckpoint(waiting) => {
                waiting.push(StoredMessage::new(header, message));

                return;
            }
            _ => {
                // We can't reply to state requests when requesting state ourselves
                return;
            }
        }

        let state = match &self.current_checkpoint_state {
            CheckpointState::PartialWithEarlier { earlier, seq: _ } => earlier.clone(),
            CheckpointState::Complete(checkpoint) => checkpoint.clone(),
            _ => {
                if let ProtoPhase::WaitingCheckpoint(waiting) = &mut self.phase {
                    waiting.push(StoredMessage::new(header, message));
                } else {
                    self.phase =
                        ProtoPhase::WaitingCheckpoint(vec![StoredMessage::new(header, message)]);
                }

                return;
            }
        };

        let reply = CstMessage::new(
            message.sequence_number(),
            CstMessageKind::ReplyState(RecoveryState { checkpoint: state }),
        );

        self.node.send_signed(reply, header.from(), true).unwrap();
    }

    /// Advances the state of the CST state machine.
    pub fn process_message<V>(&mut self, view: V, progress: CstProgress<S>) -> CstStatus<S>
    where
        V: NetworkView,
    {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_) => {
                self.process_pending_state_requests();

                CstStatus::Nil
            }
            ProtoPhase::Init => {
                let (header, message) = getmessage!(progress, CstStatus::Nil);

                match message.kind() {
                    CstMessageKind::RequestStateCid => {
                        self.process_request_seq(header, message);
                    }
                    CstMessageKind::RequestState => {
                        self.process_request_state(header, message);
                    }
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestState
                    _ => (),
                }

                CstStatus::Nil
            }
            ProtoPhase::ReceivingCid(i) => {
                let (header, message) = getmessage!(progress, CstStatus::RequestStateCid);

                debug!("{:?} // Received Cid with {} responses from {:?} for CST Seq {:?} vs Ours {:?}", self.node.id(),
                   i, header.from(), message.sequence_number(), self.curr_seq);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.curr_seq {
                    debug!(
                        "{:?} // Wait what? {:?} {:?}",
                        self.node.id(),
                        self.curr_seq,
                        message.sequence_number()
                    );
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
                    CstMessageKind::ReplyStateCid(state_cid) => {
                        if let Some((cid, digest)) = state_cid {
                            debug!("{:?} // Received state cid {:?} with digest {:?} from {:?} with seq {:?}",
                            self.node.id(), state_cid, digest, header.from(), cid);

                            let currently_active_states = self
                                .received_state_ids
                                .entry(*digest)
                                .or_insert_with(|| ReceivedStateCid {
                                    cid: *cid,
                                    count: 0,
                                });

                            match (*cid).cmp(&currently_active_states.cid) {
                                Ordering::Greater => {
                                    info!("{:?} // Received newer state for old cid {:?} vs new cid {:?} with digest {:?}.",
                                    self.node.id(), currently_active_states.cid, *cid, digest);

                                    currently_active_states.cid = *cid;
                                    currently_active_states.count = 1;
                                }
                                Ordering::Equal => {
                                    info!("{:?} // Received matching state for cid {:?} with digest {:?}. Count {}",
                                self.node.id(), currently_active_states.cid, digest, currently_active_states.count + 1);

                                    currently_active_states.count += 1;
                                }
                                _ => {
                                    debug!(
                                        "{:?} // Received older state cid {:?} vs new cid {:?} with digest {:?}.",
                                        self.node.id(),
                                        currently_active_states.cid,
                                        *cid,
                                        digest
                                    );
                                }
                            }
                        } else {
                            debug!(
                                "{:?} // Received blank state cid from node {:?}",
                                self.node.id(),
                                header.from()
                            );
                        }
                    }
                    CstMessageKind::RequestStateCid => {
                        self.process_request_seq(header, message);

                        return CstStatus::Running;
                    }
                    CstMessageKind::RequestState => {
                        self.process_request_state(header, message);

                        return CstStatus::Running;
                    }
                    // drop invalid message kinds
                    _ => return CstStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                debug!(
                    "{:?} // Quorum count {}, i: {}, cst_seq {:?}. Current Latest Cid: {:?}",
                    self.node.id(),
                    view.quorum(),
                    i,
                    self.curr_seq,
                    self.received_state_ids
                );

                if i >= view.quorum() {
                    self.phase = ProtoPhase::Init;

                    // reset timeout, since req was successful
                    self.curr_timeout = self.base_timeout;

                    let mut received_state_ids: Vec<_> = self
                        .received_state_ids
                        .iter()
                        .map(|(digest, cid)| (digest, cid.cid, cid.count))
                        .collect();

                    received_state_ids
                        .sort_by(|(_, _, count), (_, _, count2)| count.cmp(count2).reverse());

                    if let Some((digest, seq, count)) = received_state_ids.first() {
                        if *count >= view.quorum() {
                            info!("{:?} // Received quorum of states for CST Seq {:?} with digest {:?} and seq {:?}",
                                self.node.id(), self.curr_seq, digest, seq);

                            return CstStatus::SeqNo(*seq);
                        } else {
                            warn!("Received quorum state messages but we still don't have a quorum of states? Faulty replica? {:?}", self.received_state_ids)
                        }
                    } else {
                        // If we are completely blank, then no replicas have state, so we can initialize

                        warn!("We have received a quorum of blank messages, which means we are probably at the start");
                        return CstStatus::SeqNo(SeqNo::ZERO);
                    }

                    // we don't need the latest cid to be available in at least
                    // f+1 replicas since the replica has the proof that the system
                    // has decided
                }

                self.phase = ProtoPhase::ReceivingCid(i);

                CstStatus::Running
            }
            ProtoPhase::ReceivingState(i) => {
                let (_header, mut message) = getmessage!(progress, CstStatus::RequestState);

                if message.sequence_number() != self.curr_seq {
                    // NOTE: check comment above, on ProtoPhase::ReceivingCid
                    return CstStatus::Running;
                }

                let state = match message.take_state() {
                    Some(state) => state,
                    // drop invalid message kinds
                    None => return CstStatus::Running,
                };

                let state_digest = *state.checkpoint.digest();

                debug!(
                    "{:?} // Received state with digest {:?}, is contained in map? {}",
                    self.node.id(),
                    state_digest,
                    self.received_states.contains_key(&state_digest)
                );

                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.received_states.entry(state_digest)
                {
                    e.insert(ReceivedState { count: 1, state });
                } else {
                    let current_state = self.received_states.get_mut(&state_digest).unwrap();

                    let current_state_seq: SeqNo =
                        current_state.state.checkpoint().sequence_number();
                    let recv_state_seq: SeqNo = state.checkpoint().sequence_number();

                    match recv_state_seq.cmp(&current_state_seq) {
                        Ordering::Less | Ordering::Equal => {
                            // we have just verified that the state is the same, but the decision log is
                            // smaller than the one we have already received
                            current_state.count += 1;
                        }
                        Ordering::Greater => {
                            current_state.state = state;
                            // We have also verified that the state is the same but the decision log is
                            // Larger, so we want to store the newest one. However we still want to increment the count
                            // We can do this since to be in the decision log, a replica must have all of the messages
                            // From at least 2f+1 replicas, so we know that the log is valid
                            current_state.count += 1;
                        }
                    }
                }

                // check if we have gathered enough state
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                if i <= view.f() {
                    self.phase = ProtoPhase::ReceivingState(i);
                    return CstStatus::Running;
                }

                // NOTE: clear saved states when we return;
                // this is important, because each state
                // may be several GBs in size

                // check if we have at least f+1 matching states
                let digest = {
                    let received_state = self.received_states.iter().max_by_key(|(_, st)| st.count);

                    match received_state {
                        Some((digest, _)) => *digest,
                        None => {
                            return if i >= view.quorum() {
                                self.received_states.clear();

                                debug!(
                                    "{:?} // No matching states found, clearing",
                                    self.node.id()
                                );
                                CstStatus::RequestState
                            } else {
                                CstStatus::Running
                            };
                        }
                    }
                };

                let received_state = {
                    let received_state = self.received_states.remove(&digest);
                    self.received_states.clear();
                    received_state
                };

                // reset timeout, since req was successful
                self.curr_timeout = self.base_timeout;

                // return the state
                let f = view.f();

                match received_state {
                    Some(ReceivedState { count, state }) if count > f => {
                        self.phase = ProtoPhase::Init;

                        info!("{:?} // Received quorum of states for CST Seq {:?} with digest {:?}, returning the state to the replica",
                            self.node.id(), self.curr_seq, digest);

                        CstStatus::State(state)
                    }
                    _ => {
                        debug!(
                            "{:?} // No states with more than f {} count",
                            self.node.id(),
                            f
                        );

                        CstStatus::RequestState
                    }
                }
            }
        }
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, checkpoint: Arc<ReadOnly<Checkpoint<S>>>) -> Result<()>
    where
        PL: MonolithicStateLog<S>,
    {
        match &self.current_checkpoint_state {
            CheckpointState::None => {
                Err!(StateTransferError::CheckpointNotInitiated)
            }
            CheckpointState::Complete(_) => {
                Err!(StateTransferError::CheckpointAlreadyFinalized)
            }
            CheckpointState::Partial { seq: _ }
            | CheckpointState::PartialWithEarlier { seq: _, .. } => {
                let checkpoint_state = CheckpointState::Complete(checkpoint.clone());

                self.current_checkpoint_state = checkpoint_state;

                self.persistent_log
                    .write_checkpoint(OperationMode::NonBlockingSync(None), checkpoint)?;

                Ok(())
            }
        }
    }

    fn curr_seq(&mut self) -> SeqNo {
        self.curr_seq
    }

    fn next_seq(&mut self) -> SeqNo {
        self.curr_seq = self.curr_seq.next();

        self.curr_seq
    }

    /// Handle a timeout received from the timeouts layer.
    /// Returns a bool to signify if we must move to the Retrieving state
    /// If the timeout is no longer relevant, returns false (Can remain in current phase)
    pub fn cst_request_timed_out<V>(&mut self, seq: SeqNo, view: V) -> bool
    where
        V: NetworkView,
    {
        let status = self.timed_out(seq);

        match status {
            CstStatus::RequestStateCid => {
                self.request_latest_consensus_seq_no(view);

                true
            }
            CstStatus::RequestState => {
                self.request_latest_state(view);

                true
            }
            // nothing to do
            _ => false,
        }
    }

    fn timed_out(&mut self, seq: SeqNo) -> CstStatus<S> {
        if seq != self.curr_seq {
            // the timeout we received is for a request
            // that has already completed, therefore we ignore it
            //
            // TODO: this check is probably not necessary,
            // as we have likely already updated the `ProtoPhase`
            // to reflect the fact we are no longer receiving state
            // from peer nodes
            return CstStatus::Nil;
        }

        self.next_seq();

        match self.phase {
            // retry requests if receiving state and we have timed out
            ProtoPhase::ReceivingCid(_) => {
                self.curr_timeout *= 2;
                CstStatus::RequestStateCid
            }
            ProtoPhase::ReceivingState(_) => {
                self.curr_timeout *= 2;
                CstStatus::RequestState
            }
            // ignore timeouts if not receiving any kind
            // of state from peer nodes
            _ => CstStatus::Nil,
        }
    }

    /// Used by a recovering node to retrieve the latest sequence number
    /// attributed to a client request by the consensus layer.
    pub fn request_latest_consensus_seq_no<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        // Reset the map of received state ids
        self.received_state_ids.clear();

        self.next_seq();

        let cst_seq = self.curr_seq();

        info!(
            "{:?} // Requesting latest state seq no with seq {:?}",
            self.node.id(),
            cst_seq
        );

        let _ = self.timeouts.request_timeout(
            TimeoutID::SeqNoBased(cst_seq),
            None,
            self.curr_timeout,
            view.quorum(),
            false,
        );

        self.phase = ProtoPhase::ReceivingCid(0);

        let message = CstMessage::new(cst_seq, CstMessageKind::RequestStateCid);

        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        let _ = self.node.broadcast_signed(message, targets);
    }

    /// Used by a recovering node to retrieve the latest state.
    pub fn request_latest_state<V>(&mut self, view: V)
    where
        V: NetworkView,
    {
        // reset hashmap of received states
        self.received_states.clear();

        self.next_seq();

        let cst_seq = self.curr_seq();

        info!(
            "{:?} // Requesting latest state with cst msg seq {:?}",
            self.node.id(),
            cst_seq
        );

        let _ = self.timeouts.request_timeout(
            TimeoutID::SeqNoBased(cst_seq),
            None,
            self.curr_timeout,
            view.quorum(),
            false,
        );

        self.phase = ProtoPhase::ReceivingState(0);

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas
        let message = CstMessage::new(cst_seq, CstMessageKind::RequestState);
        let targets = view
            .quorum_members()
            .clone()
            .into_iter()
            .filter(|id| *id != self.node.id());

        let _ = self.node.broadcast_signed(message, targets);
    }
}

impl<S, NT, PL> PersistableStateTransferProtocol for CollabStateTransfer<S, NT, PL> where
    S: MonolithicState + 'static
{
}

impl<S> Orderable for CheckpointState<S> {
    fn sequence_number(&self) -> SeqNo {
        match self {
            CheckpointState::None => SeqNo::ZERO,
            CheckpointState::Partial { seq: _ } => SeqNo::ZERO,
            CheckpointState::PartialWithEarlier { earlier, .. } => earlier.sequence_number(),
            CheckpointState::Complete(arc) => arc.sequence_number(),
        }
    }
}

#[derive(Error, Debug)]
pub enum StateTransferError {
    #[error("The checkpoint has already been finalized")]
    CheckpointAlreadyFinalized,
    #[error("No checkpoint has been initiated yet")]
    CheckpointNotInitiated,
}
