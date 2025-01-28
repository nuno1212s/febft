use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use either::Either;
use getset::Getters;
use intmap::IntMap;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{
    tbo_advance_message_queue, tbo_pop_message, tbo_queue_message_arc, Orderable, SeqNo,
};
use atlas_common::quiet_unwrap;
use atlas_common::serialization_helper::SerMsg;
use atlas_common::{collections, prng};

use atlas_communication::lookup_table::MessageModule;
use atlas_communication::message::{Header, StoredMessage, WireMessage};
use atlas_communication::reconfiguration::NetworkInformationProvider;
use atlas_core::messages::{ClientRqInfo, SessionBased};
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_core::ordering_protocol::reconfigurable_order_protocol::ReconfigurationAttemptResult;
use atlas_core::ordering_protocol::{unwrap_shareable_message, ShareableMessage};
use atlas_core::request_pre_processing::{RequestPProcessorSync, RequestPreProcessing};

use atlas_core::timeouts::timeout::{ModTimeout, TimeoutModHandle};

use crate::bft::consensus::{Consensus, ConsensusStatus};
use crate::bft::log::decisions::{CollectData, Proof, ViewDecisionPair};
use crate::bft::log::Log;
use crate::bft::message::{
    ConsensusMessage, ConsensusMessageKind, FwdConsensusMessage, PBFTMessage, ViewChangeMessage,
    ViewChangeMessageKind,
};
use crate::bft::sync::view::ViewInfo;
use crate::bft::{FeDecision, PBFT};

use self::{follower_sync::FollowerSynchronizer, replica_sync::ReplicaSynchronizer};

pub mod follower_sync;
pub mod replica_sync;
pub mod view;

/// Attempt to extract a msg from the tbo queue
/// If the message is not null (there is a message in the tbo queue)
/// The code provided in the first argument gets executed
/// The first T is the type of message that we should expect to be returned from the queue
macro_rules! extract_msg {
    ($t:ty => $g:expr, $q:expr) => {
        extract_msg!($t => {}, $g, $q)
    };

    ($t:ty => $opt:block, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ShareableMessage<PBFTMessage<$t>>>($q) {
            $opt
            SynchronizerPollStatus::NextMessage(stored)
        } else {
            *$g = false;
            SynchronizerPollStatus::Recv
        }
    };
}

macro_rules! stop_status {
    ($i:expr, $view:expr) => {{
        let f = $view.params().f();
        if $i > f {
            SynchronizerStatus::Running
        } else {
            SynchronizerStatus::Nil
        }
    }};
}

/// Finalize a view change with the given arguments
/// This will run the pre finalize, which will verify if we need to run the CST algorithm or not
/// And then runs the appropriate protocol
macro_rules! finalize_view_change {
    (
        $self:expr,
        $state:expr,
        $proof:expr,
        $normalized_collects:expr,
        $log:expr,
        $timeouts:expr,
        $consensus:expr,
        $node:expr $(,)?
    ) => {{
        match $self.pre_finalize($state, $proof, $normalized_collects, $log) {
            // wait for next timeout
            FinalizeStatus::NoValue => SynchronizerStatus::Running,
            // we need to run cst before proceeding with view change
            FinalizeStatus::RunCst(state) => {
                $self.finalize_state.replace(Some(state));
                $self.phase.replace(ProtoPhase::SyncingState);
                SynchronizerStatus::RunCst
            }
            // we may finish the view change proto
            FinalizeStatus::Commit(state) => {
                $self.finalize(state, $log, $timeouts, $consensus, $node)
            }
        }
    }};
}

/// Contains the `COLLECT` structures the leader received in the `STOP-DATA` phase
/// of the view change protocol, as well as a value to be proposed in the `SYNC` message.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Getters)]
pub struct LeaderCollects<O> {
    //The pre prepare message, created and signed by the leader to be executed when the view change is
    // Done
    #[get = "pub"]
    proposed: FwdConsensusMessage<O>,
    // The collect messages the leader has received.
    #[get = "pub"]
    collects: Vec<StoredMessage<PBFTMessage<O>>>,
}

impl<O> LeaderCollects<O> {
    pub fn new(
        proposed: FwdConsensusMessage<O>,
        collects: Vec<StoredMessage<PBFTMessage<O>>>,
    ) -> Self {
        Self { proposed, collects }
    }

    pub fn message(&self) -> &FwdConsensusMessage<O> {
        &self.proposed
    }

    /// Gives up ownership of the inner values of this `LeaderCollects`.
    pub fn into_inner(self) -> (FwdConsensusMessage<O>, Vec<StoredMessage<PBFTMessage<O>>>) {
        (self.proposed, self.collects)
    }
}

pub(super) struct FinalizeState<O> {
    curr_cid: SeqNo,
    sound: Sound,
    proposed: FwdConsensusMessage<O>,
    last_proof: Option<Proof<O>>,
}

pub(super) enum FinalizeStatus<O> {
    NoValue,
    RunCst(FinalizeState<O>),
    Commit(FinalizeState<O>),
}

///
#[derive(Clone, Debug)]
enum Sound {
    Unbound(bool),
    Bound(Digest),
}

impl Sound {
    fn value(&self) -> Option<&Digest> {
        match self {
            Sound::Bound(d) => Some(d),
            _ => None,
        }
    }

    fn test(&self) -> bool {
        match self {
            Sound::Unbound(ok) => *ok,
            _ => true,
        }
    }
}

/// Represents a queue of view change messages that arrive out of context into a node.
pub struct TboQueue<O> {
    // the current view
    view: ViewInfo,
    // The preview to the next view, in case we are in the process of changing views
    next_view: Option<ViewInfo>,
    // Stores the previous view, for useful information when changing views
    previous_view: Option<ViewInfo>,
    // probe messages from this queue instead of
    // fetching them from the network
    get_queue: bool,
    // stores all STOP messages for the next view
    stop: VecDeque<VecDeque<ShareableMessage<PBFTMessage<O>>>>,
    // stores all STOP-DATA messages for the next view
    stop_data: VecDeque<VecDeque<ShareableMessage<PBFTMessage<O>>>>,
    // stores all SYNC messages for the next view
    sync: VecDeque<VecDeque<ShareableMessage<PBFTMessage<O>>>>,
}

impl<O> TboQueue<O> {
    pub(crate) fn new(view: ViewInfo) -> Self {
        Self {
            view,
            next_view: None,
            previous_view: None,
            get_queue: false,
            stop: VecDeque::new(),
            stop_data: VecDeque::new(),
            sync: VecDeque::new(),
        }
    }

    pub fn install_next_view(&mut self, view: ViewInfo) {
        self.next_view = Some(view);
    }

    pub fn next_view(&self) -> Option<&ViewInfo> {
        self.next_view.as_ref()
    }

    /// Advance to the next view we are working on
    pub fn advance(&mut self) -> bool {
        if let Some(next_view) = self.next_view.take() {
            self.install_view(next_view)
        } else {
            false
        }
    }

    /// Installs a new view into the queue.
    /// clear_tbo indicates if we should clear the index corresponding to the installed view
    /// or not. (In the case of state transfers, for example we don't want to clear the index)
    pub fn install_view(&mut self, view: ViewInfo) -> bool {
        let index = view.sequence_number().index(self.view.sequence_number());

        match index {
            Either::Right(i) if i > 0 => {
                info!("Installing view {:?} into tbo queue, moving right by {}, currently at sequence number {:?}", view, i, self.view.sequence_number());

                let prev_view = std::mem::replace(&mut self.view, view);

                self.previous_view = Some(prev_view);

                for _ in 0..i {
                    self.next_instance_queue();
                }

                true
            }
            Either::Right(_) => {
                warn!("Installing a view with the same seq number as the current one?");
                false
            }
            Either::Left(_) => {
                unreachable!(
                    "How can we possibly go back in time? View {:?} vs our current {:?}",
                    view, self.view
                );
            }
        }
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// view change messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    fn next_instance_queue(&mut self) {
        tbo_advance_message_queue(&mut self.stop);
        tbo_advance_message_queue(&mut self.stop_data);
        tbo_advance_message_queue(&mut self.sync);
    }

    /// Queues a view change message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    pub fn queue(&mut self, m: ShareableMessage<PBFTMessage<O>>) {
        match m.message().view_change().kind() {
            ViewChangeMessageKind::Stop(_) | ViewChangeMessageKind::StopQuorumJoin(_) => {
                self.queue_stop(m)
            }
            ViewChangeMessageKind::StopData(_) => self.queue_stop_data(m),
            ViewChangeMessageKind::Sync(_) => self.queue_sync(m),
        }
    }

    /// Verifies if we have new `STOP` messages to be processed for
    /// the current view.
    pub fn can_process_stops(&self) -> bool {
        self.stop
            .front()
            .map(|deque| !deque.is_empty())
            .unwrap_or(false)
    }

    /// Verifies if we have new `STOP` messages to be processed for
    /// the current view.
    pub fn can_process_stop_data(&self) -> bool {
        self.stop_data
            .front()
            .map(|deque| !deque.is_empty())
            .unwrap_or(false)
    }

    /// Verifies if we have new `STOP` messages to be processed for
    /// the current view.
    pub fn can_process_sync(&self) -> bool {
        self.sync
            .front()
            .map(|deque| !deque.is_empty())
            .unwrap_or(false)
    }

    /// Queues a `STOP` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop(&mut self, m: ShareableMessage<PBFTMessage<O>>) {
        // NOTE: we use next() because we want to retrieve messages
        // for v+1, as we haven't started installing the new view yet
        let seq = self.view.sequence_number().next();
        tbo_queue_message_arc(seq, &mut self.stop, (m.sequence_number(), m))
    }

    /// Queues a `STOP-DATA` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop_data(&mut self, m: ShareableMessage<PBFTMessage<O>>) {
        let seq = self.view.sequence_number().next();
        tbo_queue_message_arc(seq, &mut self.stop_data, (m.sequence_number(), m))
    }

    /// Queues a `SYNC` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_sync(&mut self, m: ShareableMessage<PBFTMessage<O>>) {
        let seq = self.view.sequence_number().next();
        tbo_queue_message_arc(seq, &mut self.sync, (m.sequence_number(), m))
    }

    pub fn view(&self) -> &ViewInfo {
        &self.view
    }

    pub fn previous_view(&self) -> &Option<ViewInfo> {
        &self.previous_view
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) enum ProtoPhase {
    // the view change protocol isn't running;
    // we are watching pending client requests for
    // any potential timeouts
    Init,
    // we are running the stopping phase of the
    // Mod-SMaRt protocol
    Stopping(usize),
    // we are still running the stopping phase of
    // Mod-SMaRt, but we have either locally triggered
    // a view change, or received at least f+1 STOP msgs,
    // so we don't need to broadcast a new STOP;
    // this is effectively an implementation detail,
    // and not a real phase of Mod-SMaRt!
    Stopping2(usize),
    // we are running the STOP-QUORUM-JOIN phase
    ViewStopping(usize),
    // we are running the STOP-QUORUM-JOIN phase
    // but we have already locally triggered the view change
    // Or we have received at least f+1 STOP-QUORUM-JOIN messages.
    ViewStopping2(usize),
    // we are running the STOP-DATA phase of Mod-SMaRt
    StoppingData(usize),
    // we are running the SYNC phase of Mod-SMaRt
    Syncing,
    // we are running the SYNC phase of Mod-SMaRt,
    // but are paused while waiting for the state
    // transfer protocol to finish
    SyncingState,
}

// TODO: finish statuses returned from `process_message`
#[derive(Debug)]
pub enum SynchronizerStatus<O> {
    /// We are not running the view change protocol.
    Nil,
    /// The view change protocol is currently running.
    Running,
    /// The view change protocol just finished running.
    NewView(ConsensusStatus<O>, Option<FeDecision<O>>),
    /// The view change protocol just finished running and we
    /// have successfully joined the quorum.
    NewViewJoinedQuorum(ConsensusStatus<O>, Option<FeDecision<O>>, NodeId),
    /// Before we finish the view change protocol, we need
    /// to run the CST protocol.
    RunCst,
    /// The following set of client requests timed out.
    ///
    /// We need to invoke the leader change protocol if
    /// we have a non empty set of stopped messages.
    RequestsTimedOut {
        forwarded: Vec<ClientRqInfo>,
        stopped: Vec<ClientRqInfo>,
    },
}

/// Represents the status of calling `poll()` on a `Synchronizer`.
#[derive(Clone)]
pub enum SynchronizerPollStatus<O> {
    /// The `Replica` associated with this `Synchronizer` should
    /// poll its network channel for more messages, as it has no messages
    /// That can be processed in cache
    Recv,
    /// A new view change message is available to be processed, retrieved from the
    /// Ordered queue
    NextMessage(ShareableMessage<PBFTMessage<O>>),
    /// We need to resume the view change protocol, after
    /// running the CST protocol.
    ResumeViewChange,
}

/// The result of attempting to join a quorum
#[derive(Clone)]
pub enum SyncReconfigurationResult {
    // Something failed when attempting to reconfigure the quorum
    Failed,
    // There is already a view change being processed
    OnGoingViewChange,
    // There is already a quorum change being processed
    OnGoingQuorumChange(NodeId),
    // This node is already a part of the quorum
    AlreadyPartOfQuorum,
    // The change is currently in progress
    InProgress,
    // We have successfully completed the reconfiguration
    Completed,
}

///A trait describing some of the necessary methods for the synchronizer
pub trait AbstractSynchronizer<RQ>
where
    RQ: SerMsg,
{
    /// Returns information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo;

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn received_view_from_state_transfer(&self, view: ViewInfo) -> bool;

    fn queue(&self, message: ShareableMessage<PBFTMessage<RQ>>);
}

type CollectsType<RQ> = IntMap<u64, StoredMessage<PBFTMessage<RQ>>>;

///The synchronizer for the SMR protocol
/// This part of the protocol is responsible for handling the changing of views and
/// for keeping track of any timed out client requests
pub struct Synchronizer<RQ: SerMsg> {
    node_id: NodeId,

    phase: Cell<ProtoPhase>,
    //Tbo queue, keeps track of the current view and keeps messages arriving in order
    tbo: Mutex<TboQueue<RQ>>,
    //Stores currently received requests from other nodes
    stopped: RefCell<IntMap<u64, Vec<StoredMessage<RQ>>>>,
    //Stores currently received requests from other nodes
    currently_adding_node: Cell<Option<NodeId>>,
    //Stores which nodes are currently being added to the quorum, along with the number of votes
    //For each of the nodeIDs
    currently_adding: RefCell<BTreeMap<NodeId, BTreeSet<NodeId>>>,
    //TODO: This does not require a Mutex I believe since it's only accessed when
    // Processing messages (which is always done in the replica thread)
    collects: Mutex<CollectsType<RQ>>,
    // Used to store the finalize state when we are forced to run the CST protocol
    finalize_state: RefCell<Option<FinalizeState<RQ>>>,
    // We need to keep track of whether we are entering the quorum
    entering_quorum: Cell<bool>,
    // Replica accessory
    accessory: SynchronizerAccessory<RQ>,
}

///Justification/Sort of correction proof:
/// In general, all fields and methods will be accessed by the replica thread, never by the client rq thread.
/// Therefore, we only have to protect the fields that will be accessed by both clients and replicas.
/// So we protect collects, watching and tbo as those are the fields that are going to be
/// accessed by both those threads.
/// Since the other fields are going to be accessed by just 1 thread, we just need them to be Send, which they are
unsafe impl<RQ: SerMsg> Sync for Synchronizer<RQ> {}

impl<RQ> AbstractSynchronizer<RQ> for Synchronizer<RQ>
where
    RQ: SerMsg + SessionBased + 'static,
{
    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo {
        self.tbo.lock().unwrap().view().clone()
    }

    /// Install a new view received from the CST protocol
    /// This means that we are recovering from a failure or entering the
    /// system, so we have to catch up to any new information about the view
    /// Returns whether we should run the view change protocol to process pending
    /// messages
    fn received_view_from_state_transfer(&self, view: ViewInfo) -> bool {
        // FIXME: is the following line necessary?

        let current_view = self.view();

        if let Some(previous_view) = view.previous_view() {
            if current_view.sequence_number() != previous_view.sequence_number()
                && !self.tbo.lock().unwrap().install_view(previous_view)
            {
                // If we don't install a new view, then we don't want to forget our current state now do we?

                debug!("Replacing our phase with Init");
                self.phase.replace(ProtoPhase::Init);
            }

            // Perform various check to assert we obtain all the necessary information to
            // Recover from a fault

            if view.leader() == self.node_id && self.can_process_stop_data() {
                // If we are the leader of the new view, it means that we might still
                // Have time to actually process the messages that we received from the
                // Other nodes in time to maintain regency
                self.phase.replace(ProtoPhase::StoppingData(0));
            } else if self.can_process_sync() {
                // If we are not the leader, then we need to check for the same thing
                // But for sync messages. If we have a sync message in our queue,
                // It means that we probably didn't receive the first decision after
                // The view change in the log, which means we can't keep executing
                // Without first processing this sync message
                self.phase.replace(ProtoPhase::Syncing);
            } else {
                self.tbo.lock().unwrap().install_view(view);
                return false;
            }

            self.install_next_view(view);

            return true;
        } else {
            // This is the first view, so we can just install it
            if !self.tbo.lock().unwrap().install_view(view) {
                // If we don't install a new view, then we don't want to forget our current state now do we?
                debug!("Replacing our phase with Init");
                self.phase.replace(ProtoPhase::Init);
            }
        }

        false
    }

    fn queue(&self, message: ShareableMessage<PBFTMessage<RQ>>) {
        self.tbo.lock().unwrap().queue(message)
    }
}

impl<RQ> Synchronizer<RQ>
where
    RQ: SerMsg + SessionBased + 'static,
{
    pub fn new_follower(node_id: NodeId, view: ViewInfo) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            phase: Cell::new(ProtoPhase::Init),
            stopped: RefCell::new(Default::default()),
            currently_adding_node: Cell::new(None),
            currently_adding: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
            entering_quorum: Cell::new(false),
            accessory: SynchronizerAccessory::Follower(FollowerSynchronizer::new()),
        })
    }

    pub fn new_replica(node_id: NodeId, view: ViewInfo, timeout_dur: Duration) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            phase: Cell::new(ProtoPhase::Init),
            stopped: RefCell::new(Default::default()),
            currently_adding_node: Cell::new(None),
            currently_adding: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
            entering_quorum: Cell::new(false),
            accessory: SynchronizerAccessory::Replica(ReplicaSynchronizer::new(timeout_dur)),
        })
    }

    /// Initialize a new `Synchronizer` with the given quorum members.
    pub fn initialize_with_quorum(
        node_id: NodeId,
        seq_no: SeqNo,
        quorum_members: Vec<NodeId>,
        timeout_dur: Duration,
    ) -> Result<Arc<Self>> {
        let n = quorum_members.len();

        let _f = (n - 1) / 3;

        let view_info = ViewInfo::from_quorum(seq_no, quorum_members)?;

        info!("Initializing synchronizer with view {:?}", view_info);

        Ok(Arc::new(Self {
            node_id,
            phase: Cell::new(ProtoPhase::Init),
            tbo: Mutex::new(TboQueue::new(view_info)),
            stopped: RefCell::new(Default::default()),
            currently_adding_node: Cell::new(None),
            currently_adding: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            finalize_state: RefCell::new(None),
            entering_quorum: Cell::new(false),
            accessory: SynchronizerAccessory::Replica(ReplicaSynchronizer::new(timeout_dur)),
        }))
    }

    /// The next view that is going to be processed
    fn next_view(&self) -> Option<ViewInfo> {
        self.tbo.lock().unwrap().next_view().cloned()
    }

    /// The previous view that was processed
    fn previous_view(&self) -> Option<ViewInfo> {
        self.tbo.lock().unwrap().previous_view().clone()
    }

    /// Install the next view which we are currently working on changing to
    fn install_next_view(&self, view: ViewInfo) {
        self.tbo.lock().unwrap().install_next_view(view)
    }

    /// Advance the view the next one in the queue
    fn advance_view(&self) -> bool {
        self.tbo.lock().unwrap().advance()
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// view change messages from its internal storage.
    pub fn signal(&self) {
        self.tbo.lock().unwrap().signal()
    }

    /// Verifies if we have new `STOP` messages to be processed for
    /// the next view.
    pub fn can_process_stops(&self) -> bool {
        self.tbo.lock().unwrap().can_process_stops()
    }

    /// Verifies if we have new `STOP_DATA` messages to be processed for
    /// the next view.
    pub fn can_process_stop_data(&self) -> bool {
        self.tbo.lock().unwrap().can_process_stop_data()
    }

    /// Verifies if we have new `SYNC` messages to be processed for
    /// the next view.
    pub fn can_process_sync(&self) -> bool {
        self.tbo.lock().unwrap().can_process_sync()
    }

    /// Check if we can process new view change messages.
    /// If there are pending messages that are now processable (but weren't when we received them)
    /// We return them. If there are no pending messages then we will wait for new messages from other replicas
    pub fn poll(&self) -> SynchronizerPollStatus<RQ> {
        let mut tbo_guard = self.tbo.lock().unwrap();
        match self.phase.get() {
            _ if !tbo_guard.get_queue => SynchronizerPollStatus::Recv,
            ProtoPhase::Init => {
                //If we are in the init phase and there is a pending request, move to the stopping phase
                let result = extract_msg!(RQ  =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                );

                if let SynchronizerPollStatus::NextMessage(message) = &result {
                    match message.message().view_change().kind() {
                        ViewChangeMessageKind::StopQuorumJoin(_) => {
                            self.phase.replace(ProtoPhase::ViewStopping(0));
                        }
                        _ => {
                            self.phase.replace(ProtoPhase::Stopping(0));
                        }
                    }
                };

                result
            }
            ProtoPhase::Stopping(_)
            | ProtoPhase::Stopping2(_)
            | ProtoPhase::ViewStopping(_)
            | ProtoPhase::ViewStopping2(_) => {
                extract_msg!(RQ=>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                )
            }
            ProtoPhase::StoppingData(_) => {
                extract_msg!(RQ  =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop_data
                )
            }
            ProtoPhase::Syncing => {
                extract_msg!(RQ  =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.sync
                )
            }
            ProtoPhase::SyncingState => SynchronizerPollStatus::ResumeViewChange,
        }
    }

    /// Advances the state of the view change state machine.
    //
    // TODO: retransmit STOP msgs
    pub fn process_message<NT, RP>(
        &self,
        s_message: ShareableMessage<PBFTMessage<RQ>>,
        timeouts: &TimeoutModHandle,
        log: &mut Log<RQ>,
        rq_pre_processor: &RP,
        consensus: &mut Consensus<RQ>,
        node: &Arc<NT>,
    ) -> SynchronizerStatus<RQ>
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
        RP: RequestPreProcessing<RQ> + RequestPProcessorSync<RQ>,
    {
        debug!(
            "{:?} // Processing view change message {:?} in phase {:?} from {:?}",
            node.id(),
            s_message.message().view_change(),
            self.phase.get(),
            s_message.header().from()
        );

        match self.phase.get() {
            ProtoPhase::Init => {
                let (_header, message) = (s_message.header(), s_message.message().view_change());

                return match message.kind() {
                    ViewChangeMessageKind::Stop(_) | ViewChangeMessageKind::StopQuorumJoin(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        debug!(
                            "{:?} // Received {:?} message while in init state. Queueing",
                            node.id(),
                            message
                        );

                        guard.queue_stop(s_message);

                        SynchronizerStatus::Nil
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                SynchronizerStatus::Nil
                            }
                            SynchronizerAccessory::Replica(_) => {
                                let mut guard = self.tbo.lock().unwrap();
                                debug!("{:?} // Received stop data {:?} message while in init state. Queueing", node.id(), message);

                                guard.queue_stop_data(s_message);

                                SynchronizerStatus::Nil
                            }
                        }
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(s_message);

                        debug!(
                            "{:?} // Received sync message while in init state. Queueing",
                            node.id()
                        );

                        SynchronizerStatus::Nil
                    }
                };
            }
            ProtoPhase::Stopping(i) | ProtoPhase::Stopping2(i) => {
                let (header, message) = (s_message.header(), s_message.message().view_change());

                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let next_seq = current_view.sequence_number().next();

                let i = match message.kind() {
                    ViewChangeMessageKind::Stop(_) | ViewChangeMessageKind::StopQuorumJoin(_)
                        if msg_seq != next_seq =>
                    {
                        debug!("{:?} // Received stop message {:?} that does not match up to our local view {:?}", node.id(), message, current_view);

                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(s_message);

                        return stop_status!(i, &current_view);
                    }
                    ViewChangeMessageKind::Stop(_)
                        if self.stopped.borrow().contains_key(header.from().into()) =>
                    {
                        warn!(
                            "{:?} // Received double stop message from node {:?}",
                            node.id(),
                            header.from()
                        );

                        // drop attempts to vote twice
                        return stop_status!(i, &current_view);
                    }
                    ViewChangeMessageKind::StopQuorumJoin(_) => {
                        warn!("{:?} // Received stop quorum join message while in stopping state. Ignoring", node.id());

                        return stop_status!(i, &current_view);
                    }
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        return match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                stop_status!(i, &current_view)
                            }
                            SynchronizerAccessory::Replica(_) => {
                                {
                                    let mut guard = self.tbo.lock().unwrap();

                                    guard.queue_stop_data(s_message);

                                    debug!("{:?} // Received stop data message while in stopping state. Queueing", node.id());
                                }

                                stop_status!(i, &current_view)
                            }
                        };
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        {
                            let mut guard = self.tbo.lock().unwrap();

                            guard.queue_sync(s_message);

                            debug!(
                                "{:?} // Received sync message while in init state. Queueing",
                                node.id()
                            );
                        }

                        return stop_status!(i, &current_view);
                    }
                };

                // store pending requests from this STOP
                let stopped = match message.kind() {
                    ViewChangeMessageKind::Stop(stopped) => stopped.clone(),
                    _ => unreachable!(),
                };

                // FIXME: Check if we have already seen the messages in the stop quorum

                self.stopped
                    .borrow_mut()
                    .insert(header.from().into(), stopped);

                // NOTE: we only take this branch of the code before
                // we have sent our own STOP message
                if let ProtoPhase::Stopping(_i) = self.phase.get() {
                    return if i > current_view.params().f() {
                        self.begin_view_change(None, &**node, timeouts, log);

                        SynchronizerStatus::Running
                    } else {
                        self.phase.replace(ProtoPhase::Stopping(i));

                        SynchronizerStatus::Nil
                    };
                }

                if i >= current_view.params().quorum() {
                    let next_view = current_view.next_view();

                    let previous_view = current_view.clone();

                    //We have received the necessary amount of stopping requests
                    //To now that we should move to the next view

                    let next_leader = next_view.leader();

                    warn!(
                        "{:?} // Stopping quorum reached, moving to next view {:?}. ",
                        node.id(),
                        next_view
                    );

                    self.install_next_view(next_view);

                    match &self.accessory {
                        SynchronizerAccessory::Replica(rep) => rep.handle_stopping_quorum(
                            self,
                            previous_view,
                            consensus,
                            log,
                            rq_pre_processor,
                            timeouts,
                            &**node,
                        ),
                        SynchronizerAccessory::Follower(_) => {}
                    }

                    if next_leader == node.id() {
                        warn!(
                            "{:?} // I am the new leader, moving to the stopping data phase.",
                            node.id()
                        );

                        //Move to the stopping data phase as we are the new leader
                        self.phase.replace(ProtoPhase::StoppingData(0));
                    } else {
                        self.phase.replace(ProtoPhase::Syncing);
                    }
                } else {
                    self.phase.replace(ProtoPhase::Stopping2(i));
                }

                SynchronizerStatus::Running
            }
            ProtoPhase::ViewStopping(received) | ProtoPhase::ViewStopping2(received) => {
                let (header, message) = (s_message.header(), s_message.message().view_change());

                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let next_seq = current_view.sequence_number().next();

                let (received, node_id) = match message.kind() {
                    ViewChangeMessageKind::Stop(_) | ViewChangeMessageKind::StopQuorumJoin(_)
                        if msg_seq != next_seq =>
                    {
                        debug!("{:?} // Received stop message {:?} that does not match up to our local view {:?}", node.id(), message, current_view);

                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(s_message);

                        return stop_status!(received, &current_view);
                    }
                    ViewChangeMessageKind::Stop(_requests) => {
                        let mut guard = self.tbo.lock().unwrap();

                        warn!("{:?} // Received stop message while in View Stopping state. Since STOP takes precendence over the quorum updating, we will now change to stopping phase ", node.id());

                        guard.queue_stop(s_message);

                        self.phase.replace(ProtoPhase::Stopping(0));

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopQuorumJoin(node) => (received + 1, *node),
                    ViewChangeMessageKind::StopData(_) => {
                        return match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                stop_status!(received, &current_view)
                            }
                            SynchronizerAccessory::Replica(_) => {
                                {
                                    let mut guard = self.tbo.lock().unwrap();

                                    guard.queue_stop_data(s_message);

                                    debug!("{:?} // Received stop data message while in stopping state. Queueing", node.id());
                                }

                                stop_status!(received, &current_view)
                            }
                        };
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        {
                            let mut guard = self.tbo.lock().unwrap();

                            guard.queue_sync(s_message);

                            debug!(
                                "{:?} // Received sync message while in init state. Queueing",
                                node.id()
                            );
                        }

                        return stop_status!(received, &current_view);
                    }
                };

                {
                    let mut write_guard = self.currently_adding.borrow_mut();

                    let received_votes = write_guard.entry(node_id).or_default();

                    if received_votes.insert(header.from()) {
                        debug!(
                            "{:?} // Received stop quorum join message from {:?} with node {:?} ",
                            node.id(),
                            header.from(),
                            node_id
                        );
                    } else {
                        debug!("{:?} // Received duplicate stop quorum join message from {:?} with node {:?} ", node.id(), header.from(), node_id);
                    }
                }

                // We don't need to actually receive the reconfiguration confirmation to add a node to the quorum, if the quorum is already reached
                //TODO: Is this the correct procedure?
                self.phase.replace(ProtoPhase::ViewStopping(received));

                if received >= current_view.params().quorum() {
                    let mut votes: Vec<_> = self
                        .currently_adding
                        .borrow()
                        .iter()
                        .map(|(node, voters)| (*node, voters.len()))
                        .collect();

                    votes.sort_by(|(_node, votes), (_node_2, votes_2)| votes_2.cmp(votes));

                    if let Some(vote_count) = votes.first() {
                        if vote_count.1 >= current_view.params().quorum() {
                            self.currently_adding_node.replace(Some(node_id));

                            let node_to_add = vote_count.0;

                            let next_view = current_view.next_view_with_new_node(node_to_add);

                            let previous_view = current_view.clone();

                            //We have received the necessary amount of stopping requests
                            //To now that we should move to the next view

                            let next_leader = next_view.leader();

                            warn!("{:?} // Stopping quorum reached with {} votes for node {:?} moving to next view {:?}. ", node.id(), vote_count.1, node_to_add, next_view);

                            self.install_next_view(next_view);

                            match &self.accessory {
                                SynchronizerAccessory::Replica(rep) => rep.handle_stopping_quorum(
                                    self,
                                    previous_view,
                                    consensus,
                                    log,
                                    rq_pre_processor,
                                    timeouts,
                                    &**node,
                                ),
                                SynchronizerAccessory::Follower(_) => {}
                            }

                            if next_leader == node.id() {
                                warn!("{:?} // I am the new leader, moving to the stopping data phase.", node.id());

                                //Move to the stopping data phase as we are the new leader
                                self.phase.replace(ProtoPhase::StoppingData(0));
                            } else {
                                self.phase.replace(ProtoPhase::Syncing);
                            }
                        } else if received >= current_view.params().n() {
                            error!("We have received view stopping messages from all nodes in the network and yet we don't have quorum {} votes for any node. {:?}",
                                   current_view.params().quorum(), votes);

                            todo!("")
                        } else {
                            warn!("{:?} // Stopping quorum reached, but not enough votes to add node {:?}. ", node.id(), vote_count.0);
                        }
                    }
                } else {
                    self.phase.replace(ProtoPhase::ViewStopping2(received));
                }

                SynchronizerStatus::Running
            }
            ProtoPhase::StoppingData(i) => {
                let (header, message) = (s_message.header(), s_message.message().view_change());

                match &self.accessory {
                    SynchronizerAccessory::Follower(_) => {
                        //Since a follower can never be a leader (as he isn't a part of the
                        // quorum, he can never be in this state)
                        unreachable!()
                    }
                    SynchronizerAccessory::Replica(_rep) => {
                        //Obtain the view seq no of the message
                        let msg_seq = message.sequence_number();

                        let next_view = self
                            .next_view()
                            .expect("We can't be here without having installed the next view?");
                        let seq = next_view.sequence_number();

                        // reject STOP-DATA messages if we are not the leader
                        let mut collects_guard = self.collects.lock().unwrap();

                        let i = match message.kind() {
                            ViewChangeMessageKind::Stop(_)
                            | ViewChangeMessageKind::StopQuorumJoin(_) => {
                                {
                                    let mut guard = self.tbo.lock().unwrap();

                                    guard.queue_stop(s_message);

                                    debug!("{:?} // Received stop message while in stopping data state. Queueing", node.id());
                                }

                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_) if msg_seq != seq => {
                                warn!("{:?} // Received stop data message for view {:?} but we are in view {:?}",
                                      node.id(), msg_seq, seq);

                                if next_view.peek(msg_seq).leader() == node.id() {
                                    warn!("{:?} // We are the leader of the view of the received message, so we will accept it",
                                      node.id());

                                    //If we are the leader of the view the message is in,
                                    //Then we want to accept the message, but since it is not the current
                                    //View, then it cannot be processed atm
                                    {
                                        let mut guard = self.tbo.lock().unwrap();

                                        guard.queue_stop_data(s_message);
                                    }
                                }

                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_)
                                if next_view.leader() != node.id() =>
                            {
                                warn!("{:?} // Received stop data message but we are not the leader of the current view",
                                      node.id());
                                //If we are not the leader, ignore
                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_)
                                if collects_guard.contains_key(header.from().into()) =>
                            {
                                warn!("{:?} // Received stop data message but we have already received one from this node",
                                      node.id());
                                // drop attempts to vote twice
                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_) => {
                                // The message is related to the view we are awaiting
                                // In order to reach this point, we must be the leader of the current view,
                                // The vote must not be repeated
                                i + 1
                            }
                            ViewChangeMessageKind::Sync(_) => {
                                {
                                    let mut guard = self.tbo.lock().unwrap();
                                    //Since we are the current leader and are waiting for stop data,
                                    //This must be related to another view.
                                    guard.queue_sync(s_message);
                                }

                                debug!("{:?} // Received sync message while in stopping data phase. Queueing", node.id());

                                return SynchronizerStatus::Running;
                            }
                        };

                        // NOTE: the STOP-DATA message signatures are already
                        // verified by the TLS layer, but we still need to
                        // verify their content when we retransmit the COLLECT's
                        // to other nodes via a SYNC message! this guarantees
                        // the new leader isn't forging messages.

                        // store collects from this STOP-DATA
                        let unwrapped_msg = unwrap_shareable_message(s_message);

                        let (header, _message) = (unwrapped_msg.header(), unwrapped_msg.message());

                        collects_guard.insert(header.from().into(), unwrapped_msg);

                        if i < next_view.params().quorum() {
                            self.phase.replace(ProtoPhase::StoppingData(i));

                            SynchronizerStatus::Running
                        } else {
                            // NOTE:
                            // - fetch highest CID from consensus proofs
                            // - broadcast SYNC msg with collected
                            //   STOP-DATA proofs so other replicas
                            //   can repeat the leader's computation

                            let current_view = self.view();

                            //Since all of these requests were done in the previous view of the algorithm
                            // then we should also use the previous view to verify the validity of them
                            let previous_view_ref = &current_view;

                            let proof =
                                Self::highest_proof(&*collects_guard, previous_view_ref, &**node);

                            info!("{:?} // Highest proof: {:?}", node.id(), proof);

                            let curr_cid = proof
                                .map(|p| p.sequence_number())
                                .map(|seq| SeqNo::from(u32::from(seq) + 1))
                                .unwrap_or(SeqNo::ZERO);

                            //Here we use the normalized_collects method, which uses data from self.collects
                            //Which is protected by a mutex. Therefore, we must carry the consensus guard along
                            //While we access the normalized collects to prevent any errors.
                            let normalized_collects: Vec<Option<&CollectData<RQ>>> =
                                Self::normalized_collects(&*collects_guard, curr_cid).collect();

                            let sound = sound(&next_view, &normalized_collects);

                            if !sound.test() {
                                //FIXME: BFT-SMaRt doesn't do anything if `sound`
                                // evaluates to false; do we keep the same behavior,
                                // and wait for a new time out? but then, no other
                                // consensus messages have been processed... this
                                // may be a point of contention on the lib!

                                error!(
                                    "{:?} // The view change is not sound. Cancelling.",
                                    node.id()
                                );
                                /*
                                collects_guard.clear();

                                return SynchronizerStatus::Running;
                                */
                            }

                            let p = quiet_unwrap!(
                                rq_pre_processor.collect_pending_rqs(),
                                SynchronizerStatus::Running
                            );
                            let node_sign = node.network_info_provider().get_key_pair().clone();

                            //We create the pre-prepare here as we are the new leader,
                            //And we sign it right now
                            let (header, message) = {
                                info!("{:?} // Forged pre-prepare: {}", node.id(), p.len());

                                let forged_pre_prepare = consensus.forge_propose(p, &next_view);

                                let (message, digest) =
                                    node.serialize_digest_message(forged_pre_prepare).unwrap();

                                let (message, buf) = message.into_inner();

                                let mut prng_state = prng::State::new();

                                //Create the pre-prepare message that contains the requests
                                //Collected during the STOPPING DATA phase
                                let (h, _, _) = WireMessage::new(
                                    next_view.leader(),
                                    next_view.leader(),
                                    MessageModule::Protocol,
                                    buf,
                                    prng_state.next_state(),
                                    Some(digest),
                                    Some(&*node_sign),
                                )
                                .into_inner();

                                (h, message)
                            };

                            let message = match message {
                                PBFTMessage::Consensus(msg) => msg,
                                _ => unreachable!(),
                            };

                            let fwd_request = FwdConsensusMessage::new(header, message);

                            let collects = collects_guard.values().cloned().collect();

                            let message = PBFTMessage::ViewChange(ViewChangeMessage::new(
                                next_view.sequence_number(),
                                ViewChangeMessageKind::Sync(LeaderCollects {
                                    proposed: fwd_request.clone(),
                                    collects,
                                }),
                            ));

                            let our_id = node.id();

                            let targets = next_view
                                .quorum_members()
                                .clone()
                                .into_iter()
                                .filter(move |&id| id != our_id);

                            let _ = node.broadcast_signed(message, targets);

                            let state = FinalizeState {
                                curr_cid,
                                sound,
                                proposed: fwd_request,
                                last_proof: proof.cloned(),
                            };

                            finalize_view_change!(
                                self,
                                state,
                                proof,
                                normalized_collects,
                                log,
                                timeouts,
                                consensus,
                                node,
                            )
                        }
                    }
                }
            }
            ProtoPhase::Syncing => {
                let msg_seq = s_message.sequence_number();
                let next_view = self
                    .next_view()
                    .expect("We should have a next view in this situation");
                let seq = next_view.sequence_number();

                // reject SYNC messages if these were not sent by the leader
                let (proposed, collects) = match s_message.message().view_change().kind() {
                    ViewChangeMessageKind::Stop(_) | ViewChangeMessageKind::StopQuorumJoin(_) => {
                        {
                            let mut guard = self.tbo.lock().unwrap();

                            guard.queue_stop(s_message);

                            debug!(
                                "{:?} // Received stop message while in syncing phase. Queueing",
                                node.id()
                            );
                        }

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        return match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                SynchronizerStatus::Running
                            }
                            SynchronizerAccessory::Replica(_) => {
                                //FIXME: We are not the leader of this view so we can't receive stop data messages
                                // For this view. The only possibility is that we are the leader of the view
                                // This stop data message is for
                                {
                                    let mut guard = self.tbo.lock().unwrap();

                                    guard.queue_stop_data(s_message);

                                    debug!("{:?} // Received stop data message while in syncing phase. Queueing", node.id());
                                }

                                SynchronizerStatus::Running
                            }
                        };
                    }
                    ViewChangeMessageKind::Sync(_) if msg_seq != seq => {
                        {
                            debug!("{:?} // Received sync message whose sequence number does not match our current one {:?} vs {:?}. Queueing", node.id(),
                                s_message.message().view_change(), next_view);

                            let mut guard = self.tbo.lock().unwrap();

                            guard.queue_sync(s_message);
                        }

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_)
                        if s_message.header().from() != next_view.leader() =>
                    {
                        //You're not the leader, what are you saying
                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let stored_message = unwrap_shareable_message(s_message);

                        let (_header, message) = stored_message.into_inner();

                        message
                            .into_view_change()
                            .take_collects()
                            .unwrap()
                            .into_inner()
                    }
                };

                // leader has already performed this computation in the
                // STOP-DATA phase of Mod-SMaRt
                let signed: Vec<_> = signed_collects::<RQ, _>(&**node, collects);

                let proof = highest_proof::<RQ, _, _>(&next_view, &**node, signed.iter());

                let curr_cid = proof
                    .map(|p| p.sequence_number())
                    .map(|seq| seq.next())
                    .unwrap_or(SeqNo::ZERO);

                let normalized_collects: Vec<_> =
                    { normalized_collects(curr_cid, collect_data(signed.iter())).collect() };

                let sound = sound(&next_view, &normalized_collects);

                if !sound.test() {
                    error!(
                        "{:?} // The view change is not sound. Cancelling.",
                        node.id()
                    );

                    //FIXME: BFT-SMaRt doesn't do anything if `sound`
                    // evaluates to false; do we keep the same behavior,
                    // and wait for a new time out? but then, no other
                    // consensus messages have been processed... this
                    // may be a point of contention on the lib!
                    //return SynchronizerStatus::Running;
                }

                let state = FinalizeState {
                    curr_cid,
                    sound,
                    proposed,
                    last_proof: proof.cloned(),
                };

                finalize_view_change!(
                    self,
                    state,
                    proof,
                    normalized_collects,
                    log,
                    timeouts,
                    consensus,
                    node,
                )
            }

            // handled by `resume_view_change()`
            ProtoPhase::SyncingState => unreachable!(),
        }
    }

    /// Resume the view change protocol after running the CST protocol.
    pub fn resume_view_change<NT>(
        &self,
        log: &mut Log<RQ>,
        timeouts: &TimeoutModHandle,
        consensus: &mut Consensus<RQ>,
        node: &Arc<NT>,
    ) -> Option<SynchronizerStatus<RQ>>
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        let state = self.finalize_state.borrow_mut().take()?;

        //This is kept alive until it is out of the scope
        let _lock_guard = self.collects.lock().unwrap();

        Some(finalize_view_change!(
            self,
            state,
            None,
            Vec::new(),
            log,
            timeouts,
            consensus,
            node,
        ))
    }

    /// Start the quorum join procedure to integrate the given joining node into the current quorum
    /// of the system
    pub fn start_join_quorum<NT>(
        &self,
        joining_node: NodeId,
        node: &NT,
        timeouts: &TimeoutModHandle,
        log: &Log<RQ>,
    ) -> SyncReconfigurationResult
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        let current_view = self.view();

        info!(
            "{:?} // Starting the quorum join procedure for node {:?}",
            node.id(),
            joining_node
        );

        if current_view.quorum_members().contains(&joining_node) {
            //We are already a part of the quorum, so we don't need to do anything
            info!(
                "{:?} // Attempted to add node {:?} quorum but it is already a part of the quorum",
                node.id(),
                joining_node
            );

            return SyncReconfigurationResult::AlreadyPartOfQuorum;
        }

        match self.phase.get() {
            ProtoPhase::Init => {
                // This means this is ready to change views
            }
            ProtoPhase::StoppingData(_) | ProtoPhase::SyncingState | ProtoPhase::Syncing => {
                return if let Some(currently_adding) = self.currently_adding_node.get() {
                    info!("{:?} // Attempted to add node {:?} quorum but we are currently already adding another node to it {:?}", node.id(), joining_node, currently_adding);

                    SyncReconfigurationResult::OnGoingQuorumChange(currently_adding)
                } else {
                    SyncReconfigurationResult::OnGoingViewChange
                };
            }
            ProtoPhase::ViewStopping2(_) | ProtoPhase::Stopping(_) | ProtoPhase::Stopping2(_) => {
                // Here we still don't know what is the target of the view change.
                return SyncReconfigurationResult::OnGoingViewChange;
            }
            _ => {
                info!("{:?} // Attempted to add node {:?} quorum but we are currently performing a view change", node.id(), joining_node);

                return SyncReconfigurationResult::OnGoingViewChange;
            }
        }

        if joining_node == node.id() {
            unreachable!("We should never try to add ourselves to the quorum this way, there is a specific function for that")
        } else {
            self.begin_quorum_view_change(Some(joining_node), node, timeouts, log);
        }

        SyncReconfigurationResult::InProgress
    }

    /// Prepare ourselves for the quorum join procedure by stopping the current view and starting a new one
    pub fn attempt_join_quorum<NT>(
        &self,
        node: &NT,
        _timeouts: &TimeoutModHandle,
    ) -> ReconfigurationAttemptResult
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        let current_view = self.view();

        info!("{:?} // Attempting to join quorum, skipping stop phase as we are not yet part of the quorum", node.id());

        if current_view.quorum_members().contains(&node.id()) {
            //We are already a part of the quorum, so we don't need to do anything
            info!("{:?} // Attempted to join quorum, but we are already a part of it, can we process sync messages {:?}", node.id(), self.can_process_sync());

            return ReconfigurationAttemptResult::AlreadyPartOfQuorum;
        } else if let Some(next_view) = self.next_view() {
            if next_view.quorum_members().contains(&node.id()) {
                //We are already a part of the quorum, so we don't need to do anything
                info!("{:?} // Attempted to join quorum, but we are already a part of the next view, can we process sync messages {:?}", node.id(), self.can_process_sync());

                return ReconfigurationAttemptResult::AlreadyPartOfQuorum;
            }
        }

        //TODO: Timeout waiting for the sync/stopping data. This is because
        // We actually might try to enter while the protocol is running a different view change,
        // so the view change to integrate us into the quorum might be delayed

        // Simulate that we were accepted into the quorum
        let view = current_view.next_view_with_new_node(node.id());

        self.entering_quorum.replace(true);
        self.currently_adding_node.replace(Some(self.node_id));

        self.install_next_view(view.clone());

        if view.leader() == node.id() {
            // If we are the leader of the next view, then we should move to the stopping data phase and wait
            // For the rest of the nodes to send us the information
            self.phase.replace(ProtoPhase::StoppingData(0));
        } else {
            self.phase.replace(ProtoPhase::Syncing);
        }

        ReconfigurationAttemptResult::InProgress
    }

    /// Trigger a view change locally
    pub fn begin_quorum_view_change<NT>(
        &self,
        join_cert: Option<NodeId>,
        node: &NT,
        timeouts: &TimeoutModHandle,
        _log: &Log<RQ>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        debug!(
            "Beginning quorum view change with certificate {} at phase {:?}",
            join_cert.is_some(),
            self.phase.get()
        );

        match (self.phase.get(), &join_cert) {
            (ProtoPhase::ViewStopping(i), None) => {
                // We have not received a join certificate message from the node, so we still will
                self.phase.replace(ProtoPhase::ViewStopping(i + 1));
            }
            (ProtoPhase::ViewStopping(i), _) => {
                // We have ourselves received a join certificate message from the node, so we will now
                // Have to broadcast a STOP Quorum Join message. As such, we don't want to increment
                // The amount of messages received (since we have not actually received any messages)
                self.phase.replace(ProtoPhase::ViewStopping2(i));
            }
            (ProtoPhase::StoppingData(_), _)
            | (ProtoPhase::Syncing, _)
            | (ProtoPhase::ViewStopping2(_), _) => {
                // we have already started a view change protocol
                return;
            }
            _ => {
                // If we are called when in the init phase, we should clear the state as a new
                // View change is going to start.
                self.stopped.borrow_mut().clear();
                self.collects.lock().unwrap().clear();
                self.currently_adding_node.replace(None);
                self.currently_adding.borrow_mut().clear();

                self.phase.replace(ProtoPhase::ViewStopping2(0));
            }
        }

        match &self.accessory {
            SynchronizerAccessory::Follower(_) => {}
            SynchronizerAccessory::Replica(replica) => {
                if let Some(join_cert) = join_cert {
                    // We only want to send our STOP message when we have received the notification
                    // From the reconfiguration protocol, even if there are already f+1 STOP messages
                    replica.handle_begin_quorum_view_change(self, timeouts, node, join_cert)
                }
            }
        }
    }

    /// Trigger a view change locally.
    ///
    /// The value `timed_out` corresponds to a list of client requests
    /// that have timed out on the current replica.
    /// If the timed out requests are None, that means that the view change
    /// originated in the other replicas.
    pub fn begin_view_change<NT>(
        &self,
        timed_out: Option<Vec<StoredMessage<RQ>>>,
        node: &NT,
        timeouts: &TimeoutModHandle,
        _log: &Log<RQ>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        match (self.phase.get(), &timed_out) {
            // we have received STOP messages from peer nodes,
            // but haven't sent our own STOP, yet; (And in the case of followers we will never send it)
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => {
                // We update here to Stopping2 as we will send our own message right after this
                self.phase.replace(ProtoPhase::Stopping2(i + 1));
            }
            //When the timeout is not null, this means it was called from timed out client requests
            //And therefore we don't increase the received message count, just update the phase to Stopping2
            (ProtoPhase::Stopping(i), _) => {
                // We have begun our own stopping protocol, so we will update our phase to Stopping2
                self.phase.replace(ProtoPhase::Stopping2(i));
            }
            (ProtoPhase::ViewStopping(_), None) | (ProtoPhase::ViewStopping2(_), None) => {
                // We are currently in the quorum alteration view change protocol. Since we have received
                // A timeout and we know that timeouts take precedence over quorum alteration, we will
                // Stop this current view stopping protocol and start the normal view change protocol
                self.phase.replace(ProtoPhase::Stopping(1));
            }
            (ProtoPhase::ViewStopping(_), _) | (ProtoPhase::ViewStopping2(_), _) => {
                // We are currently in the quorum alteration view change protocol. Since we have received
                // A timeout and we know that timeouts take precedence over quorum alteration, we will
                // Stop this current view stopping protocol and start the normal view change protocol
                self.phase.replace(ProtoPhase::Stopping2(0));
            }
            (ProtoPhase::StoppingData(_), _)
            | (ProtoPhase::Syncing, _)
            | (ProtoPhase::Stopping2(_), _) => {
                // we have already started a view change protocol or we have already sent our STOP message
                return;
            }
            // we have timed out, therefore we should send a STOP msg;
            //
            // note that we might have already been running the view change proto,
            // and started another view because we timed out again (e.g. because of
            // a faulty leader during the view change)
            _ => {
                // clear state from previous views
                self.stopped.borrow_mut().clear();
                self.collects.lock().unwrap().clear();
                self.currently_adding_node.replace(None);
                self.currently_adding.borrow_mut().clear();
                self.entering_quorum.replace(false);

                //Set the new state to be stopping
                self.phase.replace(ProtoPhase::Stopping2(0));
            }
        };

        match &self.accessory {
            SynchronizerAccessory::Follower(_) => {}
            SynchronizerAccessory::Replica(rep) => {
                rep.handle_begin_view_change(self, timeouts, node, timed_out)
            }
        }
    }

    // this function mostly serves the purpose of consuming
    // values with immutable references, to allow borrowing data mutably
    fn pre_finalize(
        &self,
        state: FinalizeState<RQ>,
        proof: Option<&Proof<RQ>>,
        _normalized_collects: Vec<Option<&CollectData<RQ>>>,
        log: &Log<RQ>,
    ) -> FinalizeStatus<RQ> {
        let last_executed_cid = proof
            .as_ref()
            .map(|p| p.sequence_number())
            .unwrap_or(SeqNo::ZERO);

        //If we are more than one operation behind the most recent consensus id,
        //Then we must run a consensus state transfer
        if u32::from(log.decision_log().last_execution().unwrap_or(SeqNo::ZERO)) + 1
            < u32::from(last_executed_cid)
        {
            return FinalizeStatus::RunCst(state);
        }

        let rqs = match state.proposed.consensus().kind() {
            ConsensusMessageKind::PrePrepare(rqs) => rqs,
            _ => {
                panic!("Can only have pre prepare messages");
            }
        };

        if rqs.is_empty() && !state.sound.test() {
            return FinalizeStatus::NoValue;
        }

        FinalizeStatus::Commit(state)
    }

    /// Finalize a view change and install the new view in the other
    /// state machines (Consensus)
    fn finalize<NT>(
        &self,
        state: FinalizeState<RQ>,
        log: &mut Log<RQ>,
        timeouts: &TimeoutModHandle,
        consensus: &mut Consensus<RQ>,
        node: &Arc<NT>,
    ) -> SynchronizerStatus<RQ>
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        let FinalizeState {
            curr_cid,
            proposed,
            sound: _,
            last_proof,
        } = state;

        // Finalize the view change by advancing our tbo queue to the new view
        self.advance_view();

        let view = self.view();

        warn!(
            "{:?} // Finalizing view change to view {:?} and consensus ID {:?}, Adding node? {:?}",
            node.id(),
            view,
            curr_cid,
            self.currently_adding_node.get()
        );

        let (header, message) = proposed.into_inner();

        let last_executed_cid = last_proof
            .as_ref()
            .map(|p| p.sequence_number())
            .unwrap_or(SeqNo::ZERO);

        //TODO: Install the Last CID that was received in the finalize state
        let to_execute = if u32::from(log.decision_log().last_execution().unwrap_or(SeqNo::ZERO))
            + 1
            == u32::from(last_executed_cid)
        {
            warn!("{:?} // Received more recent consensus ID, making quorum aware of it {:?} vs {:?} (Ours)", node.id(),
            curr_cid, log.decision_log().last_execution());

            // We are missing the last decision, which should be included in the collect data
            // sent by the leader in the SYNC message

            if let Some(last_proof) = last_proof {
                let quorum_result = consensus
                    .catch_up_to_quorum(&view, last_proof, log)
                    .expect("Failed to catch up to quorum");

                Some(quorum_result)
            } else {
                // This maybe happens when a checkpoint is done and the first execution after it
                // fails, leading to a view change? Don't really know how this would be possible
                // FIXME:
                None
            }
        } else {
            None
        };

        // finalize view change by broadcasting a PREPARE msg
        let consensus_result = consensus
            .finalize_view_change((header, message), &view, self, timeouts, log, node)
            .expect("Failed to finalize view change in consensus");

        // Update proto phase
        self.phase.replace(ProtoPhase::Init);

        if self.currently_adding_node.get().is_some() {
            let node = self.currently_adding_node.replace(None);

            self.currently_adding.borrow_mut().clear();

            SynchronizerStatus::NewViewJoinedQuorum(consensus_result, to_execute, node.unwrap())
        } else {
            // resume normal phase
            SynchronizerStatus::NewView(consensus_result, to_execute)
        }
    }

    /// Handle a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn request_batch_received(
        &self,
        header: &Header,
        pre_prepare: &ConsensusMessage<RQ>,
        timeouts: &TimeoutModHandle,
    ) -> Vec<ClientRqInfo> {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) => {
                rep.received_request_batch(header, pre_prepare, timeouts)
            }
            SynchronizerAccessory::Follower(fol) => fol.watch_request_batch(pre_prepare),
        }
    }

    /// Watch requests that have been received from other replicas
    ///
    pub fn watch_received_requests(&self, digest: Vec<ClientRqInfo>, timeouts: &TimeoutModHandle) {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) => {
                rep.watch_received_requests(digest, timeouts);
            }
            SynchronizerAccessory::Follower(_) => {}
        }
    }

    /// Forward the requests that have timed out to the whole network
    /// So that everyone knows about (including a leader that could still be correct, but
    /// Has not received the requests from the client)
    pub fn forward_requests<NT>(&self, timed_out: Vec<StoredMessage<RQ>>, node: &NT)
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        match &self.accessory {
            SynchronizerAccessory::Follower(_) => {}
            SynchronizerAccessory::Replica(rep) => {
                rep.forward_requests(self, timed_out, node);
            }
        }
    }

    /// Client requests have timed out. We must now send a stop message containing all of the
    /// Requests that have timed out
    pub fn client_requests_timed_out(
        &self,
        my_id: NodeId,
        seq: &Vec<ModTimeout>,
    ) -> SynchronizerStatus<RQ> {
        match &self.accessory {
            SynchronizerAccessory::Follower(_) => SynchronizerStatus::Nil,
            SynchronizerAccessory::Replica(rep) => rep.client_requests_timed_out(self, my_id, seq),
        }
    }

    // collects whose in execution cid is different from the given `in_exec` become `None`
    // A set of collects is considered normalized if or when
    // all collects are related to the same CID. This is important because not all replicas
    // may be executing the same CID when there is a leader change
    #[inline]
    fn normalized_collects(
        collects: &IntMap<u64, StoredMessage<PBFTMessage<RQ>>>,
        in_exec: SeqNo,
    ) -> impl Iterator<Item = Option<&'_ CollectData<RQ>>> {
        let values = collects.values();

        let collects = normalized_collects(in_exec, collect_data(values));

        collects
    }

    // TODO: quorum sizes may differ when we implement reconfiguration
    #[inline]
    fn highest_proof<'a, NT>(
        guard: &'a IntMap<u64, StoredMessage<PBFTMessage<RQ>>>,
        view: &ViewInfo,
        node: &NT,
    ) -> Option<&'a Proof<RQ>>
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        highest_proof::<RQ, _, _>(view, node, guard.values())
    }
}

///The accessory services that complement the base follower state machine
/// This allows us to maximize code re usage and therefore reduce the amount of failure places
pub enum SynchronizerAccessory<RQ: SerMsg> {
    Follower(FollowerSynchronizer<RQ>),
    Replica(ReplicaSynchronizer<RQ>),
}

////////////////////////////////////////////////////////////////////////////////
//
// NOTE: the predicates below were taken from
// Cachin's 'Yet Another Visit to Paxos' (April 2011), pages 10-11
//
// in this consensus algorithm, the WRITE phase is equivalent to the
// PREPARE phase of PBFT, so we will observe a mismatch of terminology
//
// in the arguments of the predicates below, 'ts' means 'timestamp',
// and it is equivalent to the sequence number of a view
//
////////////////////////////////////////////////////////////////////////////////

fn sound<O>(curr_view: &ViewInfo, normalized_collects: &[Option<&CollectData<O>>]) -> Sound {
    // collect timestamps and values
    let mut seq_numbers = collections::hash_set();
    let mut values = collections::hash_set();

    debug!(
        "Checking soundness of view {:?} with collects: {:?}",
        curr_view, normalized_collects
    );

    for maybe_collect in normalized_collects.iter() {
        // NOTE: BFT-SMaRt assumes normalized values start on view 0,
        // if their CID is different from the one in execution;
        // see `LCManager::normalizeCollects` on its code
        let c = match maybe_collect {
            Some(c) => c,
            None => {
                debug!("Found no collect data.");

                seq_numbers.insert(SeqNo::ZERO);
                continue;
            }
        };

        // add quorum write sequence numers
        seq_numbers.insert(
            c.incomplete_proof()
                .quorum_prepares()
                .map(|ViewDecisionPair(ts, _)| *ts)
                .unwrap_or(SeqNo::ZERO),
        );

        // add writeset timestamps and values
        for ViewDecisionPair(seq_no, value) in c.incomplete_proof().write_set().iter() {
            seq_numbers.insert(*seq_no);
            values.insert(*value);
        }
    }

    debug!(
        "View change sound final sequence numbers: {:?}",
        seq_numbers
    );
    debug!("View change sound final values: {:?}", values);

    for seq_no in seq_numbers {
        for value in values.iter() {
            if binds(curr_view, seq_no, value, normalized_collects) {
                return Sound::Bound(*value);
            } else {
                debug!("Failed to bind seq no {:?} and value {:?}.", seq_no, value);
            }
        }
    }

    Sound::Unbound(unbound(curr_view, normalized_collects))
}

fn binds<O>(
    curr_view: &ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        debug!(
            "Not enough collects to bind. Need {:?}, have {:?}.",
            curr_view.params().quorum(),
            normalized_collects.len()
        );

        false
    } else {
        let quorum_highest = quorum_highest(curr_view, ts, value, normalized_collects);

        let certified_value = certified_value(curr_view, ts, value, normalized_collects);

        quorum_highest && certified_value
    }
}

fn unbound<O>(curr_view: &ViewInfo, normalized_collects: &[Option<&CollectData<O>>]) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        debug!(
            "Not enough collects to unbound. Need {:?}, have {:?}.",
            curr_view.params().quorum(),
            normalized_collects.len()
        );

        false
    } else {
        let count = normalized_collects
            .iter()
            .filter(move |maybe_collect| {
                maybe_collect
                    .map(|collect| {
                        collect
                            .incomplete_proof()
                            .quorum_prepares()
                            .map(|ViewDecisionPair(other_ts, _)| *other_ts == SeqNo::ZERO)
                            // when there is no quorum write, BFT-SMaRt
                            // assumes replicas are on view 0
                            .unwrap_or(true)
                    })
                    // check NOTE above on the `sound` predicate
                    .unwrap_or(true)
            })
            .count();

        debug!(
            "Unbound count: {:?} for collect data: {:?}.",
            count, normalized_collects
        );

        count >= curr_view.params().quorum()
    }
}

// NOTE: `filter_map` on the predicates below filters out
// collects whose cid was different from the one in execution;
//
// in BFT-SMaRt's code, a `TimestampValuePair` is generated in
// `LCManager::normalizeCollects`, containing an empty (zero sized
// byte array) digest, which will always evaluate to false when
// comparing its equality to other digests from collects whose
// cid is the same as the one in execution;
//
// therefore, our code *should* be correct :)
fn quorum_highest<O>(
    curr_view: &ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    let appears = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .any(|collect| {
            collect
                .incomplete_proof()
                .quorum_prepares()
                .map(|ViewDecisionPair(other_ts, other_value)| {
                    *other_ts == ts && other_value == value
                })
                .unwrap_or(false)
        });

    let count = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .filter(move |collect| {
            collect
                .incomplete_proof()
                .quorum_prepares()
                .map(
                    |ViewDecisionPair(other_ts, other_value)| match other_ts.cmp(&ts) {
                        Ordering::Less => true,
                        Ordering::Equal if other_value == value => true,
                        _ => false,
                    },
                )
                .unwrap_or(false)
        })
        .count();

    debug!(
        "Quorum highest: {:?} appears? {} {:?} times.",
        value, appears, count
    );

    appears && count >= curr_view.params().quorum()
}

fn certified_value<O>(
    curr_view: &ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    let count: usize = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .map(move |collect| {
            collect
                .incomplete_proof()
                .write_set()
                .iter()
                .filter(|ViewDecisionPair(other_ts, other_value)| {
                    *other_ts >= ts && other_value == value
                })
                .count()
        })
        .sum();

    debug!("Certified value: {:?} appears {:?} times.", value, count);

    count > curr_view.params().f()
}

fn collect_data<'a, O: 'a>(
    collects: impl Iterator<Item = &'a StoredMessage<PBFTMessage<O>>>,
) -> impl Iterator<Item = &'a CollectData<O>> {
    collects.filter_map(|stored| match stored.message().view_change().kind() {
        ViewChangeMessageKind::StopData(collects) => Some(collects),
        _ => None,
    })
}

fn normalized_collects<'a, O: 'a>(
    in_exec: SeqNo,
    collects: impl Iterator<Item = &'a CollectData<O>>,
) -> impl Iterator<Item = Option<&'a CollectData<O>>> {
    collects.map(move |collect| {
        if collect.incomplete_proof().executing() == in_exec {
            Some(collect)
        } else {
            None
        }
    })
}

fn signed_collects<RQ, NT>(
    node: &NT,
    collects: Vec<StoredMessage<PBFTMessage<RQ>>>,
) -> Vec<StoredMessage<PBFTMessage<RQ>>>
where
    RQ: SerMsg,
    NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    collects
        .into_iter()
        .filter(|stored| validate_signature::<RQ, _, _>(node, stored))
        .collect()
}

fn validate_signature<'a, RQ, M, NT>(node: &'a NT, stored: &'a StoredMessage<M>) -> bool
where
    RQ: SerMsg,
    NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    //TODO: Fix this as I believe it will always be false
    let wm = match WireMessage::from_header(*stored.header(), MessageModule::Protocol) {
        Ok(wm) => wm,
        _ => {
            error!("{:?} // Failed to parse WireMessage", node.id());

            return false;
        }
    };

    // check if we even have the public key of the node that claims
    // to have sent this particular message
    let key = match node
        .network_info_provider()
        .get_node_info(&stored.header().from())
    {
        Some(k) => k,
        None => {
            error!(
                "{:?} // Failed to get public key for node {:?}",
                node.id(),
                stored.header().from()
            );

            return false;
        }
    };

    wm.is_valid(Some(key.public_key()), false).is_ok()
}

fn highest_proof<'a, RQ, I, NT>(view: &ViewInfo, node: &NT, collects: I) -> Option<&'a Proof<RQ>>
where
    RQ: SerMsg,
    I: Iterator<Item = &'a StoredMessage<PBFTMessage<RQ>>>,
    NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
{
    collect_data(collects)
        // fetch proofs
        .filter_map(|collect| collect.last_proof())
        // check if COMMIT msgs are signed, and all have the same digest
        //
        .filter(move |proof| {
            let digest = proof.batch_digest();

            let commits_valid = proof
                .commits()
                .iter()
                .filter(|stored| {
                    stored
                        .message()
                        .consensus()
                        .has_proposed_digest(&digest)
                        //If he does not have the digest, then it is not valid
                        .unwrap_or(false)
                })
                .filter(move |&stored| validate_signature::<RQ, _, _>(node, stored))
                .count()
                >= view.params().quorum();

            let prepares_valid = proof
                .prepares()
                .iter()
                .filter(|stored| {
                    stored
                        .message()
                        .consensus()
                        .has_proposed_digest(&digest)
                        //If he does not have the digest, then it is not valid
                        .unwrap_or(false)
                })
                .filter(move |&stored| validate_signature::<RQ, _, _>(node, stored))
                .count()
                >= view.params().quorum();

            debug!(
                "{:?} // Proof {:?} is valid? commits valid: {:?} &&  prepares_valid: {:?}",
                node.id(),
                proof,
                commits_valid,
                prepares_valid
            );

            commits_valid && prepares_valid
        })
        .max_by_key(|proof| proof.sequence_number())
}

impl<O> Debug for SynchronizerPollStatus<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SynchronizerPollStatus::Recv => {
                write!(f, "SynchronizerPollStatus::Recv")
            }
            SynchronizerPollStatus::NextMessage(message) => {
                write!(
                    f,
                    "SynchronizerPollStatus::NextMessage Header {:?}, Message {:?}",
                    message.header(),
                    message.message().view_change()
                )
            }
            SynchronizerPollStatus::ResumeViewChange => {
                write!(f, "SynchronizerPollStatus::ResumeViewChange")
            }
        }
    }
}
