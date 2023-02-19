use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use std::sync::MutexGuard;
use bytes::BytesMut;

use self::{follower_sync::FollowerSynchronizer, replica_sync::ReplicaSynchronizer};

use super::{
    collections,
    communication::{
        message::{
            ConsensusMessage, ConsensusMessageKind, ForwardedRequestsMessage, FwdConsensusMessage,
            Header, RequestMessage, StoredMessage, SystemMessage, ViewChangeMessage,
            ViewChangeMessageKind, WireMessage,
        },
        serialize::{Buf, DigestData},
        Node, NodeId,
    },
    consensus::{
        Consensus,
    },
    crypto::hash::Digest,
    executable::{Reply, Request, Service, State},
    globals::ReadOnly,
    ordering::{tbo_advance_message_queue, tbo_pop_message, tbo_queue_message, Orderable, SeqNo},
    prng,
};

use intmap::IntMap;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::decisions::{CollectData, Proof, ViewDecisionPair};
use crate::bft::msg_log::pending_decision::PendingRequestLog;

use crate::bft::sync::view::ViewInfo;
use crate::bft::timeouts::{ClientRqInfo, Timeouts};


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
        if let Some(stored) = tbo_pop_message::<ViewChangeMessage<$t>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            SynchronizerPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            SynchronizerPollStatus::Recv
        }
    };
}

macro_rules! stop_status {
    ($self:expr, $i:expr) => {{
        let f = $self.view().params().f();
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
#[derive(Clone)]
pub struct LeaderCollects<O> {
    //The pre prepare message, created and signed by the leader to be executed when the view change is
    // Done
    proposed: FwdConsensusMessage<O>,
    // The collect messages the leader has received.
    collects: Vec<StoredMessage<ViewChangeMessage<O>>>,
}

impl<O> LeaderCollects<O> {
    /// Gives up ownership of the inner values of this `LeaderCollects`.
    pub fn into_inner(
        self,
    ) -> (
        FwdConsensusMessage<O>,
        Vec<StoredMessage<ViewChangeMessage<O>>>,
    ) {
        (self.proposed, self.collects)
    }
}

pub(super) struct FinalizeState<O> {
    curr_cid: SeqNo,
    sound: Sound,
    proposed: FwdConsensusMessage<O>,
    last_proof: Option<Proof<O>>
}

pub(super) enum FinalizeStatus<O> {
    NoValue,
    RunCst(FinalizeState<O>),
    Commit(FinalizeState<O>),
}

///
pub(self) enum Sound {
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
    // Stores the previous view, for useful information when changing views
    previous_view: Option<ViewInfo>,
    // probe messages from this queue instead of
    // fetching them from the network
    get_queue: bool,
    // stores all STOP messages for the next view
    stop: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
    // stores all STOP-DATA messages for the next view
    stop_data: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
    // stores all SYNC messages for the next view
    sync: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
}

impl<O> TboQueue<O> {
    fn new(view: ViewInfo) -> Self {
        Self {
            view,
            previous_view: None,
            get_queue: false,
            stop: VecDeque::new(),
            stop_data: VecDeque::new(),
            sync: VecDeque::new(),
        }
    }


    /// Installs a new view into the queue.
    pub fn install_view(&mut self, view: ViewInfo) {
        let prev_view = std::mem::replace(&mut self.view, view);

        self.previous_view = Some(prev_view);

        //TODO: should we move to the next instance queue
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
    pub fn queue(&mut self, h: Header, m: ViewChangeMessage<O>) {
        match m.kind() {
            ViewChangeMessageKind::Stop(_) => self.queue_stop(h, m),
            ViewChangeMessageKind::StopData(_) => self.queue_stop_data(h, m),
            ViewChangeMessageKind::Sync(_) => self.queue_sync(h, m),
        }
    }

    /// Verifies if we have new `STOP` messages to be processed for
    /// the current view.
    pub fn can_process_stops(&self) -> bool {
        self.stop
            .get(0)
            .map(|deque| deque.len() > 0)
            .unwrap_or(false)
    }

    /// Queues a `STOP` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop(&mut self, h: Header, m: ViewChangeMessage<O>) {
        // NOTE: we use next() because we want to retrieve messages
        // for v+1, as we haven't started installing the new view yet
        let seq = self.view.sequence_number().next();
        tbo_queue_message(seq, &mut self.stop, StoredMessage::new(h, m))
    }

    /// Queues a `STOP-DATA` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop_data(&mut self, h: Header, m: ViewChangeMessage<O>) {
        let seq = self.view.sequence_number();
        tbo_queue_message(seq, &mut self.stop_data, StoredMessage::new(h, m))
    }

    /// Queues a `SYNC` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_sync(&mut self, h: Header, m: ViewChangeMessage<O>) {
        let seq = self.view.sequence_number();
        tbo_queue_message(seq, &mut self.sync, StoredMessage::new(h, m))
    }

    pub fn view(&self) -> &ViewInfo {
        &self.view
    }

    pub fn previous_view(&self) -> &Option<ViewInfo> {
        &self.previous_view
    }
}

#[derive(Copy, Clone)]
pub(super) enum TimeoutPhase {
    // we have never received a timeout
    Init(Instant),
    // we received a second timeout for the same request;
    // start view change protocol
    TimedOutOnce(Instant),
    // keep requests that timed out stored in memory,
    // for efficiency
    TimedOut,
}

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
pub enum SynchronizerStatus {
    /// We are not running the view change protocol.
    Nil,
    /// The view change protocol is currently running.
    Running,
    /// The view change protocol just finished running.
    NewView,
    /// Before we finish the view change protocol, we need
    /// to run the CST protocol.
    RunCst,
    /// The following set of client requests timed out.
    ///
    /// We need to invoke the leader change protocol if
    /// we have a non empty set of stopped messages.
    RequestsTimedOut {
        forwarded: Vec<Digest>,
        stopped: Vec<Digest>,
    },
}

/// Represents the status of calling `poll()` on a `Synchronizer`.
pub enum SynchronizerPollStatus<O> {
    /// The `Replica` associated with this `Synchronizer` should
    /// poll its main channel for more messages.
    Recv,
    /// A new view change message is available to be processed.
    NextMessage(Header, ViewChangeMessage<O>),
    /// We need to resume the view change protocol, after
    /// running the CST protocol.
    ResumeViewChange,
}

///A trait describing some of the necessary methods for the synchronizer
pub trait AbstractSynchronizer<S: Service + 'static> {
    /// Returns information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo;

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn install_view(&self, view: ViewInfo);

    fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>);
}

type CollectsType<S> = IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>;

///The synchronizer for the SMR protocol
/// This part of the protocol is responsible for handling the changing of views and
/// for keeping track of any timed out client requests
pub struct Synchronizer<S: Service> {
    phase: RefCell<ProtoPhase>,
    //Tbo queue, keeps track of the current view and keeps messages arriving in order
    tbo: Mutex<TboQueue<Request<S>>>,
    //Stores currently received requests from other nodes
    stopped: RefCell<IntMap<Vec<StoredMessage<RequestMessage<Request<S>>>>>>,
    //TODO: This does not require a Mutex I believe since it's only accessed when
    // Processing messages (which is always done in the replica thread)
    collects: Mutex<CollectsType<S>>,
    // Used to store the finalize state when we are forced to run the CST protocol
    finalize_state: RefCell<Option<FinalizeState<Request<S>>>>,
    accessory: SynchronizerAccessory<S>,
}

///Justification/Sort of correction proof:
/// In general, all fields and methods will be accessed by the replica thread, never by the client rq thread.
/// Therefore, we only have to protect the fields that will be accessed by both clients and replicas.
/// So we protect collects, watching and tbo as those are the fields that are going to be
/// accessed by both those threads.
/// Since the other fields are going to be accessed by just 1 thread, we just need them to be Send, which they are
unsafe impl<S: Service> Sync for Synchronizer<S> {}

impl<S: Service + 'static> AbstractSynchronizer<S> for Synchronizer<S> {
    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo {
        self.tbo.lock().unwrap().view().clone()
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn install_view(&self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        self.phase.replace(ProtoPhase::Init);
        let mut guard = self.tbo.lock().unwrap();

        guard.install_view(view);
    }

    fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>) {
        self.tbo.lock().unwrap().queue(header, message)
    }
}

impl<S> Synchronizer<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    pub fn new_follower(view: ViewInfo) -> Arc<Self> {
        Arc::new(Self {
            phase: RefCell::new(ProtoPhase::Init),
            stopped: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
            accessory: SynchronizerAccessory::Follower(FollowerSynchronizer::new()),
        })
    }

    pub fn new_replica(view: ViewInfo, timeout_dur: Duration) -> Arc<Self> {
        Arc::new(Self {
            phase: RefCell::new(ProtoPhase::Init),
            stopped: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
            accessory: SynchronizerAccessory::Replica(ReplicaSynchronizer::new(timeout_dur)),
        })
    }

    fn previous_view(&self) -> Option<ViewInfo> { self.tbo.lock().unwrap().previous_view().clone() }

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

    /// Check if we can process new view change messages.
    /// If there are pending messages that are now processable (but weren't when we received them)
    /// We return them. If there are no pending messages then we will wait for new messages from other replicas
    pub fn poll(&self) -> SynchronizerPollStatus<Request<S>> {
        let mut tbo_guard = self.tbo.lock().unwrap();
        match *self.phase.borrow() {
            _ if !tbo_guard.get_queue => SynchronizerPollStatus::Recv,
            ProtoPhase::Init => {
                //If we are
                extract_msg!(Request<S> =>
                    { self.phase.replace(ProtoPhase::Stopping(0)); },
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                )
            }
            ProtoPhase::Stopping(_) | ProtoPhase::Stopping2(_) => {
                extract_msg!(Request<S> =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                )
            }
            ProtoPhase::StoppingData(_) => {
                extract_msg!(Request<S> =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop_data
                )
            }
            ProtoPhase::Syncing => {
                extract_msg!(Request<S> =>
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
    pub fn process_message(
        &self,
        header: Header,
        message: ViewChangeMessage<Request<S>>,
        timeouts: &Timeouts,
        log: &mut DecidedLog<S>,
        pending_rq_log: &PendingRequestLog<S>,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> SynchronizerStatus
    {
        match *self.phase.borrow() {
            ProtoPhase::Init => {
                return match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

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

                                guard.queue_stop_data(header, message);

                                SynchronizerStatus::Nil
                            }
                        }
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        SynchronizerStatus::Nil
                    }
                };
            }
            ProtoPhase::Stopping(i) | ProtoPhase::Stopping2(i) => {
                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let next_seq = current_view.sequence_number().next();

                let i = match message.kind() {
                    ViewChangeMessageKind::Stop(_) if msg_seq != next_seq => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return stop_status!(self, i);
                    }
                    ViewChangeMessageKind::Stop(_)
                    if self.stopped.borrow().contains_key(header.from().into()) =>
                        {
                            // drop attempts to vote twice
                            return stop_status!(self, i);
                        }
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                return stop_status!(self, i);
                            }
                            SynchronizerAccessory::Replica(_) => {
                                let mut guard = self.tbo.lock().unwrap();

                                guard.queue_stop_data(header, message);

                                return stop_status!(self, i);
                            }
                        }
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return stop_status!(self, i);
                    }
                };

                // store pending requests from this STOP
                let stopped = match message.into_kind() {
                    ViewChangeMessageKind::Stop(stopped) => stopped,
                    _ => unreachable!(),
                };

                self.stopped
                    .borrow_mut()
                    .insert(header.from().into(), stopped);

                // NOTE: we only take this branch of the code before
                // we have sent our own STOP message
                if let ProtoPhase::Stopping(_i) = *self.phase.borrow() {
                    return if i > current_view.params().f() {
                        self.begin_view_change(None, node, timeouts, log);
                        SynchronizerStatus::Running
                    } else {
                        SynchronizerStatus::Nil
                    };
                }

                if i == current_view.params().quorum() {
                    let previous_view = current_view.clone();

                    //We have received the necessary amount of stopping requests
                    //To now that we should move to the next view
                    self.install_view(current_view.next_view());

                    match &self.accessory {
                        SynchronizerAccessory::Replica(rep) => {
                            rep.handle_stopping_quorum(self, previous_view, log,
                                                       pending_rq_log, timeouts, node)
                        }
                        SynchronizerAccessory::Follower(_) => {}
                    }

                    if current_view.leader() == node.id() {
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
            ProtoPhase::StoppingData(i) => {
                match &self.accessory {
                    SynchronizerAccessory::Follower(_) => {
                        //Since a follower can never be a leader (as he isn't a part of the
                        // quorum, he can never be in this state)
                        unreachable!()
                    }
                    SynchronizerAccessory::Replica(_rep) => {
                        //Obtain the view seq no of the message
                        let msg_seq = message.sequence_number();

                        let current_view = self.view();
                        let seq = current_view.sequence_number();

                        // reject STOP-DATA messages if we are not the leader
                        let mut collects_guard = self.collects.lock().unwrap();

                        let i = match message.kind() {
                            ViewChangeMessageKind::Stop(_) => {
                                let mut guard = self.tbo.lock().unwrap();

                                guard.queue_stop(header, message);

                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_) if msg_seq != seq => {
                                if current_view.peek(msg_seq).leader() == node.id() {
                                    //If we are the leader of the view the message is in,
                                    //Then we want to accept the message, but since it is not the current
                                    //View, then it cannot be processed atm
                                    let mut guard = self.tbo.lock().unwrap();

                                    guard.queue_stop_data(header, message);
                                }

                                return SynchronizerStatus::Running;
                            }
                            ViewChangeMessageKind::StopData(_)
                            if current_view.leader() != node.id() =>
                                {
                                    //If we are not the leader, ignore
                                    return SynchronizerStatus::Running;
                                }
                            ViewChangeMessageKind::StopData(_)
                            if collects_guard.contains_key(header.from().into()) =>
                                {
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
                                let mut guard = self.tbo.lock().unwrap();
                                //Since we are the current leader and are waiting for stop data,
                                //This must be related to another view.
                                guard.queue_sync(header, message);

                                return SynchronizerStatus::Running;
                            }
                        };

                        // NOTE: the STOP-DATA message signatures are already
                        // verified by the TLS layer, but we still need to
                        // verify their content when we retransmit the COLLECT's
                        // to other nodes via a SYNC message! this guarantees
                        // the new leader isn't forging messages.

                        // store collects from this STOP-DATA
                        collects_guard
                            .insert(header.from().into(), StoredMessage::new(header, message));

                        if i != current_view.params().quorum() {
                            return SynchronizerStatus::Running;
                        } else {

                            // NOTE:
                            // - fetch highest CID from consensus proofs
                            // - broadcast SYNC msg with collected
                            //   STOP-DATA proofs so other replicas
                            //   can repeat the leader's computation

                            let previous_view = self.previous_view();

                            //Since all of these requests were done in the previous view of the algorithm
                            // then we should also use the previous view to verify the validity of them
                            let previous_view_ref = previous_view.as_ref().unwrap_or(&current_view);

                            let proof = Self::highest_proof(&*collects_guard,
                                                            previous_view_ref, node);

                            let curr_cid = proof
                                .map(|p| p.sequence_number())
                                .map(|seq| SeqNo::from(u32::from(seq) + 1))
                                .unwrap_or(SeqNo::ZERO);

                            //Here we use the normalized_collects method, which uses data from self.collects
                            //Which is protected by a mutex. Therefore, we must carry the consensus guard along
                            //While we access the normalized collects to prevent any errors.
                            let normalized_collects: Vec<Option<&CollectData<Request<S>>>> =
                                Self::normalized_collects(&*collects_guard, curr_cid).collect();

                            let sound = sound(&current_view, &normalized_collects);

                            if !sound.test() {
                                //FIXME: BFT-SMaRt doesn't do anything if `sound`
                                // evaluates to false; do we keep the same behavior,
                                // and wait for a new time out? but then, no other
                                // consensus messages have been processed... this
                                // may be a point of contention on the lib!
                                collects_guard.clear();

                                return SynchronizerStatus::Running;
                            }

                            let p = pending_rq_log.view_change_propose();

                            let node_sign = node.sign_detached();

                            //We create the pre-prepare here as we are the new leader,
                            //And we sign it right now
                            let (header, message) = {
                                let mut buf = Vec::new();

                                let forged_pre_prepare = consensus.forge_propose(p.clone(), self);

                                let digest = <S::Data as DigestData>::serialize_digest(
                                    &forged_pre_prepare,
                                    &mut buf,
                                ).unwrap();

                                let buf = Buf::from(buf);

                                let mut prng_state = prng::State::new();

                                //Create the pre-prepare message that contains the requests
                                //Collected during the STOPPING DATA phase
                                let (h, _) = WireMessage::new(
                                    self.view().leader(),
                                    node.id(),
                                    buf,
                                    prng_state.next_state(),
                                    Some(digest),
                                    Some(node_sign.key_pair()),
                                ).into_inner();

                                if let SystemMessage::Consensus(consensus) = forged_pre_prepare {
                                    (h, consensus)
                                } else {
                                    //This is basically impossible
                                    panic!("Returned random message from forge propose?")
                                }
                            };

                            let fwd_request = FwdConsensusMessage::new(header, message);

                            let collects = collects_guard.values()
                                .cloned().collect();

                            let message = SystemMessage::ViewChange(ViewChangeMessage::new(
                                current_view.sequence_number(),
                                ViewChangeMessageKind::Sync(LeaderCollects {
                                    proposed: fwd_request.clone(),
                                    collects,
                                }),
                            ));

                            let node_id = node.id();
                            let targets = NodeId::targets(0..current_view.params().n())
                                .filter(move |&id| id != node_id);

                            node.broadcast(message, targets);

                            let state = FinalizeState {
                                curr_cid,
                                sound,
                                proposed: fwd_request,
                                last_proof: proof.cloned()
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
                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let seq = current_view.sequence_number();

                // reject SYNC messages if these were not sent by the leader
                let (proposed, collects) = match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) if msg_seq != seq => {
                        todo!()
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        match &self.accessory {
                            SynchronizerAccessory::Follower(_) => {
                                //Ignore stop data messages as followers can never reach this state
                                return SynchronizerStatus::Running;
                            }
                            SynchronizerAccessory::Replica(_) => {
                                let mut guard = self.tbo.lock().unwrap();

                                guard.queue_stop_data(header, message);

                                return SynchronizerStatus::Running;
                            }
                        }
                    }
                    ViewChangeMessageKind::Sync(_) if msg_seq != seq => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) if header.from() != current_view.leader() => {
                        //You're not the leader, what are you saying
                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let message = message;

                        message.take_collects().unwrap().into_inner()
                    }
                };

                // leader has already performed this computation in the
                // STOP-DATA phase of Mod-SMaRt
                let signed: Vec<_> = signed_collects::<S>(node, collects);

                let proof = highest_proof::<S, _>(&current_view, node, signed.iter());

                let curr_cid = proof
                    .map(|p| p.sequence_number())
                    .map(|seq| seq.next())
                    .unwrap_or(SeqNo::ZERO);

                let normalized_collects: Vec<_> =
                    { normalized_collects(curr_cid, collect_data(signed.iter())).collect() };

                let sound = sound(&current_view, &normalized_collects);

                if !sound.test() {
                    //FIXME: BFT-SMaRt doesn't do anything if `sound`
                    // evaluates to false; do we keep the same behavior,
                    // and wait for a new time out? but then, no other
                    // consensus messages have been processed... this
                    // may be a point of contention on the lib!
                    return SynchronizerStatus::Running;
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
    pub fn resume_view_change(
        &self,
        log: &mut DecidedLog<S>,
        timeouts: &Timeouts,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> Option<()>
    {
        let state = self.finalize_state.borrow_mut().take()?;

        //This is kept alive until it is out of the scope
        let _lock_guard = self.collects.lock().unwrap();

        finalize_view_change!(
            self,
            state,
            None,
            Vec::new(),
            log,
            timeouts,
            consensus,
            node,
        );

        Some(())
    }

    /// Trigger a view change locally.
    ///
    /// The value `timed_out` corresponds to a list of client requests
    /// that have timed out on the current replica.
    /// If the timed out requests are None, that means that the view change
    /// originated in the other replicas.
    pub fn begin_view_change(
        &self,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
        node: &Node<S::Data>,
        timeouts: &Timeouts,
        _log: &DecidedLog<S>,
    )
    {
        match (&*self.phase.borrow(), &timed_out) {
            // we have received STOP messages from peer nodes,
            // but haven't sent our own STOP, yet; (And in the case of followers we will never send it)
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => {
                self.phase.replace(ProtoPhase::Stopping2(*i + 1));
            }
            //When the timeout is not null, this means it was called from timed out client requests
            //And therefore we don't increase the received message count, just update the phase to Stopping2
            (ProtoPhase::Stopping(i), _) => {
                self.phase.replace(ProtoPhase::Stopping2(*i));
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
        state: FinalizeState<Request<S>>,
        _proof: Option<&Proof<Request<S>>>,
        _normalized_collects: Vec<Option<&CollectData<Request<S>>>>,
        log: &DecidedLog<S>,
    ) -> FinalizeStatus<Request<S>>
    {
        //If we are more than one operation behind the most recent consensus id,
        //Then we must run a consensus state transfer
        if u32::from(log.decision_log().last_execution().unwrap_or(SeqNo::ZERO)) + 1 < u32::from(state.curr_cid) {
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
    fn finalize(
        &self,
        state: FinalizeState<Request<S>>,
        log: &mut DecidedLog<S>,
        timeouts: &Timeouts,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> SynchronizerStatus
    {
        let FinalizeState {
            curr_cid,
            proposed,
            sound,
            last_proof
        } = state;

        // we will get some value to be proposed because of the
        // check we did in `pre_finalize()`, guarding against no values
        log.clear_last_occurrence(curr_cid);

        let (header, message) = proposed.into_inner();

        //TODO: Install the Last CID that was received in the finalize state
        if u32::from(log.decision_log().last_execution().unwrap_or(SeqNo::ZERO)) + 1 == u32::from(curr_cid) {

            // We are missing the last decision, which should be included in the collect data
            // sent by the leader in the SYNC message
            if let Some(last_proof) = last_proof {

                consensus.catch_up_to_quorum(last_proof.seq_no(), last_proof, log)
                    .expect("Failed to catch up to quorum");


                //TODO: Now we must replay this in the executor.
                // Maybe do a sync write so we can make sure we only execute when it is done

            } else {
                // This maybe happens when a checkpoint is done and the first execution after it
                // fails, leading to a view change? Don't really know how this would be possible
                // FIXME:
            }
        }

        // finalize view change by broadcasting a PREPARE msg
        consensus.finalize_view_change((header, message), self, timeouts, log, node);

        // skip queued messages from the current view change
        // and update proto phase
        self.tbo.lock().unwrap().next_instance_queue();
        self.phase.replace(ProtoPhase::Init);

        // resume normal phase
        SynchronizerStatus::NewView
    }

    /// Handle a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn request_batch_received(
        &self,
        pre_prepare: &StoredMessage<ConsensusMessage<Request<S>>>,
        timeouts: &Timeouts,
    ) -> Vec<Digest>
    {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) => {
                rep.received_request_batch(pre_prepare, timeouts)
            }
            SynchronizerAccessory::Follower(fol) => fol.watch_request_batch(pre_prepare),
        }
    }

    /// Watch requests that have been forwarded to us
    pub fn watch_forwarded_requests(
        &self,
        requests: ForwardedRequestsMessage<Request<S>>,
        timeouts: &Timeouts,
        log: &PendingRequestLog<S>,
    )
    {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) => {
                rep.watch_forwarded_requests(requests, timeouts, log)
            }
            _ => {}
        }
    }

    /// Watch requests that have been received from other replicas
    ///
    pub fn watch_received_requests(&self, digest: Vec<Digest>, timeouts: &Timeouts) {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) => {
                rep.watch_received_requests(digest, timeouts);
            }
            SynchronizerAccessory::Follower(_) => {}
        }
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(&self, digest: Digest, timeouts: &Timeouts) {
        match &self.accessory {
            SynchronizerAccessory::Replica(rep) =>
                rep.watch_request(digest, timeouts),
            _ => {}
        }
    }

    /// Forward the requests that have timed out to the whole network
    /// So that everyone knows about (including a leader that could still be correct, but
    /// Has not received the requests from the client)
    pub fn forward_requests(&self,
                            timed_out: Vec<StoredMessage<RequestMessage<Request<S>>>>,
                            node: &Node<S::Data>,
                            log: &PendingRequestLog<S>) {
        match &self.accessory {
            SynchronizerAccessory::Follower(_) => {}
            SynchronizerAccessory::Replica(rep) => {
                rep.forward_requests(self, timed_out, node, log);
            }
        }
    }

    /// Client requests have timed out. We must now send a stop message containing all of the
    /// Requests that have timed out
    pub fn client_requests_timed_out(
        &self,
        seq: &Vec<ClientRqInfo>,
    ) -> SynchronizerStatus {
        match &self.accessory {
            SynchronizerAccessory::Follower(_) => {
                SynchronizerStatus::Nil
            }
            SynchronizerAccessory::Replica(rep) => {
                rep.client_requests_timed_out(seq)
            }
        }
    }

    // collects whose in execution cid is different from the given `in_exec` become `None`
    // A set of collects is considered normalized if or when
    // all collects are related to the same CID. This is important because not all replicas
    // may be executing the same CID when there is a leader change
    #[inline]
    fn normalized_collects<'a>(
        collects: &'a IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>,
        in_exec: SeqNo,
    ) -> impl Iterator<Item=Option<&'a CollectData<Request<S>>>> {
        let values = collects.values();

        let collects = normalized_collects(in_exec, collect_data(values));

        collects
    }

    // TODO: quorum sizes may differ when we implement reconfiguration
    #[inline]
    fn highest_proof<'a>(
        guard: &'a IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>,
        view: &ViewInfo,
        node: &Node<S::Data>,
    ) -> Option<&'a Proof<Request<S>>> {
        highest_proof::<S, _>(&view, node, guard.values())
    }
}

///The accessory services that complement the base follower state machine
/// This allows us to maximize code re usage and therefore reduce the amount of failure places
pub enum SynchronizerAccessory<S: Service> {
    Follower(FollowerSynchronizer<S>),
    Replica(ReplicaSynchronizer<S>),
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

fn sound<'a, O>(curr_view: &ViewInfo, normalized_collects: &[Option<&'a CollectData<O>>]) -> Sound {
    // collect timestamps and values
    let mut seq_numbers = collections::hash_set();
    let mut values = collections::hash_set();

    for maybe_collect in normalized_collects.iter() {
        // NOTE: BFT-SMaRt assumes normalized values start on view 0,
        // if their CID is different from the one in execution;
        // see `LCManager::normalizeCollects` on its code
        let c = match maybe_collect {
            Some(c) => c,
            None => {
                seq_numbers.insert(SeqNo::ZERO);
                continue;
            }
        };

        // add quorum write sequence numers
        seq_numbers.insert(
            c.incomplete_proof()
                .quorum_writes()
                .map(|ViewDecisionPair(ts, _)| *ts)
                .unwrap_or(SeqNo::ZERO),
        );

        // add writeset timestamps and values
        for ViewDecisionPair(seq_no, value) in c.incomplete_proof().write_set().iter() {
            seq_numbers.insert(*seq_no);
            values.insert(value.clone());
        }
    }

    for seq_no in seq_numbers {
        for value in values.iter() {
            if binds(&curr_view, seq_no, value, normalized_collects) {
                return Sound::Bound(*value);
            }
        }
    }

    Sound::Unbound(unbound(&curr_view, normalized_collects))
}

fn binds<O>(
    curr_view: &ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        false
    } else {
        quorum_highest(curr_view, ts, value, normalized_collects)
            && certified_value(curr_view, ts, value, normalized_collects)
    }
}

fn unbound<O>(curr_view: &ViewInfo, normalized_collects: &[Option<&CollectData<O>>]) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        false
    } else {
        let count = normalized_collects
            .iter()
            .filter(move |maybe_collect| {
                maybe_collect
                    .map(|collect| {
                        collect
                            .incomplete_proof()
                            .quorum_writes()
                            .map(|ViewDecisionPair(other_ts, _)| *other_ts == SeqNo::ZERO)
                            // when there is no quorum write, BFT-SMaRt
                            // assumes replicas are on view 0
                            .unwrap_or(true)
                    })
                    // check NOTE above on the `sound` predicate
                    .unwrap_or(true)
            })
            .count();
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
        .position(|collect| {
            collect
                .incomplete_proof()
                .quorum_writes()
                .map(|ViewDecisionPair(other_ts, other_value)| {
                    *other_ts == ts && other_value == value
                })
                .unwrap_or(false)
        })
        .is_some();

    let count = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .filter(move |collect| {
            collect
                .incomplete_proof()
                .quorum_writes()
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
    count > curr_view.params().f()
}

fn collect_data<'a, O: 'a>(
    collects: impl Iterator<Item=&'a StoredMessage<ViewChangeMessage<O>>>,
) -> impl Iterator<Item=&'a CollectData<O>> {
    collects.filter_map(|stored| match stored.message().kind() {
        ViewChangeMessageKind::StopData(collects) => Some(collects),
        _ => None,
    })
}

fn normalized_collects<'a, O: 'a>(
    in_exec: SeqNo,
    collects: impl Iterator<Item=&'a CollectData<O>>,
) -> impl Iterator<Item=Option<&'a CollectData<O>>> {
    collects.map(move |collect| {
        if collect.incomplete_proof().executing() == in_exec {
            Some(collect)
        } else {
            None
        }
    })
}

fn signed_collects<S>(
    node: &Node<S::Data>,
    collects: Vec<StoredMessage<ViewChangeMessage<Request<S>>>>,
) -> Vec<StoredMessage<ViewChangeMessage<Request<S>>>>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    collects
        .into_iter()
        .filter(|stored| validate_signature::<S, _>(node, stored))
        .collect()
}

fn validate_signature<'a, S, M>(node: &'a Node<S::Data>, stored: &'a StoredMessage<M>) -> bool
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{

    //TODO: Fix this as I believe it will always be false
    let wm = match WireMessage::from_parts(*stored.header(), Buf::new()) {
        Ok(wm) => wm,
        _ => return false,
    };

    // check if we even have the public key of the node that claims
    // to have sent this particular message
    let key = match node.get_public_key(stored.header().from()) {
        Some(k) => k,
        None => return false,
    };

    wm.is_valid(Some(key))
}

fn highest_proof<'a, S, I>(
    view: &ViewInfo,
    node: &Node<S::Data>,
    collects: I,
) -> Option<&'a Proof<Request<S>>>
    where
        I: Iterator<Item=&'a StoredMessage<ViewChangeMessage<Request<S>>>>,
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
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
                        .has_proposed_digest(&digest)
                        //If he does not have the digest, then it is not valid
                        .unwrap_or(false)
                })
                .filter(move |&stored|
                    { validate_signature::<S, _>(node, stored) })
                .count() >= view.params().quorum();

            let prepares_valid = proof
                .prepares()
                .iter()
                .filter(|stored| {
                    stored
                        .message()
                        .has_proposed_digest(&digest)
                        //If he does not have the digest, then it is not valid
                        .unwrap_or(false)
                })
                .filter(move |&stored|
                    { validate_signature::<S, _>(node, stored) })
                .count() >= view.params().quorum();

            commits_valid && prepares_valid
        })
        .max_by_key(|proof| proof.sequence_number())
}
