//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::time::{Instant, Duration};

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

//use either::{
//    Left,
//    Right,
//};

use crate::bft::prng;
use crate::bft::consensus::Consensus;
use crate::bft::crypto::hash::Digest;
use crate::bft::core::server::ViewInfo;
use crate::bft::communication::serialize::{
    DigestData,
    Buf,
};
use crate::bft::communication::{
    Node,
    NodeId
};
use crate::bft::ordering::{
    SeqNo,
    Orderable,
    tbo_pop_message,
    tbo_queue_message,
    tbo_advance_message_queue,
};
use crate::bft::consensus::log::{
    Log,
    Proof,
    CollectData,
    ViewDecisionPair,
};
use crate::bft::timeouts::{
    TimeoutKind,
    TimeoutsHandle,
};
use crate::bft::collections::{
    self,
    HashMap,
};
use crate::bft::communication::message::{
    Header,
    WireMessage,
    StoredMessage,
    SystemMessage,
    RequestMessage,
    ViewChangeMessage,
    ViewChangeMessageKind,
    ForwardedRequestsMessage,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

/// Contains the `COLLECT` structures the leader received in the `STOP-DATA` phase
/// of the view change protocol, as well as a value to be proposed in the `SYNC` message.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct LeaderCollects<O> {
    proposed: Vec<Digest>,
    collects: Vec<StoredMessage<ViewChangeMessage<O>>>,
}

impl<O> LeaderCollects<O> {
    /// Returns an empty `LeaderCollects` value.
    pub fn empty() -> LeaderCollects<O> {
        LeaderCollects {
            proposed: Vec::new(),
            collects: Vec::new(),
        }
    }

    /// Gives up ownership of the inner values of this `LeaderCollects`.
    pub fn into_inner(self) -> (Vec<Digest>, Vec<StoredMessage<ViewChangeMessage<O>>>) {
        (self.proposed, self.collects)
    }
}

struct FinalizeState {
    curr_cid: SeqNo,
    proposed: Vec<Digest>,
    sound: Sound,
}


enum FinalizeStatus {
    NoValue,
    RunCst(FinalizeState),
    Commit(FinalizeState),
}

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
            get_queue: false,
            stop: VecDeque::new(),
            stop_data: VecDeque::new(),
            sync: VecDeque::new(),
        }
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// view change messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    fn next_instance_queue(&mut self) {
        self.view = self.view.next_view();
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
    /// the next view.
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
}

#[derive(Copy, Clone)]
enum TimeoutPhase {
    // we have never received a timeout
    Init(Instant),
    // we received a second timeout for the same request;
    // start view change protocol
    TimedOutOnce(Instant),
    // keep requests that timed out stored in memory,
    // for efficienty
    TimedOut,
}

enum ProtoPhase {
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
    RequestsTimedOut { forwarded: Vec<Digest>, stopped: Vec<Digest> },
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

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages?
pub struct Synchronizer<S: Service> {
    watching_timeouts: bool,
    phase: ProtoPhase,
    timeout_seq: SeqNo,
    timeout_dur: Duration,
    stopped: HashMap<NodeId, Vec<StoredMessage<RequestMessage<Request<S>>>>>,
    collects: HashMap<NodeId, StoredMessage<ViewChangeMessage<Request<S>>>>,
    watching: HashMap<Digest, TimeoutPhase>,
    tbo: TboQueue<Request<S>>,
    finalize_state: Option<FinalizeState>,
}

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
        if $i > f { SynchronizerStatus::Running }
            else { SynchronizerStatus::Nil }
    }}
}

macro_rules! finalize_view_change {
    (
        $self:expr,
        $state:expr,
        $proof:expr,
        $normalized_collects:expr,
        $log:expr,
        $consensus:expr,
        $node:expr $(,)?
    ) => {{
        match $self.pre_finalize($state, $proof, $normalized_collects, $log) {
            // wait for next timeout
            FinalizeStatus::NoValue => {
                $self.collects.clear();
                SynchronizerStatus::Running
            },
            // we need to run cst before proceeding with view change
            FinalizeStatus::RunCst(state) => {
                $self.collects.clear();
                $self.finalize_state = Some(state);
                $self.phase = ProtoPhase::SyncingState;
                SynchronizerStatus::RunCst
            },
            // we may finish the view change proto
            FinalizeStatus::Commit(state) => {
                $self.collects.clear();
                $self.finalize(state, $log, $consensus, $node)
            },
        }
    }}
}

impl<S> Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + Clone + 'static,
    Reply<S>: Send + 'static,
{
    pub fn new(timeout_dur: Duration, view: ViewInfo) -> Self {
        Self {
            timeout_dur,
            phase: ProtoPhase::Init,
            watching_timeouts: false,
            timeout_seq: SeqNo::ZERO,
            watching: collections::hash_map(),
            stopped: collections::hash_map(),
            collects: collections::hash_map(),
            tbo: TboQueue::new(view),
            finalize_state: None,
        }
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(
        &mut self,
        digest: Digest,
        timeouts: &TimeoutsHandle<S>,
    ) {
        let phase = TimeoutPhase::Init(Instant::now());
        self.watch_request_impl(phase, digest, timeouts);
    }

    /// Watch a group of client requests that we received from a
    /// forwarded requests system message.
    pub fn watch_forwarded_requests(
        &mut self,
        requests: ForwardedRequestsMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
    ) {
        let phase = TimeoutPhase::TimedOutOnce(Instant::now());
        let requests = requests
            .into_inner()
            .into_iter()
            .map(|forwarded| forwarded.into_inner());

        for (header, request) in requests {
            self.watch_request_impl(phase, header.unique_digest(), timeouts);
            log.insert(header, SystemMessage::Request(request));
        }
    }

    fn add_stopped_requests(
        &mut self,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
    ) {
        // TODO: maybe optimize this `stopped_requests` call, to avoid
        // a heap allocation of a `Vec`?
        let requests = self
            .stopped_requests(None)
            .into_iter()
            .map(|stopped| stopped.into_inner());

        for (header, request) in requests {
            self.watching.insert(header.unique_digest(), TimeoutPhase::TimedOut);
            log.insert(header, SystemMessage::Request(request));
        }
    }

    fn watch_request_impl(
        &mut self,
        phase: TimeoutPhase,
        digest: Digest,
        timeouts: &TimeoutsHandle<S>,
    ) {
        if !self.watching_timeouts {
            let seq = self.next_timeout();
            timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));
            self.watching_timeouts = true;
        }
        self.watching.insert(digest, phase);
    }

    /// Remove a client request with digest `digest` from the watched list
    /// of requests.
    pub fn unwatch_request(&mut self, digest: &Digest) {
        self.watching.remove(digest);
        self.watching_timeouts = !self.watching.is_empty();
    }

    /// Stop watching all pending client requests.
    pub fn unwatch_all_requests(&mut self) {
        // since we will be on a different seq no,
        // the time out will do nothing
        self.next_timeout();
    }

    /// Start watching all pending client requests.
    pub fn watch_all_requests(&mut self, timeouts: &TimeoutsHandle<S>) {
        let phase = TimeoutPhase::Init(Instant::now());
        for timeout_phase in self.watching.values_mut() {
            *timeout_phase = phase;
        }
        self.watching_timeouts = !self.watching.is_empty();
        if self.watching_timeouts {
            let seq = self.next_timeout();
            timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));
        }
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    pub fn install_view(&mut self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        //self.phase = ProtoPhase::Init;
        self.tbo.view = view;
    }

    /// Check if we can process new view change messages.
    pub fn poll(&mut self) -> SynchronizerPollStatus<Request<S>> {
        match self.phase {
            _ if !self.tbo.get_queue => SynchronizerPollStatus::Recv,
            ProtoPhase::Init => {
                extract_msg!(Request<S> => 
                    { self.phase = ProtoPhase::Stopping(0); },
                    &mut self.tbo.get_queue,
                    &mut self.tbo.stop
                )
            },
            ProtoPhase::Stopping(_) | ProtoPhase::Stopping2(_) => {
                extract_msg!(Request<S> =>
                    &mut self.tbo.get_queue,
                    &mut self.tbo.stop
                )
            },
            ProtoPhase::StoppingData(_) => {
                extract_msg!(Request<S> =>
                    &mut self.tbo.get_queue,
                    &mut self.tbo.stop_data
                )
            },
            ProtoPhase::Syncing => {
                extract_msg!(Request<S> =>
                    &mut self.tbo.get_queue,
                    &mut self.tbo.sync
                )
            },
            ProtoPhase::SyncingState => {
                SynchronizerPollStatus::ResumeViewChange
            },
        }
    }

    /// Advances the state of the view change state machine.
    //
    // TODO: retransmit STOP msgs
    pub fn process_message(
        &mut self,
        header: Header,
        message: ViewChangeMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        consensus: &mut Consensus<S>,
        node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        match self.phase {
            ProtoPhase::Init => {
                match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        self.queue_stop(header, message);
                        return SynchronizerStatus::Nil;
                    },
                    ViewChangeMessageKind::StopData(_) => {
                        self.queue_stop_data(header, message);
                        return SynchronizerStatus::Nil;
                    },
                    ViewChangeMessageKind::Sync(_) => {
                        self.queue_sync(header, message);
                        return SynchronizerStatus::Nil;
                    },
                }
            },
            ProtoPhase::Stopping(i) | ProtoPhase::Stopping2(i) => {
                let msg_seq = message.sequence_number();
                let next_seq = self.view().sequence_number().next();

                let i = match message.kind() {
                    ViewChangeMessageKind::Stop(_) if msg_seq != next_seq => {
                        self.queue_stop(header, message);
                        return stop_status!(self, i);
                    },
                    ViewChangeMessageKind::Stop(_) if self.stopped.contains_key(&header.from()) => {
                        // drop attempts to vote twice
                        return stop_status!(self, i);
                    },
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        self.queue_stop_data(header, message);
                        return stop_status!(self, i);
                    },
                    ViewChangeMessageKind::Sync(_) => {
                        self.queue_sync(header, message);
                        return stop_status!(self, i);
                    },
                };

                // store pending requests from this STOP
                let stopped = match message.into_kind() {
                    ViewChangeMessageKind::Stop(stopped) => stopped,
                    _ => unreachable!(),
                };
                self.stopped.insert(header.from(), stopped);

                // NOTE: we only take this branch of the code before
                // we have sent our own STOP message
                if let ProtoPhase::Stopping(_) = self.phase {
                    return if i > self.view().params().f() {
                        self.begin_view_change(None, node);
                        SynchronizerStatus::Running
                    } else {
                        self.phase = ProtoPhase::Stopping(i);
                        SynchronizerStatus::Nil
                    };
                }

                if i == self.view().params().quorum() {
                    // NOTE:
                    // - add requests from STOP into client requests
                    //   in the log, to be ordered
                    // - reset the timers of the requests in the STOP
                    //   messages with TimeoutPhase::Init(_)
                    // - install new view (i.e. update view seq no)
                    // - send STOP-DATA message
                    self.add_stopped_requests(log);
                    self.watch_all_requests(timeouts);

                    self.phase = ProtoPhase::StoppingData(0);
                    self.install_view(self.view().next_view());

                    let collect = log.decision_log().collect_data(*self.view());
                    let message = SystemMessage::ViewChange(ViewChangeMessage::new(
                        self.view().sequence_number(),
                        ViewChangeMessageKind::StopData(collect),
                    ));
                    node.send(message, self.view().leader());
                } else {
                    self.phase = ProtoPhase::Stopping2(i);
                }

                SynchronizerStatus::Running
            },
            ProtoPhase::StoppingData(i) => {
                let msg_seq = message.sequence_number();
                let seq = self.view().sequence_number();

                // reject STOP-DATA messages if we are not the leader
                let i = match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        self.queue_stop(header, message);
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::StopData(_) if msg_seq != seq => {
                        if self.view().peek(msg_seq).leader() == node.id() {
                            self.queue_stop_data(header, message);
                        }
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::StopData(_) if self.view().leader() != node.id() => {
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::StopData(_) if self.collects.contains_key(&header.from()) => {
                        // drop attempts to vote twice
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::StopData(_) => i + 1,
                    ViewChangeMessageKind::Sync(_) => {
                        self.queue_sync(header, message);
                        return SynchronizerStatus::Running;
                    },
                };

                // NOTE: the STOP-DATA message signatures are already
                // verified by the TLS layer, but we still need to
                // verify their content when we retransmit the COLLECTs
                // to other nodes via a SYNC message! this guarantees
                // the new leader isn't forging messages.

                // store collects from this STOP-DATA
                self.collects.insert(header.from(), StoredMessage::new(header, message));

                if i != self.view().params().quorum() {
                    self.phase = ProtoPhase::StoppingData(i);
                    return SynchronizerStatus::Running;
                }

                // NOTE:
                // - fetch highest CID from consensus proofs
                // - broadcast SYNC msg with collected
                //   STOP-DATA proofs so other replicas
                //   can repeat the leader's computation
                let proof = self.highest_proof(*self.view(), node);
                let curr_cid = proof
                    .map(|p| p.pre_prepare().message().sequence_number())
                    .map(|seq| SeqNo::from(u32::from(seq) + 1))
                    .unwrap_or(SeqNo::ZERO);

                let normalized_collects: Vec<Option<&CollectData>> = self
                    .normalized_collects(curr_cid)
                    .collect();

                let sound = sound(*self.view(), &normalized_collects);
                if !sound.test() {
                    // FIXME: BFT-SMaRt doesn't do anything if `sound`
                    // evaluates to false; do we keep the same behavior,
                    // and wait for a new time out? but then, no other
                    // consensus messages have been processed... this
                    // may be a point of contention on the lib!
                    self.collects.clear();
                    return SynchronizerStatus::Running;
                }

                let p = log.view_change_propose();
                let collects = self.collects
                    .values()
                    .cloned()
                    .collect();
                let message = SystemMessage::ViewChange(ViewChangeMessage::new(
                    self.view().sequence_number(),
                    ViewChangeMessageKind::Sync(LeaderCollects { proposed: p.clone(), collects }),
                ));
                let node_id = node.id();
                let targets = NodeId::targets(0..self.view().params().n())
                    .filter(move |&id| id != node_id);
                node.broadcast(message, targets);

                let state = FinalizeState {
                    curr_cid,
                    sound,
                    proposed: p,
                };
                finalize_view_change!(
                    self,
                    state,
                    proof,
                    normalized_collects,
                    log,
                    consensus,
                    node,
                )
            },
            ProtoPhase::Syncing => {
                let msg_seq = message.sequence_number();
                let seq = self.view().sequence_number();

                // reject SYNC messages if these were not sent by the leader
                let (proposed, collects) = match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        self.queue_stop(header, message);
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::StopData(_) => {
                        self.queue_stop_data(header, message);
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::Sync(_) if msg_seq != seq => {
                        self.queue_sync(header, message);
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::Sync(_) if header.from() != self.view().leader() => {
                        return SynchronizerStatus::Running;
                    },
                    ViewChangeMessageKind::Sync(_) => {
                        let mut message = message;
                        message.take_collects().unwrap().into_inner()
                    },
                };

                // leader has already performed this computation in the
                // STOP-DATA phase of Mod-SMaRt
                let signed: Vec<_> = signed_collects::<S>(node, collects);
                let proof = highest_proof::<S, _>(*self.view(), node, signed.iter());
                let curr_cid = proof
                    .map(|p| p.pre_prepare().message().sequence_number())
                    .map(|seq| SeqNo::from(u32::from(seq) + 1))
                    .unwrap_or(SeqNo::ZERO);
                let normalized_collects: Vec<_> = {
                    normalized_collects(curr_cid, collect_data(signed.iter()))
                        .collect()
                };

                let sound = sound(*self.view(), &normalized_collects);
                if !sound.test() {
                    // FIXME: BFT-SMaRt doesn't do anything if `sound`
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
                };
                finalize_view_change!(
                    self,
                    state,
                    proof,
                    normalized_collects,
                    log,
                    consensus,
                    node,
                )
            },
            // handled by `resume_view_change()`
            ProtoPhase::SyncingState => unreachable!(),
        }
    }

    /// Resume the view change protocol after running the CST protocol.
    pub fn resume_view_change(
        &mut self,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        consensus: &mut Consensus<S>,
        node: &mut Node<S::Data>,
    ) -> Option<()> {
        let state = self
            .finalize_state
            .take()?;
        finalize_view_change!(
            self,
            state,
            None,
            Vec::new(),
            log,
            consensus,
            node,
        );
        Some(())
    }

    /// Handle a timeout received from the timeouts layer.
    ///
    /// This timeout pertains to a group of client requests awaiting to be decided.
    //
    //
    // TODO: fix current timeout impl, as most requests won't actually
    // have surpassed their defined timeout period, after the timeout event
    // is fired on the master channel of the core server task
    //
    pub fn client_requests_timed_out(
        &mut self,
        seq: SeqNo,
        timeouts: &TimeoutsHandle<S>,
    ) -> SynchronizerStatus {
        let ignore_timeout = !self.watching_timeouts
            || seq.next() != self.timeout_seq;

        if ignore_timeout {
            return SynchronizerStatus::Nil;
        }

        // iterate over list of watched pending requests,
        // and select the ones to be stopped or forwarded
        // to peer nodes
        let mut forwarded = Vec::new();
        let mut stopped = Vec::new();
        let now = Instant::now();

        for (digest, timeout_phase) in self.watching.iter_mut() {
            // NOTE:
            // =====================================================
            // - on the first timeout we forward pending requests to
            //   the leader
            // - on the second timeout, we start a view change by
            //   broadcasting a STOP message
            match timeout_phase {
                TimeoutPhase::Init(i) if now.duration_since(*i) > self.timeout_dur => {
                    forwarded.push(digest.clone());
                    // NOTE: we don't update the timeout phase here, because this is
                    // done with the message we receive locally containing the forwarded
                    // requests, on `watch_forwarded_requests`
                },
                TimeoutPhase::TimedOutOnce(i) if now.duration_since(*i) > self.timeout_dur => {
                    stopped.push(digest.clone());
                    *timeout_phase = TimeoutPhase::TimedOut;
                },
                _ => (),
            }
        }

        // restart timer
        let seq = self.next_timeout();
        timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));

        SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Trigger a view change locally.
    ///
    /// The value `timed_out` corresponds to a list of client requests
    /// that have timed out on the current replica.
    pub fn begin_view_change(
        &mut self,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
        node: &mut Node<S::Data>,
    ) {
        match (&self.phase, &timed_out) {
            // we have received STOP messages from peer nodes,
            // but haven't sent our own STOP, yet;
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => self.phase = ProtoPhase::Stopping2(*i + 1),
            (ProtoPhase::Stopping(i), _) => self.phase = ProtoPhase::Stopping2(*i),
            // we have timed out, therefore we should send a STOP msg;
            //
            // note that we might have already been running the view change proto,
            // and started another view because we timed out again (e.g. because of
            // a faulty leader during the view change)
            _ => {
                // clear state from previous views
                self.stopped.clear();
                self.collects.clear();
                self.phase = ProtoPhase::Stopping2(0);
            },
        }

        // stop all timers
        self.unwatch_all_requests();

        // broadcast STOP message with pending requests collected
        // from peer nodes' STOP messages
        let requests = self.stopped_requests(timed_out);
        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            self.view().sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));
        let targets = NodeId::targets(0..self.view().params().n());
        node.broadcast(message, targets);
    }

    /// Forward the requests that timed out, `timed_out`, to all the nodes in the
    /// current view.
    pub fn forward_requests(
        &self,
        timed_out: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        node: &mut Node<S::Data>,
    ) {
        let message = SystemMessage::ForwardedRequests(ForwardedRequestsMessage::new(
            timed_out,
        ));
        let targets = NodeId::targets(0..self.view().params().n());
        node.broadcast(message, targets);
    }

    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    pub fn view(&self) -> &ViewInfo {
        &self.tbo.view
    }

    fn next_timeout(&mut self) -> SeqNo {
        let next = self.timeout_seq;
        self.timeout_seq = self.timeout_seq.next();
        next
    }

    fn stopped_requests(
        &mut self,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
    ) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        let mut all_reqs = collections::hash_map();

        // TODO: optimize this; we are including every STOP we have
        // received thus far for the new view in our own STOP, plus
        // the requests that timed out on us
        if let Some(requests) = timed_out {
            for r in requests {
                all_reqs.insert(r.header().unique_digest(), r);
            }
            for (_, stopped) in self.stopped.iter() {
                for r in stopped {
                    all_reqs
                        .entry(r.header().unique_digest())
                        .or_insert_with(|| r.clone());
                }
            }
        } else {
            // we did not time out, but rather are just
            // clearing the buffer of STOP messages received
            // for the current view change
            for (_, stopped) in self.stopped.drain() {
                for r in stopped {
                    all_reqs
                        .entry(r.header().unique_digest())
                        .or_insert_with(|| r);
                }
            }
        }

        all_reqs
            .drain()
            .map(|(_, stop)| stop)
            .collect()
    }

    // collects whose in execution cid is different from the given `in_exec` become `None`
    #[inline]
    fn normalized_collects<'a>(&'a self, in_exec: SeqNo) -> impl Iterator<Item = Option<&'a CollectData>> {
        normalized_collects(in_exec, collect_data(self.collects.values()))
    }

    // TODO: quorum sizes may differ when we implement reconfiguration
    #[inline]
    fn highest_proof<'a>(&'a self, view: ViewInfo, node: &Node<S::Data>) -> Option<&'a Proof> {
        highest_proof::<S, _>(view, node, self.collects.values())
    }

    // this function mostly serves the purpose of consuming
    // values with immutable references, to allow borrowing data mutably
    fn pre_finalize(
        &self,
        state: FinalizeState,
        _proof: Option<&Proof>,
        _normalized_collects: Vec<Option<&CollectData>>,
        log: &Log<State<S>, Request<S>, Reply<S>>,
    ) -> FinalizeStatus {
        if let ProtoPhase::Syncing = self.phase {
            //
            // NOTE: this code will not run when we resume
            // the view change protocol after running CST
            //
            if log.decision_log().executing() != state.curr_cid {
                return FinalizeStatus::RunCst(state);
            }
        }

        if state.proposed.is_empty() && !state.sound.test() {
            return FinalizeStatus::NoValue;
        }

        FinalizeStatus::Commit(state)
    }

    fn finalize(
        &mut self,
        FinalizeState { curr_cid, proposed, sound }: FinalizeState,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        consensus: &mut Consensus<S>,
        node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        // we will get some value to be proposed because of the
        // check we did in `pre_finalize()`, guarding against no values
        let proposed = log
            .decision_log_mut()
            .clear_last_occurrences(curr_cid, sound.value())
            .and_then(|stored| {
                let (_, mut message) = stored.into_inner();
                message.take_proposed_requests()
            })
            .unwrap_or(proposed);

        // store new proposed value in the log
        let (digest, header, message) = {
            //
            // NOTE: yeah I know this code is ugly innit :^)
            //
            // TODO:
            // - have leader somehow sign the PRE-PREPARE
            //   message we are about to insert in the log?
            // - maybe optimize this
            //
            let mut buf = Buf::new();
            let m = consensus.forge_propose(proposed, self);
            let digest = <S::Data as DigestData>::serialize_digest(&m, &mut buf)
                .unwrap();
            let mut prng_state = prng::State::new();
            let (h, _) = WireMessage::new(
                self.view().leader(),
                node.id(),
                &buf,
                prng_state.next_state(),
                Some(digest),
                None,
            ).into_inner();
            (digest, h, m)
        };
        log.insert(header, message);

        // finalize view change by broadcasting a PREPARE msg
        consensus.finalize_view_change(digest, self, log, node);

        // skip queued messages from the current view change
        // and update proto phase
        self.tbo.next_instance_queue();
        self.phase = ProtoPhase::Init;

        // resume normal phase
        SynchronizerStatus::NewView
    }
}

impl<S> Deref for Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    type Target = TboQueue<Request<S>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tbo
    }
}

impl<S> DerefMut for Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tbo
    }
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

fn sound<'a>(
    curr_view: ViewInfo,
    normalized_collects: &[Option<&'a CollectData>],
) -> Sound {
    // collect timestamps and values
    let mut timestamps = collections::hash_set();
    let mut values = collections::hash_set();

    for maybe_collect in normalized_collects.iter() {
        // NOTE: BFT-SMaRt assumes normalized values start on view 0,
        // if their CID is different from the one in execution;
        // see `LCManager::normalizeCollects` on its code
        let c = match maybe_collect {
            Some(c) => c,
            None => {
                timestamps.insert(SeqNo::ZERO);
                continue;
            },
        };

        // add quorum write timestamp
        timestamps.insert(c
            .incomplete_proof()
            .quorum_writes()
            .map(|ViewDecisionPair(ts, _)| *ts)
            .unwrap_or(SeqNo::ZERO));

        // add writeset timestamps and values
        for ViewDecisionPair(ts, value) in c.incomplete_proof().write_set().iter() {
            timestamps.insert(*ts);
            values.insert(value);
        }
    }

    for ts in timestamps {
        for value in values.iter() {
            if binds(curr_view, ts, value, normalized_collects) {
                return Sound::Bound(**value);
            }
        }
    }

    Sound::Unbound(unbound(curr_view, normalized_collects))
}

fn binds(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData>],
) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        false
    } else {
        quorum_highest(curr_view, ts, value, normalized_collects)
            && certified_value(curr_view, ts, value, normalized_collects)
    }
}

fn unbound(
    curr_view: ViewInfo,
    normalized_collects: &[Option<&CollectData>],
) -> bool {
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
                            .map(|ViewDecisionPair(other_ts, _)| {
                                *other_ts == SeqNo::ZERO
                            })
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

fn quorum_highest(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData>],
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
                .map(|ViewDecisionPair(other_ts, other_value)| {
                    match other_ts.cmp(&ts) {
                        Ordering::Less => true,
                        Ordering::Equal if other_value == value => true,
                        _ => false,
                    }
                })
                .unwrap_or(false)
        })
        .count();
    appears && count >= curr_view.params().quorum()
}

fn certified_value(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData>],
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
    collects: impl Iterator<Item = &'a StoredMessage<ViewChangeMessage<O>>>,
) -> impl Iterator<Item = &'a CollectData> {
    collects
        .filter_map(|stored| {
            match stored.message().kind() {
                ViewChangeMessageKind::StopData(collects) => Some(collects),
                _ => None,
            }
        })
}

fn normalized_collects<'a>(
    in_exec: SeqNo,
    collects: impl Iterator<Item = &'a CollectData>,
) -> impl Iterator<Item = Option<&'a CollectData>> {
    collects
        .map(move |collect| {
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

fn validate_signature<'a, S, M>(
    node: &'a Node<S::Data>,
    stored: &'a StoredMessage<M>,
) -> bool
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + Clone + 'static,
    Reply<S>: Send + 'static,
{
    let wm = match WireMessage::from_parts(*stored.header(), &[]) {
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
    view: ViewInfo,
    node: &Node<S::Data>,
    collects: I,
) -> Option<&'a Proof>
where
    I: Iterator<Item = &'a StoredMessage<ViewChangeMessage<Request<S>>>>,
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
        // TODO: check proofs and digests of PREPAREs as well, eventually,
        // but for now we are replicating the behavior of BFT-SMaRt
        .filter(move |proof| {
            let digest = proof
                .pre_prepare()
                .header()
                .digest();

            proof
                .commits()
                .iter()
                .filter(|stored| {
                    stored
                        .message()
                        .has_proposed_digest(digest)
                        .unwrap_or(false)
                })
                .filter(move |&stored| validate_signature::<S, _>(node, stored))
                .count() >= view.params().quorum()
        })
        .max_by_key(|proof| {
            proof
                .pre_prepare()
                .message()
                .sequence_number()
        })
}
