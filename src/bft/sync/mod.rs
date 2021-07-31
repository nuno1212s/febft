//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::time::{Instant, Duration};

//use either::{
//    Left,
//    Right,
//};

use crate::bft::crypto::hash::Digest;
use crate::bft::core::server::ViewInfo;
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
    Syncing(usize),
}

// TODO: finish statuses returned from `process_message`
pub enum SynchronizerStatus {
    /// We are not running the view change protocol.
    Nil,
    /// We have received STOP messages, check if we can process them.
    HaveStops,
    /// The view change protocol is currently running.
    Running,
    /// We installed a new view, resulted from running the
    /// view change protocol.
    NewView(ViewInfo),
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
    collects: HashMap<NodeId, CollectData>,
    watching: HashMap<Digest, TimeoutPhase>,
    tbo: TboQueue<Request<S>>,
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
            timeout_seq: SeqNo::from(0),
            watching: collections::hash_map(),
            stopped: collections::hash_map(),
            collects: collections::hash_map(),
            tbo: TboQueue::new(view),
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

    /// Watch a group of client requests that we received from
    /// STOP view change messages.
    pub fn watch_stopped_requests(
        &mut self,
        timeouts: &TimeoutsHandle<S>,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
    ) {
        // TODO: maybe optimize this `stopped_requests` call, to avoid
        // a heap allocation of a `Vec`?
        let requests = self
            .stopped_requests(None)
            .into_iter()
            .map(|stopped| stopped.into_inner());
        let phase = TimeoutPhase::Init(Instant::now());

        for (header, request) in requests {
            self.watch_request_impl(phase, header.unique_digest(), timeouts);
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
            ProtoPhase::Syncing(_) => {
                extract_msg!(Request<S> =>
                    &mut self.tbo.get_queue,
                    &mut self.tbo.sync
                )
            },
        }
    }

    /// Advances the state of the view change state machine.
    pub fn process_message(
        &mut self,
        header: Header,
        message: ViewChangeMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        match self.phase {
            ProtoPhase::Init => {
                match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        self.queue_stop(header, message);
                        return SynchronizerStatus::HaveStops;
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

                        let f = self.view().params().f();
                        return if i > f { SynchronizerStatus::Running }
                            else { SynchronizerStatus::Nil };
                    },
                    ViewChangeMessageKind::Stop(_) if self.stopped.contains_key(&header.from()) => {
                        // drop attempts to vote twice
                        let f = self.view().params().f();
                        return if i > f { SynchronizerStatus::Running }
                            else { SynchronizerStatus::Nil };
                    },
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        self.queue_stop_data(header, message);

                        let f = self.view().params().f();
                        return if i > f { SynchronizerStatus::Running }
                            else { SynchronizerStatus::Nil };
                    },
                    ViewChangeMessageKind::Sync(_) => {
                        self.queue_sync(header, message);

                        let f = self.view().params().f();
                        return if i > f { SynchronizerStatus::Running }
                            else { SynchronizerStatus::Nil };
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
                    self.watch_stopped_requests(
                        timeouts,
                        log,
                    );

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
                    ViewChangeMessageKind::Stop(_)=> {
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
                let collect = match message.into_kind() {
                    ViewChangeMessageKind::StopData(collect) => collect,
                    _ => unreachable!(),
                };
                self.collects.insert(header.from(), collect);

                if i == self.view().params().quorum() {
                    // TODO:
                    // - pick decision from STOP-DATA msgs
                    // - broadcast SYNC msg with collected
                    //   STOP-DATA proofs so other replicas
                    //   can repeat the leader's computation
                    unimplemented!()
                } else {
                    self.phase = ProtoPhase::StoppingData(i);
                    //SynchronizerStatus::Nil
                    unimplemented!()
                }
            },
            // TODO: other phases
            _ => unimplemented!(),
        }
    }

    /// Handle a timeout received from the timeouts layer.
    ///
    /// This timeout pertains to a group of client requests awaiting to be decided.
    pub fn client_requests_timed_out(&mut self, seq: SeqNo) -> SynchronizerStatus {
        let ignore_timeout = !self.watching_timeouts
            //
            // FIXME: maybe we should continue even after we have
            // already stopped, since we may need to forward new requests...
            //
            // tl;dr remove the `|| self.sent_stop()` line
            //
            || self.sent_stop()
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
            // we have timed out, therefore we should send a STOP msg
            (ProtoPhase::Init, _) => self.phase = ProtoPhase::Stopping2(0),
            // we have received STOP messages from peer nodes,
            // but haven't sent our own stop, yet;
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => self.phase = ProtoPhase::Stopping2(*i + 1),
            (ProtoPhase::Stopping(i), _) => self.phase = ProtoPhase::Stopping2(*i),
            // we are already running the view change proto, and sent a stop
            _ => return,
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

    fn sent_stop(&self) -> bool {
        match self.phase {
            ProtoPhase::Init | ProtoPhase::Stopping(_) => false,
            _ => true,
        }
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

    fn normalized_collects<'a>(&'a self, in_exec: SeqNo) -> impl Iterator<Item = Option<&'a CollectData>> {
        //self.collects
        //    .values()
        //    .filter(move |&c| c.incomplete_proof().executing() == in_exec)
        self.collects
            .values()
            .map(move |c| {
                if c.incomplete_proof().executing() == in_exec {
                    Some(c)
                } else {
                    None
                }
            })
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
////////////////////////////////////////////////////////////////////////////////

// 'ts' means 'timestamp', and it is equivalent to the sequence number of a view
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
    appears && count > curr_view.params().quorum()
}
