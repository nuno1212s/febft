//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.


use std::cell::{Cell, RefCell};
use std::cmp::Ordering;


use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use intmap::IntMap;

use crate::bft::collections::{self, ConcurrentHashMap};
use crate::bft::communication::{
    Node,
    NodeId,
};
use crate::bft::communication::message::{ConsensusMessageKind, ForwardedRequestsMessage, FwdConsensusMessage, Header, RequestMessage, StoredMessage, SystemMessage, ViewChangeMessage, ViewChangeMessageKind, WireMessage};
use crate::bft::communication::serialize::{Buf, DigestData};
use crate::bft::consensus::log::{
    CollectData,
    MemLog,
    Proof,
};
use crate::bft::consensus::replica_consensus::Consensus;
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::{
    Reply,
    Request,
    Service,
    State,
};
use crate::bft::ordering::{
    Orderable,
    SeqNo,
    tbo_pop_message,
};
use crate::bft::prng;
use crate::bft::timeouts::{
    //TimeoutKind,
    TimeoutsHandle,
};

use super::{TboQueue, TimeoutPhase, FinalizeState, FinalizeStatus, ProtoPhase, SynchronizerStatus, SynchronizerPollStatus, LeaderCollects, signed_collects, sound, normalized_collects, highest_proof, collect_data, AbstractSynchronizer};


// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages
// This synchronizer will only move forward on replica messages

pub struct Synchronizer<S: Service> {
    watching_timeouts: AtomicBool,
    phase: RefCell<ProtoPhase>,
    timeout_seq: Cell<SeqNo>,
    timeout_dur: Cell<Duration>,
    stopped: RefCell<IntMap<Vec<StoredMessage<RequestMessage<Request<S>>>>>>,
    collects: Mutex<IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>>,
    watching: ConcurrentHashMap<Digest, TimeoutPhase>,
    tbo: Mutex<TboQueue<Request<S>>>,
    finalize_state: RefCell<Option<FinalizeState<Request<S>>>>,
}

///Justification/Sort of correction proof:
///In general, all fields and methods will be accessed by the replica thread, never by the client rq thread.
/// Therefore, we only have to protect the fields that will be accessed by both clients and replicas.
/// So we protect collects, watching and tbo as those are the fields that are going to be
/// accessed by both those threads.
/// Since the other fields are going to be accessed by just 1 thread, we just need them to be Send, which they are
unsafe impl<S: Service> Sync for Synchronizer<S> {}

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
        $collects_guard:expr,
        $normalized_collects:expr,
        $log:expr,
        $timeouts:expr,
        $consensus:expr,
        $node:expr $(,)?
    ) => {{
        match $self.pre_finalize($state, $proof, $normalized_collects, $log) {
            // wait for next timeout
            FinalizeStatus::NoValue => {
                $collects_guard.clear();
                SynchronizerStatus::Running
            },
            // we need to run cst before proceeding with view change
            FinalizeStatus::RunCst(state) => {
                $collects_guard.clear();
                $self.finalize_state.replace(Some(state));
                $self.phase.replace(ProtoPhase::SyncingState);
                SynchronizerStatus::RunCst
            },
            // we may finish the view change proto
            FinalizeStatus::Commit(state) => {
                $collects_guard.clear();
                $self.finalize(state, $log, $timeouts, $consensus, $node)
            },
        }
    }}
}

impl<S: Service + 'static> AbstractSynchronizer<S> for Synchronizer<S> {

    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo {
        self.tbo.lock().unwrap()
            .view.clone()
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn install_view(&self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        //self.phase = ProtoPhase::Init;
        let mut guard = self.tbo.lock().unwrap();

        guard.install_view(view);
    }

    fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>) {
        todo!()
    }
}

impl<S> Synchronizer<S>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    pub fn new(timeout_dur: Duration, view: ViewInfo) -> Arc<Self> {
        Arc::new(Self {
            timeout_dur: Cell::new(timeout_dur),
            phase: RefCell::new(ProtoPhase::Init),
            watching_timeouts: AtomicBool::new(false),
            timeout_seq: Cell::new(SeqNo::ZERO),
            watching: collections::concurrent_hash_map(),
            stopped: RefCell::new(IntMap::new()),
            collects: Mutex::new(IntMap::new()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
        })
    }

    pub fn signal(&self) {
        self.tbo.lock().unwrap().signal()
    }

    pub fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>) {
        self.tbo.lock().unwrap().queue(header, message)
    }

    pub fn can_process_stops(&self) -> bool {
        self.tbo.lock().unwrap().can_process_stops()
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(
        &self,
        digest: Digest,
        timeouts: &TimeoutsHandle<S>,
    ) {
        let phase = TimeoutPhase::Init(Instant::now());
        self.watch_request_impl(phase, digest, timeouts);
    }

    /// Watch a group of client requests that we received from a
    /// forwarded requests system message.
    pub fn watch_forwarded_requests(
        &self,
        requests: ForwardedRequestsMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
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

    ///Watch a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn watch_request_batch(
        &self,
        batch_digest: Digest,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        timeouts: &TimeoutsHandle<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) -> Vec<Digest> {
        let mut digests = Vec::with_capacity(requests.len());

        let mut final_rqs = Vec::with_capacity(requests.len());

        //TODO: Cancel ongoing timeouts of requests that are in the batch

        for x in requests {
            let header = x.header();
            let digest = header.unique_digest();


            digests.push(digest);

            final_rqs.push(x);
        }

        //It's possible that, if the latency of the client to a given replica A is smaller than the
        //Latency to leader replica B + time taken to process request in B + Latency between A and B,
        //This replica does not know of the request and yet it is valid.
        //This means that that client would not be able to process requests from that replica, which could
        //break some of the quorum properties (replica A would always be faulty for that client even if it is
        //not, so we could only tolerate f-1 faults for clients that are in that situation)
        log.insert_batched(batch_digest, final_rqs);

        digests
    }

    fn add_stopped_requests(
        &self,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
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
        &self,
        _phase: TimeoutPhase,
        _digest: Digest,
        _timeouts: &TimeoutsHandle<S>,
    ) {
        //if !self.watching_timeouts {
        //    let seq = self.next_timeout();
        //    timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));
        //    self.watching_timeouts = true;
        //}
        //self.watching.insert(digest, phase);
    }

    /// Remove a client request with digest `digest` from the watched list
    /// of requests.
    pub fn unwatch_request(&self, _digest: &Digest) {
        //self.watching.remove(digest);
        //self.watching_timeouts = !self.watching.is_empty();
    }

    /// Stop watching all pending client requests.
    pub fn unwatch_all_requests(&self) {
        // since we will be on a different seq no,
        // the time out will do nothing
        self.next_timeout();
    }

    /// Start watching all pending client requests.
    pub fn watch_all_requests(&self, _timeouts: &TimeoutsHandle<S>) {
        //let phase = TimeoutPhase::Init(Instant::now());
        //for timeout_phase in self.watching.values_mut() {
        //    *timeout_phase = phase;
        //}
        //self.watching_timeouts = !self.watching.is_empty();
        //if self.watching_timeouts {
        //    let seq = self.next_timeout();
        //    timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));
        //}
    }


    /// Check if we can process new view change messages.
    pub fn poll(&self) -> SynchronizerPollStatus<Request<S>> {
        let mut tbo_guard = self.tbo.lock().unwrap();
        match *self.phase.borrow() {
            _ if !tbo_guard.get_queue => SynchronizerPollStatus::Recv,
            ProtoPhase::Init => {
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
            ProtoPhase::SyncingState => {
                SynchronizerPollStatus::ResumeViewChange
            }
        }
    }

    /// Advances the state of the view change state machine.
    //
    // TODO: retransmit STOP msgs
    pub fn process_message(
        &self,
        header: Header,
        message: ViewChangeMessage<Request<S>>,
        timeouts: &TimeoutsHandle<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> SynchronizerStatus {
        match *self.phase.borrow() {
            ProtoPhase::Init => {
                match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return SynchronizerStatus::Nil;
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop_data(header, message);

                        return SynchronizerStatus::Nil;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return SynchronizerStatus::Nil;
                    }
                }
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
                    ViewChangeMessageKind::Stop(_) if self.stopped.borrow().contains_key(header.from().into()) => {
                        // drop attempts to vote twice
                        return stop_status!(self, i);
                    }
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop_data(header, message);

                        return stop_status!(self, i);
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

                self.stopped.borrow_mut().insert(header.from().into(), stopped);

                // NOTE: we only take this branch of the code before
                // we have sent our own STOP message
                if let ProtoPhase::Stopping(_) = *self.phase.borrow() {
                    return if i > current_view.params().f() {
                        self.begin_view_change(None, node, log);
                        SynchronizerStatus::Running
                    } else {
                        self.phase.replace(ProtoPhase::Stopping(i));
                        SynchronizerStatus::Nil
                    };
                }

                if i == current_view.params().quorum() {
                    // NOTE:
                    // - add requests from STOP into client requests
                    //   in the log, to be ordered
                    // - reset the timers of the requests in the STOP
                    //   messages with TimeoutPhase::Init(_)
                    // - install new view (i.e. update view seq no)
                    // - send STOP-DATA message
                    self.add_stopped_requests(log);
                    self.watch_all_requests(timeouts);

                    self.install_view(current_view.next_view());

                    self.phase.replace(if node.id() != current_view.leader() {
                        ProtoPhase::Syncing
                    } else {
                        ProtoPhase::StoppingData(0)
                    });

                    let collect = log.decision_log().borrow().collect_data(current_view);

                    let message = SystemMessage::ViewChange(ViewChangeMessage::new(
                        current_view.sequence_number(),
                        ViewChangeMessageKind::StopData(collect),
                    ));

                    node.send_signed(message, current_view.leader());
                } else {
                    self.phase.replace(ProtoPhase::Stopping2(i));
                }

                SynchronizerStatus::Running
            }
            ProtoPhase::StoppingData(i) => {
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
                            let mut guard = self.tbo.lock().unwrap();

                            guard.queue_stop_data(header, message);
                        }

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) if current_view.leader() != node.id() => {
                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) if collects_guard.contains_key(header.from().into()) => {
                        // drop attempts to vote twice
                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) => i + 1,
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();
                        guard.queue_sync(header, message);

                        return SynchronizerStatus::Running;
                    }
                };

                // NOTE: the STOP-DATA message signatures are already
                // verified by the TLS layer, but we still need to
                // verify their content when we retransmit the COLLECTs
                // to other nodes via a SYNC message! this guarantees
                // the new leader isn't forging messages.

                // store collects from this STOP-DATA
                collects_guard
                    .insert(header.from().into(), StoredMessage::new(header, message));

                if i != current_view.params().quorum() {
                    self.phase.replace(ProtoPhase::StoppingData(i));
                    return SynchronizerStatus::Running;
                }

                // NOTE:
                // - fetch highest CID from consensus proofs
                // - broadcast SYNC msg with collected
                //   STOP-DATA proofs so other replicas
                //   can repeat the leader's computation
                let proof = Self::highest_proof(&*collects_guard, current_view, node);

                let curr_cid = proof
                    .map(|p| p.pre_prepare().message().sequence_number())
                    .map(|seq| SeqNo::from(u32::from(seq) + 1))
                    .unwrap_or(SeqNo::ZERO);

                let normalized_collects: Vec<Option<&CollectData<Request<S>>>> =
                    Self::normalized_collects(&*collects_guard, curr_cid)
                        .collect();

                let sound = sound(current_view, &normalized_collects);
                if !sound.test() {
                    // FIXME: BFT-SMaRt doesn't do anything if `sound`
                    // evaluates to false; do we keep the same behavior,
                    // and wait for a new time out? but then, no other
                    // consensus messages have been processed... this
                    // may be a point of contention on the lib!
                    collects_guard.clear();
                    return SynchronizerStatus::Running;
                }

                let p = log.view_change_propose();

                //We create the preprepare here as we are the new leader,
                //And we sign it right now

                let (header, message) = {
                    let mut buf = Buf::new();

                    let forged_preprepare = consensus.forge_propose(p.clone(), 
                    self);

                    let digest = <S::Data as DigestData>::serialize_digest(&forged_preprepare, &mut buf)
                        .unwrap();

                    let mut prng_state = prng::State::new();

                    //Create a forged message as if the leader had sent this to us.
                    //This is safe because he has actually sent these requests in the SYNC phase
                    //TODO: This should be signed
                    let (h, _) = WireMessage::new(
                        self.view().leader(),
                        node.id(),
                        buf,
                        prng_state.next_state(),
                        Some(digest),
                        None,
                    ).into_inner();

                    let consensus = match forged_preprepare {
                        SystemMessage::Consensus(consensus) => {
                            consensus
                        }
                        _ => {
                            panic!("Returned random message from forge propose?");
                        }
                    };

                    (h, consensus)
                };

                let fwd_request = FwdConsensusMessage::new(header, message);

                let collects = collects_guard
                    .values()
                    .cloned()
                    .collect();

                let message = SystemMessage::ViewChange(ViewChangeMessage::new(
                    current_view.sequence_number(),
                    ViewChangeMessageKind::Sync(LeaderCollects { proposed: fwd_request.clone(), collects }),
                ));

                let node_id = node.id();
                let targets = NodeId::targets(0..current_view.params().n())
                    .filter(move |&id| id != node_id);

                node.broadcast(message, targets);

                let state = FinalizeState {
                    curr_cid,
                    sound,
                    proposed: fwd_request,
                };

                finalize_view_change!(
                    self,
                    state,
                    proof,
                    collects_guard,
                    normalized_collects,
                    log,
                    timeouts,
                    consensus,
                    node,
                )
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
                    ViewChangeMessageKind::StopData(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop_data(header, message);

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) if msg_seq != seq => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) if header.from() != current_view.leader() => {
                        return SynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut message = message;
                        message.take_collects().unwrap().into_inner()
                    }
                };

                // leader has already performed this computation in the
                // STOP-DATA phase of Mod-SMaRt
                let signed: Vec<_> = signed_collects::<S>(node, collects);
                let proof = highest_proof::<S, _>(current_view, node, signed.iter());
                let curr_cid = proof
                    .map(|p| p.pre_prepare().message().sequence_number())
                    .map(|seq| SeqNo::from(u32::from(seq) + 1))
                    .unwrap_or(SeqNo::ZERO);
                let normalized_collects: Vec<_> = {
                    normalized_collects(curr_cid, collect_data(signed.iter()))
                        .collect()
                };

                let sound = sound(current_view, &normalized_collects);
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

                let mut collects_guard = self.collects.lock().unwrap();

                finalize_view_change!(
                    self,
                    state,
                    proof,
                    collects_guard,
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
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        timeouts: &TimeoutsHandle<S>,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> Option<()> {
        let state = self
            .finalize_state
            .borrow_mut()
            .take()?;
        let mut lock_guard = self.collects.lock().unwrap();

        finalize_view_change!(
            self,
            state,
            None,
            lock_guard,
            Vec::new(),
            log,
            timeouts,
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
        &self,
        _seq: SeqNo,
        _timeouts: &TimeoutsHandle<S>,
    ) -> SynchronizerStatus {
        SynchronizerStatus::Nil
        //let ignore_timeout = !self.watching_timeouts
        //    || seq.next() != self.timeout_seq;

        //if ignore_timeout {
        //    return SynchronizerStatus::Nil;
        //}

        //// iterate over list of watched pending requests,
        //// and select the ones to be stopped or forwarded
        //// to peer nodes
        //let mut forwarded = Vec::new();
        //let mut stopped = Vec::new();
        //let now = Instant::now();

        //for (digest, timeout_phase) in self.watching.iter_mut() {
        //    // NOTE:
        //    // =====================================================
        //    // - on the first timeout we forward pending requests to
        //    //   the leader
        //    // - on the second timeout, we start a view change by
        //    //   broadcasting a STOP message
        //    match timeout_phase {
        //        TimeoutPhase::Init(i) if now.duration_since(*i) > self.timeout_dur => {
        //            forwarded.push(digest.clone());
        //            // NOTE: we don't update the timeout phase here, because this is
        //            // done with the message we receive locally containing the forwarded
        //            // requests, on `watch_forwarded_requests`
        //        },
        //        TimeoutPhase::TimedOutOnce(i) if now.duration_since(*i) > self.timeout_dur => {
        //            stopped.push(digest.clone());
        //            *timeout_phase = TimeoutPhase::TimedOut;
        //        },
        //        _ => (),
        //    }
        //}

        //// restart timer
        //let seq = self.next_timeout();
        //timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));

        //SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Trigger a view change locally.
    ///
    /// The value `timed_out` corresponds to a list of client requests
    /// that have timed out on the current replica.
    pub fn begin_view_change(
        &self,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
        node: &Node<S::Data>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) {
        match (&*self.phase.borrow(), &timed_out) {
            // we have received STOP messages from peer nodes,
            // but haven't sent our own STOP, yet;
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => {
                self.phase.replace(ProtoPhase::Stopping2(*i + 1));
            }
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
                self.phase.replace(ProtoPhase::Stopping2(0));
            }
        };

        // stop all timers
        self.unwatch_all_requests();

        // broadcast STOP message with pending requests collected
        // from peer nodes' STOP messages
        let requests = self.stopped_requests(timed_out);

        let current_view = self.view();

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            current_view.sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));

        let targets = NodeId::targets(0..current_view.params().n());

        node.broadcast(message, targets);
    }

    /// Forward the requests that timed out, `timed_out`, to all the nodes in the
    /// current view.
    pub fn forward_requests(
        &self,
        timed_out: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        node: &Node<S::Data>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) {
        let message = SystemMessage::ForwardedRequests(ForwardedRequestsMessage::new(
            timed_out,
        ));
        let targets = NodeId::targets(0..self.view().params().n());
        node.broadcast(message, targets);
    }


    fn next_timeout(&self) -> SeqNo {
        let next = self.timeout_seq.get();

        self.timeout_seq.replace(next.next());

        next
    }

    fn stopped_requests(
        &self,
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

            for (_, stopped) in self.stopped.borrow().iter() {
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
            for (_, stopped) in self.stopped.borrow_mut().drain() {
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
    fn normalized_collects<'a>(collects: &'a IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>, in_exec: SeqNo) -> impl Iterator<Item=Option<&'a CollectData<Request<S>>>> {
        let values = collects.values();

        let collects = normalized_collects(in_exec, collect_data(values));

        collects
    }

    // TODO: quorum sizes may differ when we implement reconfiguration
    #[inline]
    fn highest_proof<'a>(guard: &'a IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>, view: ViewInfo, node: &Node<S::Data>) -> Option<&'a Proof<Request<S>>> {
        highest_proof::<S, _>(view, node, guard.values())
    }

    // this function mostly serves the purpose of consuming
    // values with immutable references, to allow borrowing data mutably
    fn pre_finalize(
        &self,
        state: FinalizeState<Request<S>>,
        _proof: Option<&Proof<Request<S>>>,
        _normalized_collects: Vec<Option<&CollectData<Request<S>>>>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) -> FinalizeStatus<Request<S>> {
        if let ProtoPhase::Syncing = *self.phase.borrow() {
            //
            // NOTE: this code will not run when we resume
            // the view change protocol after running CST
            //
            if log.decision_log().borrow().executing() != state.curr_cid {
                return FinalizeStatus::RunCst(state);
            }
        }

        let rqs = match state.proposed.consensus().kind() {
            ConsensusMessageKind::PrePrepare(rqs) => { rqs }
            _ => {
                panic!("Can only have pre prepare messages");
            }
        };

        if rqs.is_empty() && !state.sound.test() {
            return FinalizeStatus::NoValue;
        }

        FinalizeStatus::Commit(state)
    }

    fn finalize(
        &self,
        state: FinalizeState<Request<S>>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        timeout: &TimeoutsHandle<S>,
        consensus: &mut Consensus<S>,
        node: &Node<S::Data>,
    ) -> SynchronizerStatus {
        // we will get some value to be proposed because of the
        // check we did in `pre_finalize()`, guarding against no values

        let FinalizeState { curr_cid, proposed, sound } = state;

        let (header, message) = log
            .decision_log().borrow_mut()
            .clear_last_occurrences(curr_cid, sound.value())
            .and_then(|stored| {
                Some(stored.into_inner())
            })
            .unwrap_or(proposed.into_inner());

        // finalize view change by broadcasting a PREPARE msg
        consensus.finalize_view_change((header, message), self, timeout, log, node);

        // skip queued messages from the current view change
        // and update proto phase
        self.tbo.lock().unwrap().next_instance_queue();
        self.phase.replace(ProtoPhase::Init);

        // resume normal phase
        SynchronizerStatus::NewView
    }
}

/*
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
}*/
