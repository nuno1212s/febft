//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::cell::Cell;

use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;

use std::time::{Duration, Instant};

use crate::bft::collections::{self, ConcurrentHashMap};
use crate::bft::communication::message::{
    ForwardedRequestsMessage, RequestMessage, StoredMessage, SystemMessage, ViewChangeMessage,
    ViewChangeMessageKind,
};

use crate::bft::communication::{Node, NodeId};
use crate::bft::consensus::log::MemLog;
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::ordering::{Orderable, SeqNo};

use crate::bft::timeouts::TimeoutsHandle;

use super::{AbstractSynchronizer, Synchronizer, SynchronizerStatus, TimeoutPhase};

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages
// This synchronizer will only move forward on replica messages

pub struct ReplicaSynchronizer<S: Service> {
    watching_timeouts: AtomicBool,
    timeout_seq: Cell<SeqNo>,
    timeout_dur: Cell<Duration>,
    watching: ConcurrentHashMap<Digest, TimeoutPhase>,
    _phantom: PhantomData<S>,
}

impl<S: Service> ReplicaSynchronizer<S> {
    pub fn new(timeout_dur: Duration) -> Self {
        Self {
            watching_timeouts: AtomicBool::new(false),
            timeout_seq: Cell::new(SeqNo::ZERO),
            timeout_dur: Cell::new(timeout_dur),
            watching: collections::concurrent_hash_map(),
            _phantom: Default::default(),
        }
    }

    pub(super) fn handle_stopping_quorum(
        &self,
        base_sync: &super::Synchronizer<S>,
        current_view: &ViewInfo,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        timeouts: &TimeoutsHandle<S>,
        node: &Node<S::Data>,
    ) {
        // NOTE:
        // - add requests from STOP into client requests
        //   in the log, to be ordered
        // - reset the timers of the requests in the STOP
        //   messages with TimeoutPhase::Init(_)
        // - install new view (i.e. update view seq no)
        // - send STOP-DATA message
        self.add_stopped_requests(base_sync, log);
        self.watch_all_requests(timeouts);

        let collect = log.decision_log().borrow().collect_data(current_view.clone());

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            current_view.sequence_number(),
            ViewChangeMessageKind::StopData(collect),
        ));

        node.send_signed(message, current_view.leader());
    }

    pub(super) fn handle_begin_view_change(
        &self,
        base_sync: &Synchronizer<S>,
        node: &Node<S::Data>,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
    ) {
        // stop all timers
        self.unwatch_all_requests();

        // broadcast STOP message with pending requests collected
        // from peer nodes' STOP messages
        let requests = self.stopped_requests(base_sync, timed_out);

        let current_view = base_sync.view();

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            current_view.sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));

        let targets = NodeId::targets(0..current_view.params().n());

        node.broadcast(message, targets);
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(&self, digest: Digest, timeouts: &TimeoutsHandle<S>) {
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

    fn add_stopped_requests(&self,base_sync: &Synchronizer<S>, log: &MemLog<State<S>, Request<S>, Reply<S>>) {
        // TODO: maybe optimize this `stopped_requests` call, to avoid
        // a heap allocation of a `Vec`?
        let requests = self
            .stopped_requests(base_sync, None)
            .into_iter()
            .map(|stopped| stopped.into_inner());

        for (header, request) in requests {
            self.watching
                .insert(header.unique_digest(), TimeoutPhase::TimedOut);
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
        base_sync: &Synchronizer<S>,
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

    /// Forward the requests that timed out, `timed_out`, to all the nodes in the
    /// current view.
    pub fn forward_requests(
        &self,
        base_sync: &Synchronizer<S>,
        timed_out: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        node: &Node<S::Data>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) {
        let message = SystemMessage::ForwardedRequests(ForwardedRequestsMessage::new(timed_out));
        let targets = NodeId::targets(0..base_sync.view().params().n());
        node.broadcast(message, targets);
    }

    fn next_timeout(&self) -> SeqNo {
        let next = self.timeout_seq.get();

        self.timeout_seq.replace(next.next());

        next
    }

    fn stopped_requests(
        &self,
        base_sync: &Synchronizer<S>,
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

            for (_, stopped) in base_sync.stopped.borrow().iter() {
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
            for (_, stopped) in base_sync.stopped.borrow_mut().drain() {
                for r in stopped {
                    all_reqs
                        .entry(r.header().unique_digest())
                        .or_insert_with(|| r);
                }
            }
        }

        all_reqs.drain().map(|(_, stop)| stop).collect()
    }
}

///Justification/Sort of correction proof:
///In general, all fields and methods will be accessed by the replica thread, never by the client rq thread.
/// Therefore, we only have to protect the fields that will be accessed by both clients and replicas.
/// So we protect collects, watching and tbo as those are the fields that are going to be
/// accessed by both those threads.
/// Since the other fields are going to be accessed by just 1 thread, we just need them to be Send, which they are
unsafe impl<S: Service> Sync for ReplicaSynchronizer<S> {}
