//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::cell::Cell;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use std::time::{Duration, Instant};
use log::{debug, error};

use crate::bft::collections::{self, ConcurrentHashMap};
use crate::bft::communication::message::{
    ForwardedRequestsMessage, RequestMessage, StoredMessage, SystemMessage, ViewChangeMessage,
    ViewChangeMessageKind, ConsensusMessage, ConsensusMessageKind,
};

use crate::bft::communication::{Node, NodeId};
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::{Request, Service};
use crate::bft::globals::ReadOnly;
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::sync::view::ViewInfo;
use crate::bft::timeouts::{ClientRqInfo, TimeoutKind, Timeouts};

use super::{AbstractSynchronizer, Synchronizer, SynchronizerStatus, TimeoutPhase};

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages
// This synchronizer will only move forward on replica messages

pub struct ReplicaSynchronizer<S: Service> {
    timeout_dur: Cell<Duration>,
    watching: ConcurrentHashMap<Digest, TimeoutPhase>,
    _phantom: PhantomData<S>,
}

impl<S: Service + 'static> ReplicaSynchronizer<S> {
    pub fn new(timeout_dur: Duration) -> Self {
        Self {
            timeout_dur: Cell::new(timeout_dur),
            watching: collections::concurrent_hash_map(),
            _phantom: Default::default(),
        }
    }

    /// Handle having received a quorum of Stopping messages
    /// This means we are ready to move to the next view
    /// From this point we will move to the State transfer protocol
    /// So we are 100% sure we have all the necessary data
    ///
    /// Therefore, we start by clearing our stopped requests and treating them as
    /// newly proposed requests (by resetting their timer)
    pub(super) fn handle_stopping_quorum(
        &self,
        base_sync: &Synchronizer<S>,
        previous_view: ViewInfo,
        log: &DecidedLog<S>,
        pending_rq_log: &PendingRequestLog<S>,
        timeouts: &Timeouts,
        node: &Node<S::Data>,
    ) {
        // NOTE:
        // - install new view (i.e. update view seq no) (Done in the synchronizer)
        // - add requests from STOP into client requests
        //   in the log, to be ordered
        // - reset the timers of the requests in the STOP
        //   messages with TimeoutPhase::Init(_)
        // - send STOP-DATA message
        self.add_stopped_requests(base_sync, pending_rq_log);
        self.watch_all_requests(timeouts);

        let current_view_seq = base_sync.view().sequence_number();
        let current_leader = base_sync.view().leader();

        let collect = log.decision_log()
            //we use the previous views' f because the new view could have changed
            //The N of the network (With reconfigurable views)
            .collect_data(previous_view.params().f());

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            current_view_seq,
            ViewChangeMessageKind::StopData(collect),
        ));

        node.send_signed(message, current_leader);
    }

    /// Start a new view change
    /// Receives the requests that it should send to the other
    /// nodes in its STOP message
    pub(super) fn handle_begin_view_change(
        &self,
        base_sync: &Synchronizer<S>,
        timeouts: &Timeouts,
        node: &Node<S::Data>,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
    ) {
        // stop all timers
        self.unwatch_all_requests(timeouts);

        // broadcast STOP message with pending requests collected
        // from peer nodes' STOP messages
        let requests = self.stopped_requests(base_sync,
                                             timed_out);

        let current_view = base_sync.view();

        //TODO: Timeout this request and keep sending it until we have achieved a new regency

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            current_view.sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));

        let targets = NodeId::targets(0..current_view.params().n());

        node.broadcast(message, targets);
    }


    /// Watch a group of client requests that we received from a
    /// forwarded requests system message.
    ///
    pub fn watch_forwarded_requests(
        &self,
        requests: ForwardedRequestsMessage<Request<S>>,
        timeouts: &Timeouts,
        log: &PendingRequestLog<S>,
    ) {

        let phase = TimeoutPhase::TimedOutOnce(Instant::now());

        let requests = requests
            .into_inner()
            .into_iter()
            .map(|forwarded| forwarded.into_inner());

        let mut digests = Vec::with_capacity(requests.len());

        for (header, request) in requests {
            log.insert(header, request);

            let unique_digest = header.unique_digest();

            digests.push(unique_digest.clone());

            if let Some(mut req) = self.watching.get_mut(&unique_digest) {
                *req.value_mut() = phase;
            } else {
                self.watching.insert(unique_digest, phase);
            }
        }

        timeouts.timeout_client_requests(self.timeout_dur.get(), digests);
    }

    /// Watch a vector of requests received
    pub fn watch_received_requests(
        &self,
        requests: Vec<Digest>,
        timeouts: &Timeouts,
    ) {
        let phase = TimeoutPhase::Init(Instant::now());

        for x in &requests {
            self.watching.insert(x.clone(), phase.clone());
        }

        timeouts.timeout_client_requests(
            self.timeout_dur.get(),
            requests,
        );
    }

    ///Watch a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn received_request_batch(
        &self,
        pre_prepare: &StoredMessage<ConsensusMessage<Request<S>>>,
        timeouts: &Timeouts,
    ) -> Vec<Digest> {
        let requests = match pre_prepare.message().kind() {
            ConsensusMessageKind::PrePrepare(req) => { req }
            _ => {
                error!("Cannot receive a request that is not a PrePrepare");

                panic!()
            }
        };

        let mut digests = Vec::with_capacity(requests.len());

        //let mut client_rqs = Vec::with_capacity(requests.len());

        let sending_node = pre_prepare.header().from();

        for x in requests {
            let header = x.header();
            let digest = header.unique_digest();

            let seq_no = x.message().sequence_number();
            let session = x.message().session_id();

            let request_digest = header.digest().clone();

            //remove the request from the requests we are currently watching
            self.watching.remove(&digest);

            //client_rqs.push(TimeoutKind::ClientRequestTimeout(
            //    ClientRqInfo::new(digest.clone())));

            digests.push(digest);
        }

        //Notify the timeouts that we have received the following requests
        timeouts.received_pre_prepare(sending_node, digests.clone());

        //If we only send the digest of the request in the pre prepare
        //It's possible that, if the latency of the client to a given replica A is smaller than the
        //Latency to leader replica B + time taken to process request in B + Latency between A and B,
        //This replica does not know of the request and yet it is valid.
        //This means that that client would not be able to process requests from that replica, which could
        //break some of the quorum properties (replica A would always be faulty for that client even if it is
        //not, so we could only tolerate f-1 faults for clients that are in that situation)
        //log.insert_batched(pre_prepare);

        digests
    }

    /// Register all of the requests that are missing from the view change
    fn add_stopped_requests(&self, base_sync: &Synchronizer<S>, log: &PendingRequestLog<S>) {
        // TODO: maybe optimize this `stopped_requests` call, to avoid
        // a heap allocation of a `Vec`?

        let requests = self
            .drain_stopped_request(base_sync)
            .into_iter()
            .map(|stopped| stopped.into_inner());

        for (header, _request) in requests {
            self.watching
                .insert(header.unique_digest(), TimeoutPhase::TimedOut);
        }
    }

    fn watch_request_impl(
        &self,
        _phase: TimeoutPhase,
        _digest: Digest,
        _timeouts: &Timeouts,
    ) {}

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(&self, digest: Digest, timeouts: &Timeouts) {
        let phase = TimeoutPhase::Init(Instant::now());
        self.watch_request_impl(phase, digest, timeouts);
    }

    /// Remove a client request with digest `digest` from the watched list
    /// of requests.
    pub fn unwatch_request(&self, digest: &Digest, timeouts: &Timeouts) {
        self.watching.remove(digest);

        timeouts.cancel_client_rq_timeouts(Some(vec![digest.clone()]));
    }

    /// Stop watching all pending client requests.
    pub fn unwatch_all_requests(&self, timeouts: &Timeouts) {
        // since we will be on a different seq no,
        // the time out will do nothing
        self.watching.clear();

        timeouts.cancel_client_rq_timeouts(None);
    }

    /// Restart watching all pending client requests.
    /// This happens when a new leader has been elected and
    /// We must now give him some time to propose all of the requests
    pub fn watch_all_requests(&self, timeouts: &Timeouts) {
        let mut digests = Vec::with_capacity(self.watching.len());

        let phase = TimeoutPhase::Init(Instant::now());

        self.watching.iter_mut().for_each(|mut digest| {
            let rq_digest = digest.key().clone();

            let curr_phase = digest.value_mut();

            *curr_phase = phase;

            digests.push(rq_digest);
        });

        timeouts.timeout_client_requests(self.timeout_dur.get(), digests);
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
        timed_out_rqs: &Vec<ClientRqInfo>,
    ) -> SynchronizerStatus {

        //// iterate over list of watched pending requests,
        //// and select the ones to be stopped or forwarded
        //// to peer nodes
        let mut forwarded = Vec::new();
        let mut stopped = Vec::new();
        let now = Instant::now();

        // NOTE:
        // =====================================================
        // - on the first timeout we forward pending requests to
        //   the leader
        // - on the second timeout, we start a view change by
        //   broadcasting a STOP message

        debug!("Received {} timeouts from the timeout layer", timed_out_rqs.len());

        for timed_out_rq in timed_out_rqs {
            let watching_request = self.watching.get_mut(&timed_out_rq.digest);

            if let Some(mut watched_request) = watching_request {
                let digest = watched_request.key().clone();

                let mut timeout_phase = watched_request.value_mut();

                match timeout_phase {
                    TimeoutPhase::Init(instant)
                    if now.duration_since(*instant) >= self.timeout_dur.get() => {

                        forwarded.push(digest);
                        // NOTE: we don't update the timeout phase here, because this is
                        // done with the message we receive locally containing the forwarded
                        // requests, on `watch_forwarded_requests`
                        // The timer will also be set there
                    }

                    TimeoutPhase::TimedOutOnce(instant)
                    if now.duration_since(*instant) >= self.timeout_dur.get() => {
                        stopped.push(digest.clone());

                        *timeout_phase = TimeoutPhase::TimedOut;
                    }
                    _ => {}
                }
            }
        }

        if forwarded.is_empty() && stopped.is_empty() {
            debug!("Forwarded and stopped requests are empty? What");
            return SynchronizerStatus::Nil;
        }

        debug!("Replying requests time out forwarded {}, stopped {}", forwarded.len(), stopped.len());

        SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Forward the requests that timed out, `timed_out`, to all the nodes in the
    /// current view.
    pub fn forward_requests(
        &self,
        base_sync: &Synchronizer<S>,
        timed_out: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        node: &Node<S::Data>,
        _log: &PendingRequestLog<S>,
    ) {
        let message = SystemMessage::ForwardedRequests(ForwardedRequestsMessage::new(timed_out));
        let targets = NodeId::targets(0..base_sync.view().params().n());
        node.broadcast(message, targets);
    }

    /// Obtain the requests that we know have timed out
    fn stopped_requests(
        &self,
        base_sync: &Synchronizer<S>,
        requests: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
    ) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        // Use a hashmap so we are sure we don't send any repeat requests in our stop messages
        let mut all_reqs = collections::hash_map();

        // TODO: optimize this; we are including every STOP we have
        // received thus far for the new view in our own STOP, plus
        // the requests that timed out on us
        if let Some(requests) = requests {
            for r in requests {
                all_reqs.insert(r.header().unique_digest(), r);
            }
        }

        for (_, stopped) in base_sync.stopped.borrow().iter() {
            for r in stopped {
                all_reqs
                    .entry(r.header().unique_digest())
                    .or_insert_with(|| r.clone());
            }
        }

        all_reqs.drain().map(|(_, stop)| stop).collect()
    }

    /// Drain our current received stopped messages
    fn drain_stopped_request(&self, base_sync: &Synchronizer<S>) ->
    Vec<StoredMessage<RequestMessage<Request<S>>>> {

        // Use a hashmap so we are sure we don't send any repeat requests in our stop messages
        let mut all_reqs = collections::hash_map();

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
