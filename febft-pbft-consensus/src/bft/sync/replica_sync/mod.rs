//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::cell::Cell;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use std::time::{Duration, Instant};
use log::{debug, error, info};
use atlas_common::collections;
use atlas_common::collections::ConcurrentHashMap;
use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::{NetworkMessageKind, StoredMessage, System};
use atlas_communication::{Node};
use atlas_execution::app::{Request, Service};
use atlas_execution::serialize::SharedData;
use atlas_core::messages::{ClientRqInfo, ForwardedRequestsMessage, RequestMessage, StoredRequestMessage, SystemMessage};
use atlas_core::request_pre_processing::{PreProcessorMessage, RequestPreProcessor};
use atlas_core::serialize::StateTransferMessage;
use atlas_core::timeouts::{RqTimeout, TimeoutKind, TimeoutPhase, Timeouts};
use atlas_metrics::metrics::{metric_duration, metric_increment};
use crate::bft::consensus::Consensus;

use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage, ViewChangeMessage, ViewChangeMessageKind};
use crate::bft::metric::{SYNC_BATCH_RECEIVED_ID, SYNC_FORWARDED_COUNT_ID, SYNC_FORWARDED_REQUESTS_ID, SYNC_STOPPED_COUNT_ID, SYNC_STOPPED_REQUESTS_ID, SYNC_WATCH_REQUESTS_ID};
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::decisions::CollectData;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::PBFT;
use crate::bft::sync::view::ViewInfo;

use super::{AbstractSynchronizer, Synchronizer, SynchronizerStatus};

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages
// This synchronizer will only move forward on replica messages

pub struct ReplicaSynchronizer<D: SharedData> {
    timeout_dur: Cell<Duration>,
    _phantom: PhantomData<D>,
}

impl<D: SharedData + 'static> ReplicaSynchronizer<D> {
    pub fn new(timeout_dur: Duration) -> Self {
        Self {
            timeout_dur: Cell::new(timeout_dur),
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
    pub(super) fn handle_stopping_quorum<ST, NT>(
        &self,
        base_sync: &Synchronizer<D>,
        previous_view: ViewInfo,
        consensus: &Consensus<D, ST>,
        log: &Log<D>,
        pre_processor: &RequestPreProcessor<D::Request>,
        timeouts: &Timeouts,
        node: &NT,
    )
        where ST: StateTransferMessage + 'static, NT: Node<PBFT<D, ST>> {
        // NOTE:
        // - install new view (i.e. update view seq no) (Done in the synchronizer)
        // - add requests from STOP into client requests
        //   in the log, to be ordered
        // - reset the timers of the requests in the STOP
        //   messages with TimeoutPhase::Init(_)
        // - send STOP-DATA message
        self.take_stopped_requests_and_register_them(base_sync, pre_processor, timeouts);
        self.watch_all_requests(timeouts);

        let view_info = base_sync.view();

        let current_view_seq = view_info.sequence_number();
        let current_leader = view_info.leader();

        let last_proof = log
            //we use the previous views' f because the new view could have changed
            //The N of the network (With reconfigurable views)
            .last_proof(previous_view.params().f());

        let incomplete_proof = consensus.collect_incomplete_proof(previous_view.params().f());

        let collect = CollectData::new(incomplete_proof, last_proof);

        debug!("{:?} // Sending STOP-DATA message collect data {:?}",
            node.id(), collect);

        let message = PBFTMessage::ViewChange(ViewChangeMessage::new(
            current_view_seq,
            ViewChangeMessageKind::StopData(collect),
        ));

        node.send_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), current_leader, true);
    }

    /// Start a new view change
    /// Receives the requests that it should send to the other
    /// nodes in its STOP message
    pub(super) fn handle_begin_view_change<ST, NT>(
        &self,
        base_sync: &Synchronizer<D>,
        timeouts: &Timeouts,
        node: &NT,
        timed_out: Option<Vec<StoredRequestMessage<D::Request>>>,
    ) where ST: StateTransferMessage + 'static, NT: Node<PBFT<D, ST>> {
        // stop all timers
        self.unwatch_all_requests(timeouts);

        // broadcast STOP message with pending requests collected
        // from peer nodes' STOP messages
        let requests = self.stopped_requests(base_sync,
                                             timed_out);

        let current_view = base_sync.view();

        //TODO: Timeout this request and keep sending it until we have achieved a new regency

        info!("{:?} // Beginning a view change from view {:?} to next view with stopped rqs {:?}",
            node.id(), current_view, requests.len());

        let message = PBFTMessage::ViewChange(ViewChangeMessage::new(
            current_view.sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));

        let targets = NodeId::targets(0..current_view.params().n());

        node.broadcast(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
    }

    /// Watch a vector of requests received
    pub fn watch_received_requests(
        &self,
        requests: Vec<ClientRqInfo>,
        timeouts: &Timeouts,
    ) {
        let start_time = Instant::now();

        timeouts.timeout_client_requests(
            self.timeout_dur.get(),
            requests,
        );

        metric_duration(SYNC_WATCH_REQUESTS_ID, start_time.elapsed());
    }

    /// Watch a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn received_request_batch(
        &self,
        pre_prepare: &StoredMessage<ConsensusMessage<D::Request>>,
        timeouts: &Timeouts,
    ) -> Vec<Digest> {
        let start_time = Instant::now();

        let requests = match pre_prepare.message().kind() {
            ConsensusMessageKind::PrePrepare(req) => { req }
            _ => {
                error!("Cannot receive a request that is not a PrePrepare");

                panic!()
            }
        };

        let mut timeout_info = Vec::with_capacity(requests.len());
        let mut digests = Vec::with_capacity(requests.len());

        let sending_node = pre_prepare.header().from();

        for x in requests {
            let header = x.header();
            let digest = header.unique_digest();

            let seq_no = x.message().sequence_number();
            let session = x.message().session_id();

            //let request_digest = header.digest().clone();

            digests.push(digest.clone());
            timeout_info.push(ClientRqInfo::new(digest, header.from(), seq_no, session));
        }

        //Notify the timeouts that we have received the following requests
        //TODO: Should this only be done after the commit phase?
        timeouts.received_pre_prepare(sending_node, timeout_info);

        metric_duration(SYNC_BATCH_RECEIVED_ID, start_time.elapsed());

        digests
    }

    /// Register all of the requests that are missing from the view change
    fn take_stopped_requests_and_register_them(&self, base_sync: &Synchronizer<D>,
                                               pre_processor: &RequestPreProcessor<D::Request>,
                                               timeouts: &Timeouts) {
        // TODO: maybe optimize this `stopped_requests` call, to avoid
        // a heap allocation of a `Vec`?

        let start_time = Instant::now();

        let requests = self.drain_stopped_request(base_sync);

        let rq_info = requests.iter().map(|rq| {
            ClientRqInfo::from(rq)
        }).collect();

        let count = requests.len();

        // Register the requests with the pre-processor
        pre_processor.send(PreProcessorMessage::StoppedRequests(requests)).unwrap();

        timeouts.timeout_client_requests(self.timeout_dur.get(), rq_info);

        debug!("Registering {} stopped requests",count);

        metric_increment(SYNC_STOPPED_COUNT_ID, Some(count as u64));
        metric_duration(SYNC_STOPPED_REQUESTS_ID, start_time.elapsed());
    }

    /// Stop watching all pending client requests.
    pub fn unwatch_all_requests(&self, timeouts: &Timeouts) {
        timeouts.cancel_client_rq_timeouts(None);
    }

    /// Restart watching all pending client requests.
    /// This happens when a new leader has been elected and
    /// We must now give him some time to propose all of the requests
    pub fn watch_all_requests(&self, timeouts: &Timeouts) {
        timeouts.reset_all_client_rq_timeouts(self.timeout_dur.get());
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
        base_sync: &Synchronizer<D>,
        my_id: NodeId,
        timed_out_rqs: &Vec<RqTimeout>,
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

        info!("{:?} // Received {} timeouts from the timeout layer",
            my_id,
            timed_out_rqs.len());

        for timed_out_rq in timed_out_rqs {
            match timed_out_rq.timeout_phase() {
                TimeoutPhase::TimedOut(id, time) => {
                    let timeout = timed_out_rq.timeout_kind();

                    let rq_info = match timeout {
                        TimeoutKind::ClientRequestTimeout(rq) => {
                            rq
                        }
                        _ => unreachable!("Only client requests should be timed out at the synchronizer")
                    };

                    if *id == 0 {
                        forwarded.push(rq_info.clone());
                    } else if *id >= 1 {
                        // The second timeout generates a stopped request
                        stopped.push(rq_info.clone());
                    }
                }
            }
        }

        if forwarded.is_empty() && stopped.is_empty() {
            debug!("{:?} // Forwarded and stopped requests are empty? What", my_id);
            return SynchronizerStatus::Nil;
        }

        if !stopped.is_empty() || !base_sync.stopped.borrow().is_empty() {
            let known_stops = self.stopped_request_digests(base_sync, None);

            for stopped_rq in known_stops {
                if stopped.contains(&stopped_rq) {
                    continue;
                }

                stopped.push(stopped_rq);
            }
        }

        info!("{:?} // Replying requests time out forwarded {}, stopped {}", my_id, forwarded.len(), stopped.len());

        debug!("{:?} // Stopped requests: {:?}", my_id, stopped);

        debug!("{:?} // Forwarded requests: {:?}", my_id, forwarded);

        SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Forward the requests that timed out, `timed_out`, to all the nodes in the
    /// current view.
    pub fn forward_requests<ST, NT>(
        &self,
        base_sync: &Synchronizer<D>,
        timed_out: Vec<StoredRequestMessage<D::Request>>,
        node: &NT,
    ) where ST: StateTransferMessage + 'static, NT: Node<PBFT<D, ST>> {
        let message = SystemMessage::ForwardedRequestMessage(ForwardedRequestsMessage::new(timed_out));
        let targets = NodeId::targets(0..base_sync.view().params().n());
        node.broadcast(NetworkMessageKind::from(message), targets);
    }

    /// Obtain the requests that we know have timed out so we can send out a stop message
    /// to other nodes
    ///
    /// Clones all the nodes in the `stopped` list
    fn stopped_requests(
        &self,
        base_sync: &Synchronizer<D>,
        requests: Option<Vec<StoredRequestMessage<D::Request>>>,
    ) -> Vec<StoredRequestMessage<D::Request>> {
        // Use a hashmap so we are sure we don't send any repeat requests in our stop messages
        let mut all_reqs = collections::hash_map();

        // Include the requests that we have timed out
        if let Some(requests) = requests {
            for r in requests {
                all_reqs.insert(r.header().unique_digest(), r);
            }
        }

        // TODO: optimize this; we are including every STOP we have
        // received thus far for the new view in our own STOP, plus
        // the requests that timed out on us
        for (_, stopped) in base_sync.stopped.borrow().iter() {
            for r in stopped {
                all_reqs
                    .entry(r.header().unique_digest())
                    .or_insert_with(|| r.clone());
            }
        }

        all_reqs.drain().map(|(_, stop)| stop).collect()
    }

    fn stopped_request_digests(
        &self,
        base_sync: &Synchronizer<D>,
        requests: Option<Vec<StoredRequestMessage<D::Request>>>,
    ) -> Vec<ClientRqInfo> {

        // Use a hashmap so we are sure we don't send any repeat requests in our stop messages
        let mut all_reqs = collections::hash_set();

        // Include the requests that we have timed out
        if let Some(requests) = requests {
            for r in requests {
                all_reqs.insert(ClientRqInfo::from(&r));
            }
        }

        // TODO: optimize this; we are including every STOP we have
        // received thus far for the new view in our own STOP, plus
        // the requests that timed out on us
        for (_, stopped) in base_sync.stopped.borrow().iter() {
            for r in stopped {
                all_reqs.insert(ClientRqInfo::from(r));
            }
        }

        all_reqs.drain().collect()
    }

    /// Drain our current received stopped messages
    fn drain_stopped_request(&self, base_sync: &Synchronizer<D>) ->
    Vec<StoredRequestMessage<D::Request>> {

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
unsafe impl<D: SharedData> Sync for ReplicaSynchronizer<D> {}
