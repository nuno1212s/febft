use std::sync::Arc;
use std::time::Instant;
use intmap::IntMap;
use log::warn;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotTx};
use febft_common::collections::HashMap;
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_execution::serialize::SharedData;
use febft_metrics::metrics::{metric_duration, metric_increment};
use crate::messages::{ClientRqInfo, RequestMessage, StoredRequestMessage};
use crate::metric::{RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID, RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID, RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID, RQ_PP_WORKER_ORDER_PROCESS_ID};
use crate::request_pre_processing::{operation_key, operation_key_raw, PreProcessorOutput, PreProcessorOutputMessage};
use crate::timeouts::{RqTimeout, TimeoutKind, TimeoutPhase};

const WORKER_QUEUE_SIZE: usize = 124;
const WORKER_THREAD_NAME: &str = "RQ-PRE-PROCESSING-WORKER-{}";

pub type PreProcessorWorkMessageOuter<O> = (Instant, PreProcessorWorkMessage<O>);

pub enum PreProcessorWorkMessage<O> {
    /// We have received requests from the clients, which need
    /// to be processed
    ClientPoolOrderedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Client pool requests received
    ClientPoolUnorderedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Received requests that were forwarded from other replicas
    ForwardedRequestsReceived(Vec<StoredRequestMessage<O>>),
    StoppedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Analyse timeout requests. Returns only timeouts that have not yet been executed
    TimeoutsReceived(Vec<RqTimeout>, OneShotTx<Vec<RqTimeout>>),
    /// A batch of requests has been decided by the system
    DecidedBatch(Vec<ClientRqInfo>),
    /// Collect all pending messages from the given worker
    CollectPendingMessages(OneShotTx<Vec<StoredRequestMessage<O>>>),
    /// Clone a set of given pending requests
    ClonePendingRequests(Vec<ClientRqInfo>, OneShotTx<Vec<StoredRequestMessage<O>>>),
    /// Remove all requests associated with this client (due to a disconnection, for example)
    CleanClient(NodeId),
}

/// Each worker will be assigned a given set of clients
pub struct RequestPreProcessingWorker<O> {
    /// Receive work
    message_rx: ChannelSyncRx<PreProcessorWorkMessageOuter<O>>,

    /// Output for the requests that have been processed and should now be proposed
    batch_production: ChannelSyncTx<PreProcessorOutput<O>>,

    /// The latest operations seen by this worker.
    /// Since a given session will always be handled by the same worker,
    /// we can use this to filter out duplicates.
    latest_ops: IntMap<SeqNo>,
    /// The requests that have not been added to a batch yet.
    pending_requests: HashMap<Digest, StoredRequestMessage<O>>,
}


impl<O> RequestPreProcessingWorker<O> where O: Clone {
    pub fn new(message_rx: ChannelSyncRx<PreProcessorWorkMessageOuter<O>>, batch_production: ChannelSyncTx<PreProcessorOutput<O>>) -> Self {
        Self {
            message_rx,
            batch_production,
            latest_ops: Default::default(),
            pending_requests: Default::default(),
        }
    }

    pub(crate) fn run(mut self) {
        loop {
            let (sent_time, recvd_message) = self.message_rx.recv().unwrap();

            match recvd_message {
                PreProcessorWorkMessage::ClientPoolOrderedRequestsReceived(requests) => {
                    self.process_ordered_client_pool_requests(requests);
                }
                PreProcessorWorkMessage::ClientPoolUnorderedRequestsReceived(requests) => {
                    self.process_unordered_client_pool_rqs(requests);
                }
                PreProcessorWorkMessage::ForwardedRequestsReceived(requests) => {
                    self.process_forwarded_requests(requests);
                }
                PreProcessorWorkMessage::TimeoutsReceived(requests, tx) => {
                    self.process_timeouts(requests, tx);
                }
                PreProcessorWorkMessage::DecidedBatch(requests) => {
                    self.process_decided_batch(requests);
                }
                PreProcessorWorkMessage::CollectPendingMessages(tx) => {
                    let reqs = self.collect_pending_requests();

                    tx.send(reqs).expect("Failed to send pending requests");
                }
                PreProcessorWorkMessage::ClonePendingRequests(requests, tx) => {
                    self.clone_pending_requests(requests, tx);
                }
                PreProcessorWorkMessage::CleanClient(client) => {
                    self.clean_client(client);
                }
                PreProcessorWorkMessage::StoppedRequestsReceived(reqs) => {
                    self.stopped_requests(reqs);
                }
            }

            metric_duration(RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID, sent_time.elapsed());
        }
    }

    /// Checks if we have received a more recent message for a given client/session combo
    fn has_received_more_recent_and_update(&mut self, header: &Header, message: &RequestMessage<O>) -> bool {
        let key = operation_key::<O>(header, message);

        let seq_no = {
            if let Some(seq_no) = self.latest_ops.get_mut(key) {
                seq_no.clone()
            } else {
                SeqNo::ZERO
            }
        };

        let has_received_more_recent = seq_no >= message.sequence_number();

        if !has_received_more_recent {
            self.latest_ops.insert(key, message.sequence_number());
        }

        has_received_more_recent
    }

    fn update_most_recent(&mut self, rq_info: &ClientRqInfo) {
        let key = operation_key_raw(rq_info.sender, rq_info.session);

        let seq_no = {
            if let Some(seq_no) = self.latest_ops.get_mut(key) {
                seq_no.clone()
            } else {
                SeqNo::ZERO
            }
        };

        let has_received_more_recent = seq_no >= rq_info.seqno;

        if !has_received_more_recent {
            self.latest_ops.insert(key, rq_info.seqno);
        }
    }

    /// Process the ordered client pool requests
    fn process_ordered_client_pool_requests(&mut self, requests: Vec<StoredRequestMessage<O>>) {

        let start = Instant::now();

        let processed_rqs = requests.len();

        let requests = requests.into_iter().filter(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return false;
            }

            let digest = request.header().unique_digest();

            self.pending_requests.insert(digest.clone(), request.clone());

            return true;
        }).collect();

        self.batch_production.send((PreProcessorOutputMessage::DeDupedOrderedRequests(requests), Instant::now())).expect("Failed to send batch to proposer");

        metric_duration(RQ_PP_WORKER_ORDER_PROCESS_ID, start.elapsed());
        metric_increment(RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID, Some(processed_rqs as u64));
    }

    /// Process the unordered client pool requests
    fn process_unordered_client_pool_rqs(&mut self, requests: Vec<StoredRequestMessage<O>>) {
        let requests = requests.into_iter().filter(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return false;
            }

            return true;
        }).collect();

        self.batch_production.send((PreProcessorOutputMessage::DeDupedUnorderedRequests(requests), Instant::now())).expect("Failed to send batch to proposer");
    }

    /// Process the forwarded requests
    fn process_forwarded_requests(&mut self, requests: Vec<StoredRequestMessage<O>>) {
        let requests = requests.into_iter().filter(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return true;
            }

            let digest = request.header().unique_digest();

            self.pending_requests.insert(digest.clone(), request.clone());

            return false;
        }).collect();

        self.batch_production.send((PreProcessorOutputMessage::DeDupedOrderedRequests(requests), Instant::now())).expect("Failed to send batch to proposer");
    }

    /// Process the timeouts
    fn process_timeouts(&mut self, mut timeouts: Vec<RqTimeout>, tx: OneShotTx<Vec<RqTimeout>>) {
        let mut returned_timeouts = Vec::with_capacity(timeouts.len());

        for timeout in timeouts {
            let result = if let TimeoutKind::ClientRequestTimeout(rq_info) = timeout.timeout_kind() {
                let key = operation_key_raw(rq_info.sender, rq_info.session);

                if let Some(seq_no) = self.latest_ops.get(key) {
                    *seq_no < rq_info.seqno
                } else {
                    true
                }
            } else {
                false
            };

            if result {
                returned_timeouts.push(timeout);
            }
        }

        tx.send(returned_timeouts).expect("Failed to send timeouts to client");
    }

    /// Process a decided batch
    fn process_decided_batch(&mut self, requests: Vec<ClientRqInfo>) {
        let start = Instant::now();

        requests.into_iter().for_each(|request| {
            self.pending_requests.remove(&request.digest);

            // Update so that if we later on receive the same request from the client, we can safely ignore it
            // And not get build up in the pending requests
            self.update_most_recent(&request);
        });

        metric_duration(RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID, start.elapsed());
    }

    /// Clone a set of pending requests
    fn clone_pending_requests(&self, requests: Vec<ClientRqInfo>, responder: OneShotTx<Vec<StoredRequestMessage<O>>>) {

        let mut final_rqs = Vec::with_capacity(requests.len());

        for rq_info in requests {
            if let Some(request) = self.pending_requests.get(&rq_info.digest) {
                final_rqs.push(request.clone());
            }
        }

        responder.send(final_rqs).expect("Failed to send pending requests");
    }

    /// Collect all pending requests stored in this worker
    fn collect_pending_requests(&mut self) -> Vec<StoredRequestMessage<O>> {
        std::mem::replace(&mut self.pending_requests, Default::default())
            .into_iter().map(|(_, request)| request).collect()
    }

    fn clean_client(&self, node_id: NodeId) {
        todo!()
    }

    fn stopped_requests(&mut self, requests: Vec<StoredRequestMessage<O>>) {
         requests.into_iter().for_each(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return;
            }

            let digest = request.header().unique_digest();

            self.pending_requests.insert(digest.clone(), request.clone());
        })

    }
}

pub(super) fn spawn_worker<O>(worker_id: usize, batch_tx: ChannelSyncTx<(PreProcessorOutputMessage<O>, Instant)>) -> RequestPreProcessingWorkerHandle<O>
    where O: Clone + Send + 'static {
    let (worker_tx, worker_rx) = febft_common::channel::new_bounded_sync(WORKER_QUEUE_SIZE);

    let worker = RequestPreProcessingWorker::new(worker_rx, batch_tx);

    std::thread::Builder::new()
        .name(format!("{}{}", WORKER_THREAD_NAME, worker_id))
        .spawn(move || {
            worker.run();
        }).expect("Failed to spawn worker thread");

    RequestPreProcessingWorkerHandle(worker_tx)
}

pub struct RequestPreProcessingWorkerHandle<O>(ChannelSyncTx<PreProcessorWorkMessageOuter<O>>);

impl<O> RequestPreProcessingWorkerHandle<O> {

    pub fn send(&self, message: PreProcessorWorkMessage<O>) {
        self.0.send((Instant::now(), message)).unwrap()
    }

}