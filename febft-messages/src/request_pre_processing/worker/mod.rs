use std::sync::Arc;
use std::time::Instant;
use intmap::IntMap;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx, OneShotTx};
use febft_common::collections::HashMap;
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_execution::serialize::SharedData;
use crate::messages::{ClientRqInfo, RequestMessage, StoredRequestMessage};
use crate::request_pre_processing::{operation_key, operation_key_raw, PreProcessorOutputMessage};
use crate::timeouts::{RqTimeout, TimeoutKind, TimeoutPhase};

const WORKER_QUEUE_SIZE: usize = 124;
const WORKER_THREAD_NAME: &str = "RQ-PRE-PROCESSING-WORKER-{}";

pub enum PreProcessorWorkMessage<O> {
    /// We have received requests from the clients, which need
    /// to be processed
    ClientPoolOrderedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Client pool requests received
    ClientPoolUnorderedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Received requests that were forwarded from other replicas
    ForwardedRequestsReceived(Vec<StoredRequestMessage<O>>),
    /// Analyse timeout requests. Returns only timeouts that have not yet been executed
    TimeoutsReceived(Vec<RqTimeout>, OneShotTx<Vec<RqTimeout>>),
    /// A batch of requests has been decided by the system
    DecidedBatch(Vec<ClientRqInfo>),
    /// Collect all pending messages from the given worker
    CollectPendingMessages(OneShotTx<Vec<StoredRequestMessage<O>>>),
    /// Remove all requests associated with this client (due to a disconnection, for example)
    CleanClient(NodeId),
}

/// Each worker will be assigned a given set of clients
pub struct RequestPreProcessingWorker<O> {
    /// Receive work
    message_rx: ChannelSyncRx<PreProcessorWorkMessage<O>>,

    /// Output for the requests that have been processed and should now be proposed
    batch_production: ChannelSyncTx<PreProcessorOutputMessage<O>>,

    /// The latest operations seen by this worker.
    /// Since a given session will always be handled by the same worker,
    /// we can use this to filter out duplicates.
    latest_ops: IntMap<SeqNo>,
    /// The requests that have not been added to a batch yet.
    pending_requests: HashMap<Digest, StoredRequestMessage<O>>,
}


impl<O> RequestPreProcessingWorker<O> where O: Clone {
    pub fn new(message_rx: ChannelSyncRx<PreProcessorWorkMessage<O>>, batch_production: ChannelSyncTx<PreProcessorOutputMessage<O>>) -> Self {
        Self {
            message_rx,
            batch_production,
            latest_ops: Default::default(),
            pending_requests: Default::default(),
        }
    }

    pub(crate) fn run(mut self) {
        loop {
            match self.message_rx.recv().unwrap() {
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
                PreProcessorWorkMessage::CleanClient(client) => {
                    self.clean_client(client);
                }
            }
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

    /// Process the ordered client pool requests
    fn process_ordered_client_pool_requests(&mut self, requests: Vec<StoredRequestMessage<O>>) {
        let requests = requests.into_iter().filter(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return true;
            }

            let digest = request.header().unique_digest();

            self.pending_requests.insert(digest.clone(), request.clone());

            return false;
        }).collect();

        self.batch_production.send(PreProcessorOutputMessage::DeDupedOrderedRequests(requests)).expect("Failed to send batch to proposer");
    }

    /// Process the unordered client pool requests
    fn process_unordered_client_pool_rqs(&mut self, requests: Vec<StoredRequestMessage<O>>) {
        let requests = requests.into_iter().filter(|request| {
            if self.has_received_more_recent_and_update(request.header(), request.message()) {
                return true;
            }

            return false;
        }).collect();

        self.batch_production.send(PreProcessorOutputMessage::DeDupedUnorderedRequests(requests)).expect("Failed to send batch to proposer");
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

        self.batch_production.send(PreProcessorOutputMessage::DeDupedOrderedRequests(requests)).expect("Failed to send batch to proposer");
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
        requests.into_iter().for_each(|request| {
            self.pending_requests.remove(&request.digest);
        });
    }

    /// Collect all pending requests stored in this worker
    fn collect_pending_requests(&mut self) -> Vec<StoredRequestMessage<O>> {
        std::mem::replace(&mut self.pending_requests, Default::default())
            .into_iter().map(|(_, request)| request).collect()
    }
    
    fn clean_client(&self, node_id: NodeId) {
        todo!()
    }
}

pub(super) fn spawn_worker<O>(worker_id: usize, batch_tx: ChannelSyncTx<PreProcessorOutputMessage<O>>) -> ChannelSyncTx<PreProcessorWorkMessage<O>>
    where O: Clone + Send + 'static {
    let (worker_tx, worker_rx) = febft_common::channel::new_bounded_sync(WORKER_QUEUE_SIZE);

    let worker = RequestPreProcessingWorker::new(worker_rx, batch_tx);

    std::thread::Builder::new()
        .name(format!("{}{}", WORKER_THREAD_NAME, worker_id))
        .spawn(move || {
            worker.run();
        }).expect("Failed to spawn worker thread");

    worker_tx
}
