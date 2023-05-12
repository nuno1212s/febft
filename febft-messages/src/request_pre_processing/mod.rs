use std::iter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx, new_bounded_sync, OneShotRx, OneShotTx};
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_communication::message::{Header, NetworkMessage, StoredMessage};
use febft_communication::Node;
use febft_execution::serialize::SharedData;
use febft_metrics::metrics::{metric_duration, metric_increment};
use crate::messages::{RequestMessage, StoredRequestMessage, SystemMessage};
use crate::metric::{RQ_PP_CLIENT_COUNT_ID, RQ_PP_CLIENT_MSG_ID, RQ_PP_COLLECT_PENDING_ID, RQ_PP_DECIDED_RQS_ID, RQ_PP_FWD_RQS_ID, RQ_PP_TIMEOUT_RQS_ID};
use crate::request_pre_processing::worker::{PreProcessorWorkMessage, RequestPreProcessingWorker};
use crate::serialize::{OrderingProtocolMessage, StateTransferMessage};
use crate::timeouts::ClientRqInfo;

mod worker;
mod work_dividers;

const ORCHESTRATOR_RCV_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1));
const PROPOSER_QUEUE_SIZE: usize = 1024;

const RQ_PRE_PROCESSING_ORCHESTRATOR: &str = "RQ-PRE-PROCESSING-ORCHESTRATOR";

/// The work partitioner is responsible for deciding which worker should process a given request
/// This should sign a contract to maintain all client sessions in the same worker, never changing
/// A session is defined by the client ID and the session ID.
///
pub trait WorkPartitioner<O> {
    /// Get the worker that should process this request
    fn get_worker_for(rq_info: &Header, message: &RequestMessage<O>, worker_count: usize) -> usize;

    /// Get the worker that should process this request
    fn get_worker_for_processed(rq_info: &ClientRqInfo, worker_count: usize) -> usize;
}

pub type RequestPreProcessor<O> = ChannelSyncTx<PreProcessorMessage<O>>;
pub type BatchOutput<O> = ChannelSyncRx<PreProcessorOutputMessage<O>>;

/// Message to the request pre processor
pub enum PreProcessorMessage<O> {
    /// We have received forwarded requests from other replicas.
    ForwardedRequests(Vec<StoredRequestMessage<O>>),
    /// Analyse timeout requests. Returns only timeouts that have not yet been executed
    TimeoutsReceived(Vec<ClientRqInfo>, OneShotTx<Vec<ClientRqInfo>>),
    /// A batch of requests that has been decided by the system
    DecidedBatch(Vec<ClientRqInfo>),
    /// Collect all pending messages from all workers.
    CollectAllPendingMessages(OneShotTx<Vec<StoredRequestMessage<O>>>),
}

/// Output messages of the preprocessor
pub enum PreProcessorOutputMessage<O> {
    /// A de duped batch of ordered requests that should be proposed
    DeDupedOrderedRequests(Vec<StoredRequestMessage<O>>),
    /// A de duped batch of unordered requests that should be proposed
    DeDupedUnorderedRequests(Vec<StoredRequestMessage<O>>),
}


struct RequestPreProcessingOrchestrator<WD, O, NT> {
    /// How many workers should we have
    thread_count: usize,
    /// Work message transmission for each worker
    work_comms: Vec<ChannelSyncTx<PreProcessorWorkMessage<O>>>,
    /// The RX end for a work channel for the request pre processor
    work_receiver: ChannelSyncRx<PreProcessorMessage<O>>,
    /// The network node so we can poll messages received from the clients
    network_node: Arc<NT>,
    /// How we are going to divide the work between workers
    work_divider: PhantomData<WD>,
}

impl<WD, D, NT> RequestPreProcessingOrchestrator<WD, D::Request, NT> where D: SharedData {
    fn run<OP, ST>(mut self) where NT: Node<SystemMessage<D, OP, ST>>,
                                   OP: OrderingProtocolMessage,
                                   ST: StateTransferMessage {
        loop {
            self.process_client_rqs::<OP, ST>();
            self.process_worker_rqs::<OP, ST>();
        }
    }

    fn process_client_rqs<OP, ST>(&mut self) where NT: Node<SystemMessage<D, OP, ST>>,
                                                   OP: OrderingProtocolMessage,
                                                   ST: StateTransferMessage,
                                                   WD: WorkPartitioner<D::Request> {

        let messages = match self.network_node.receive_from_clients(ORCHESTRATOR_RCV_TIMEOUT) {
            Ok(message) => {
                message
            }
            Err(_) => {
                return;
            }
        };

        let start = Instant::now();
        let msg_count = messages.len();

        if messages.is_empty() { return; }

        let mut worker_message = init_worker_vecs(self.thread_count, messages.len());

        let mut unordered_worker_message = init_worker_vecs(self.thread_count, messages.len());

        for message in messages {
            let NetworkMessage { header, message } = message;

            let sysmsg = message.into();

            match sysmsg {
                SystemMessage::OrderedRequest(req) => {
                    let worker = WD::get_worker_for(&header, &req, self.thread_count);

                    let stored_message = Arc::new(ReadOnly::new(StoredMessage::new(header, req)));

                    worker_message[worker % self.thread_count].push(stored_message);
                }
                SystemMessage::UnorderedRequest(req) => {
                    let worker = WD::get_worker_for(&header, &req, self.thread_count);

                    let stored_message = Arc::new(ReadOnly::new(StoredMessage::new(header, req)));

                    unordered_worker_message[worker % self.thread_count].push(stored_message);
                }
                _ => {}
            }
        }

        for (worker,
            (ordered_msgs, unordered_messages))
        in std::iter::zip(&self.work_comms, std::iter::zip(worker_message, unordered_worker_message)) {
            worker.send(PreProcessorWorkMessage::OrderedRequests(ordered_msgs)).unwrap();
            worker.send(PreProcessorWorkMessage::UnorderedRequests(unordered_messages)).unwrap();
        }

        metric_duration(RQ_PP_CLIENT_MSG_ID, start.elapsed());
        metric_increment(RQ_PP_CLIENT_COUNT_ID, Some(msg_count as u64));
    }

    fn process_work_messages(&mut self) where WD: WorkPartitioner<D::Request> {
        while let Ok(work_recved) = self.work_receiver.try_recv() {
            match work_recved {
                PreProcessorMessage::ForwardedRequests(fwd_reqs) => {
                    self.process_forwarded_rqs(fwd_reqs);
                }
                PreProcessorMessage::DecidedBatch(decided) => {
                    self.process_decided_batch(decided);
                }
                PreProcessorMessage::TimeoutsReceived(timeouts, responder) => {
                    self.process_timeouts(timeouts, responder);
                }
                PreProcessorMessage::CollectAllPendingMessages(tx) => {
                    self.collect_pending_rqs(tx);
                }
            }
        }
    }

    fn process_forwarded_rqs(&self, fwd_reqs: Vec<StoredRequestMessage<D::Request>>) {
        let start = Instant::now();

        let mut worker_message = init_worker_vecs::<StoredRequestMessage<D::Request>>(self.thread_count, fwd_reqs.len());

        for stored_msgs in fwd_reqs {
            let worker = WD::get_worker_for(stored_msgs.header(), stored_msgs.message(), self.thread_count);

            worker_message[worker % self.thread_count].push(stored_msgs);
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_message) {
            worker.send(PreProcessorWorkMessage::ForwardedRequests(messages)).unwrap();
        }

        metric_duration(RQ_PP_FWD_RQS_ID, start.elapsed());
    }

    fn process_decided_batch(&self, decided: Vec<ClientRqInfo>)
        where WD: WorkPartitioner<D::Request> {
        let start = Instant::now();

        let mut worker_messages = init_worker_vecs::<ClientRqInfo>(self.thread_count, decided.len());

        for request in decided {
            let worker = WD::get_worker_for_processed(&request, self.thread_count);

            worker_messages[worker % self.thread_count].push(request);
        }

        for (worker, messages)
        in iter::zip(&self.work_comms, worker_messages) {
            worker.send(PreProcessorWorkMessage::DecidedBatch(messages)).unwrap();
        }

        metric_duration(RQ_PP_DECIDED_RQS_ID, start.elapsed());
    }

    fn process_timeouts(&self, timeouts: Vec<ClientRqInfo>, responder: OneShotTx<Vec<ClientRqInfo>>)
        where WD: WorkPartitioner<D::Request> {
        let start = Instant::now();

        let timeout_count = timeouts.len();
        let mut worker_messages = init_worker_vecs::<ClientRqInfo>(self.thread_count, timeouts.len());

        let returners = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        for timeout in timeouts {
            let worker = WD::get_worker_for_processed(&timeout, self.thread_count);

            worker_messages[worker % self.thread_count].push(timeout);
        }

        let mut rxs = Vec::with_capacity(self.thread_count);

        for (worker,
            (messages, (tx, rx)))
        in iter::zip(&self.work_comms, iter::zip(worker_messages, returners)) {
            worker.send(PreProcessorWorkMessage::TimeoutsReceived(messages, tx)).unwrap();

            rxs.push(rx);
        }

        let mut final_requests = Vec::with_capacity(timeout_count);

        for rx in rxs {
            let rqs = rx.recv().unwrap();

            final_requests.extend(rqs);
        }

        responder.send(final_requests).unwrap();

        metric_duration(RQ_PP_TIMEOUT_RQS_ID, start.elapsed());
    }

    fn collect_pending_rqs(&self, tx: OneShotTx<Vec<StoredRequestMessage<D::Request>>>) {
        let start = Instant::now();

        let mut worker_responses = init_for_workers(self.thread_count, || channel::new_oneshot_channel());

        let rxs: Vec<OneShotRx<Vec<StoredRequestMessage<D::Request>>>> =
            worker_responses.into_iter().enumerate().map(|(worker, (tx, rx))| {
                self.work_comms[worker].send(PreProcessorWorkMessage::CollectAllPendingMessages(tx)).unwrap();

                rx
            }).collect();

        let mut final_requests = Vec::new();

        for rx in rxs {
            let mut requests = rx.recv().unwrap();

            final_requests.append(&mut requests);
        }

        tx.send(final_requests).unwrap();

        metric_duration(RQ_PP_COLLECT_PENDING_ID, start.elapsed());
    }
}

pub fn initialize_request_pre_processor<WD, O, NT>(concurrency: usize, node: Arc<NT>) -> RequestPreProcessor<O> {
    let (batch_tx, receiver) = new_bounded_sync(PROPOSER_QUEUE_SIZE);

    let (work_sender, work_rcvr) = new_bounded_sync(PROPOSER_QUEUE_SIZE);

    let mut work_comms = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let worker_handle = worker::spawn_worker(worker_id, batch_tx.clone());

        work_comms.push(worker_handle);
    }

    let orchestrator = RequestPreProcessingOrchestrator {
        thread_count: concurrency,
        work_comms,
        work_receiver: work_rcvr,
        network_node: node,
        work_divider: Default::default(),
    };

    launch_orchestrator_thread(orchestrator);

    work_sender
}

fn init_for_workers<V>(thread_count: usize, init: fn() -> V) -> Vec<V> {
    let mut worker_message: Vec<V> =
        std::iter::repeat_with(init)
            .take(thread_count)
            .collect();

    worker_message
}

fn init_worker_vecs<O>(thread_count: usize, message_count: usize) -> Vec<Vec<O>> {
    let message_count = message_count / thread_count;

    let workers = init_for_workers(thread_count, || Vec::with_capacity(message_count));

    workers
}

fn launch_orchestrator_thread<WD, O, NT, >(orchestrator: RequestPreProcessingOrchestrator<WD, O, NT>) {
    std::thread::Builder::new()
        .name(format!(RQ_PRE_PROCESSING_ORCHESTRATOR))
        .spawn(move || {
            orchestrator.run();
        }).expect("Failed to launch orchestrator thread.");
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    operation_key_raw(header.from(), message.session_id())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
