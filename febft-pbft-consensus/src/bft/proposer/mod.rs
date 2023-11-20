use std::collections::BTreeMap;
use std::sync::{Arc, MutexGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use log::{debug, error, info, warn};

use atlas_common::channel::TryRecvError;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::threadpool;
use atlas_core::messages::{ClientRqInfo, StoredRequestMessage};
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_core::request_pre_processing::{BatchOutput, PreProcessorOutputMessage};
use atlas_core::timeouts::Timeouts;
use atlas_smr_application::app::UnorderedBatch;
use atlas_smr_application::ExecutorHandle;
use atlas_smr_application::serialize::ApplicationData;
use atlas_metrics::metrics::{metric_duration, metric_increment, metric_store_count};

use crate::bft::config::ProposerConfig;
use crate::bft::consensus::ProposerConsensusGuard;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::metric::{CLIENT_POOL_BATCH_SIZE_ID, PROPOSER_BATCHES_MADE_ID, PROPOSER_LATENCY_ID, PROPOSER_PROPOSE_TIME_ID, PROPOSER_REQUEST_PROCESSING_TIME_ID, PROPOSER_REQUEST_TIME_ITERATIONS_ID, PROPOSER_REQUESTS_COLLECTED_ID};
use crate::bft::PBFT;
use crate::bft::sync::view::{is_request_in_hash_space, ViewInfo};

use super::sync::{AbstractSynchronizer, Synchronizer};

//pub mod follower_proposer;

pub type BatchType<R> = Vec<StoredRequestMessage<R>>;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct Proposer<D, NT>
    where D: ApplicationData + 'static, {
    /// Channel for the reception of batches from the pre processing module
    batch_reception: BatchOutput<D::Request>,
    /// Network Node
    node_ref: Arc<NT>,
    synchronizer: Arc<Synchronizer<D>>,
    timeouts: Timeouts,
    consensus_guard: Arc<ProposerConsensusGuard>,
    // Should we shut down?
    cancelled: AtomicBool,

    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,
    max_batch_size: usize,

    //For unordered request execution
    executor_handle: ExecutorHandle<D>,

}

const TIMEOUT: Duration = Duration::from_micros(10);
const PRINT_INTERVAL: usize = 10000;

struct ProposeBuilder<D> where D: ApplicationData {
    currently_accumulated: Vec<StoredRequestMessage<D::Request>>,
    last_proposal: Instant,
}

impl<D> ProposeBuilder<D> where D: ApplicationData {
    pub fn new(target_size: usize) -> Self {
        Self { currently_accumulated: Vec::with_capacity(target_size), last_proposal: Instant::now() }
    }
}

///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<D, NT> Proposer<D, NT>
    where D: ApplicationData + 'static, {
    pub fn new(
        node: Arc<NT>,
        batch_input: BatchOutput<D::Request>,
        sync: Arc<Synchronizer<D>>,
        timeouts: Timeouts,
        executor_handle: ExecutorHandle<D>,
        consensus_guard: Arc<ProposerConsensusGuard>,
        proposer_config: ProposerConfig,
    ) -> Arc<Self> {
        let ProposerConfig {
            target_batch_size, max_batch_size, batch_timeout
        } = proposer_config;

        Arc::new(Self {
            batch_reception: batch_input,
            node_ref: node,
            synchronizer: sync,
            timeouts,
            cancelled: AtomicBool::new(false),
            consensus_guard,
            target_global_batch_size: target_batch_size as usize,
            global_batch_time_limit: batch_timeout as u128,
            executor_handle,
            max_batch_size: max_batch_size as usize,
        })
    }

    ///Start this work
    pub fn start(self: Arc<Self>) -> JoinHandle<()>
        where NT: OrderProtocolSendNode<D, PBFT<D>> + 'static {
        std::thread::Builder::new()
            .name(format!("Proposer thread"))
            .spawn(move || {

                //The currently accumulated requests, accumulated while we wait for the next batch to propose
                let mut ordered_propose = ProposeBuilder::new(self.target_global_batch_size);

                let mut unordered_propose = ProposeBuilder::new(self.target_global_batch_size);

                loop {
                    if self.cancelled.load(Ordering::Relaxed) {
                        break;
                    }

                    //We only want to produce batches to the channel if we are the leader, as
                    //Only the leader will propose thing

                    // Are we able to currently propose a request?
                    while !self.consensus_guard.can_propose() {
                        // Wait until we are able to propose a new block
                        // Use a while to guard against spurious wake ups
                        warn!("{:?} // Stopping proposer as we are currently not able to propose anything.", self.node_ref.id());

                        self.consensus_guard.block_until_ready();

                        info!("{:?} // Resuming proposer as we are now able to propose again.", self.node_ref.id());
                    }

                    //We do this as we don't want to get stuck waiting for requests that might never arrive
                    //Or even just waiting for any type of request. We want to minimize the amount of time the
                    //Consensus is waiting for new requests

                    //We don't need to do this for non leader replicas, as that would cause unnecessary strain as the
                    //Thread is in an infinite loop
                    // Receive the requests from the clients and process them
                    let opt_msgs: Option<PreProcessorOutputMessage<D::Request>> = match self.batch_reception.try_recv() {
                        Ok(res) => { Some(res) }
                        Err(err) => {
                            match err {
                                TryRecvError::ChannelDc => {
                                    error!("{:?} // Failed to receive requests from pre processing module because {:?}", self.node_ref.id(), err);
                                    break;
                                }
                                _ => {
                                    None
                                }
                            }
                        }
                    };

                    //TODO: Maybe not use this as it can spam the lock on synchronizer?
                    let info = self.synchronizer.view();

                    let is_leader = info.leader_set().contains(&self.node_ref.id());

                    let leader_set_size = info.leader_set().len();

                    let our_slice = info.hash_space_division()
                        .get(&self.node_ref.id()).cloned().clone();

                    let discovered_requests;

                    if let Some(messages) = opt_msgs {
                        metric_increment(PROPOSER_REQUESTS_COLLECTED_ID, Some(messages.len() as u64));
                        metric_store_count(CLIENT_POOL_BATCH_SIZE_ID, messages.len());

                        let start_time = Instant::now();

                        let mut digest_vec = Vec::with_capacity(messages.len());
                        let mut counter = messages.len();

                        match messages {
                            PreProcessorOutputMessage::DeDupedOrderedRequests(messages) => {
                                for message in messages {
                                    let digest = message.header().unique_digest();

                                    if is_leader {
                                        if leader_set_size > 1 {
                                            if is_request_in_hash_space(&digest, our_slice.as_ref().unwrap()) {
                                                // we know that these operations will always be proposed since we are a
                                                // Correct replica. We can therefore just add them to the latest op log
                                                ordered_propose.currently_accumulated.push(message);
                                            }
                                        } else {
                                            // we know that these operations will always be proposed since we are a
                                            // Correct replica. We can therefore just add them to the latest op log
                                            ordered_propose.currently_accumulated.push(message);
                                        }
                                    } else {
                                        digest_vec.push(ClientRqInfo::new(digest, message.header().from(), message.message().sequence_number(), message.message().session_id()));
                                    }
                                }
                            }
                            PreProcessorOutputMessage::DeDupedUnorderedRequests(mut messages) => {
                                unordered_propose.currently_accumulated.append(&mut messages);
                            }
                        }

                        if !digest_vec.is_empty() {
                            self.synchronizer.watch_received_requests(digest_vec, &self.timeouts);
                        }

                        if counter > 0 {
                            metric_duration(PROPOSER_REQUEST_PROCESSING_TIME_ID, start_time.elapsed());
                            metric_increment(PROPOSER_REQUEST_TIME_ITERATIONS_ID, Some(1));
                        }

                        discovered_requests = true;
                    } else {
                        discovered_requests = false;
                    }

                    let start = Instant::now();

                    //Lets first deal with unordered requests since it should be much quicker and easier
                    let unordered = self.propose_unordered(&mut unordered_propose);

                    let ordered = self.propose_ordered(is_leader, &mut ordered_propose);

                    if unordered || ordered {
                        metric_duration(PROPOSER_PROPOSE_TIME_ID, start.elapsed());
                    }

                    if !discovered_requests {
                        //Yield to prevent active waiting
                        std::thread::yield_now();
                    }
                }
            }).unwrap()
    }

    /// Attempt to propose an unordered request batch
    /// Fails if the batch is not large enough or the timeout
    /// Has not yet occurred
    fn propose_unordered(
        &self,
        propose: &mut ProposeBuilder<D>,
    ) -> bool where NT: OrderProtocolSendNode<D, PBFT<D>> {
        if !propose.currently_accumulated.is_empty() {
            let current_batch_size = propose.currently_accumulated.len();

            let should_exec = if current_batch_size < self.target_global_batch_size {
                let micros_since_last_batch = propose.last_proposal.elapsed()
                    .as_micros();

                if micros_since_last_batch <= self.global_batch_time_limit {
                    //Don't execute yet since we don't have the size and haven't timed
                    //out
                    false
                } else {
                    //We have timed out, execute the pending requests
                    true
                }
            } else {
                true
            };

            if should_exec {

                // Swap in the latest time at which a batch was executed
                let last_unordered_batch =
                    std::mem::replace(&mut propose.last_proposal, Instant::now());
                //swap in the new vec and take the previous one to the threadpool
                let new_accumulated_vec =
                    std::mem::replace(&mut propose.currently_accumulated,
                                      Vec::new());

                let executor_handle = self.executor_handle.clone();

                let node_id = self.node_ref.id();

                threadpool::execute(move || {
                    let mut unordered_batch =
                        UnorderedBatch::new_with_cap(new_accumulated_vec.len());

                    for request in new_accumulated_vec {
                        let (header, message) = request.into_inner();

                        unordered_batch.add(
                            header.from(),
                            message.session_id(),
                            message.sequence_number(),
                            message.into_inner_operation(),
                        );
                    }

                    if let Err(err) = executor_handle.queue_update_unordered(unordered_batch) {
                        error!(
                            "Error while proposing unordered batch of requests: {:?}",
                            err
                        );
                    }
                });

                return true;
            }
        }

        return false;
    }

    /// attempt to propose the ordered requests that we have collected
    /// Returns true if a batch was proposed
    fn propose_ordered(&self, is_leader: bool,
                       propose: &mut ProposeBuilder<D>, ) -> bool
        where NT: OrderProtocolSendNode<D, PBFT<D>> {

        //Now let's deal with ordered requests
        if is_leader {
            let current_batch_size = propose.currently_accumulated.len();

            if current_batch_size < self.target_global_batch_size {
                let micros_since_last_batch = propose.last_proposal.elapsed().as_micros();

                if micros_since_last_batch <= self.global_batch_time_limit {
                    //Batch isn't large enough and time hasn't passed, don't even attempt to propose
                    return false;
                }
            }

            let last_proposed_batch = propose.last_proposal.clone();

            if self.consensus_guard.can_propose() {
                if let Some((seq, view)) = self.consensus_guard.next_seq_no() {
                    propose.last_proposal = Instant::now();

                    let next_batch = if propose.currently_accumulated.len() > self.max_batch_size {

                        //This now contains target_global_size requests. We want this to be our next batch
                        //Currently accumulated contains the remaining messages to be sent in the next batch
                        let mut next_batch = propose.currently_accumulated.split_off(propose.currently_accumulated.len() - self.max_batch_size);

                        //So we swap that memory with the other vector memory and we have it!
                        std::mem::swap(&mut next_batch, &mut propose.currently_accumulated);

                        Some(next_batch)
                    } else {
                        None
                    };

                    let current_batch = std::mem::replace(&mut propose.currently_accumulated,
                                                          next_batch.unwrap_or_else(|| Vec::new()));

                    self.propose(seq, &view, current_batch);

                    metric_duration(PROPOSER_LATENCY_ID, last_proposed_batch.elapsed());

                    return true;
                }
            }
        }

        return false;
    }

    /// Proposes a new batch.
    /// (Basically broadcasts it to all of the members)
    fn propose(
        &self,
        seq: SeqNo,
        view: &ViewInfo,
        mut currently_accumulated: Vec<StoredRequestMessage<D::Request>>,
    ) where NT: OrderProtocolSendNode<D, PBFT<D>> {
        let has_pending_messages = self.consensus_guard.has_pending_view_change_reqs();

        let is_view_change_empty = {

            // Introduce a new scope for the view change lock
            let mut view_change_msg = if has_pending_messages {
                Some(self.consensus_guard.last_view_change().lock().unwrap())
            } else {
                None
            };

            currently_accumulated.retain(|msg| {
                if self.check_if_has_been_proposed(msg, &mut view_change_msg) {
                    // if it has been proposed, then we do not want to retain it

                    let info1 = ClientRqInfo::from(msg);
                    debug!("{:?} // Request {:?} has already been proposed, not retaining it", info1, self.node_ref.id());
                    return false;
                }
                true
            });

            view_change_msg.is_some_and(|msg| msg.as_ref().is_some_and(|msg| msg.is_empty()))
        };

        if is_view_change_empty {
            info!("{:?} // View change messages have been processed, clearing them", self.node_ref.id());
            // The messages are clear, we no longer need to keep checking them
            self.consensus_guard.sync_messages_clear();
        }

        let targets = view.quorum_members().clone();

        info!("{:?} // Proposing new batch with {} request count {:?} to quorum: {:?}", self.node_ref.id(), currently_accumulated.len(), seq, targets);

        let message = PBFTMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::PrePrepare(currently_accumulated),
        ));

        self.node_ref.broadcast_signed(message, targets.into_iter());

        metric_increment(PROPOSER_BATCHES_MADE_ID, Some(1));
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if the given request has already appeared in a view change message
    /// Returns true if it has been seen previously (should not be proposed)
    /// Returns false if not
    fn check_if_has_been_proposed(&self, req: &StoredRequestMessage<D::Request>, mutex_guard: &mut Option<MutexGuard<Option<BTreeMap<NodeId, BTreeMap<SeqNo, SeqNo>>>>>) -> bool {
        if let Some(mutex_guard) = mutex_guard {
            if let Some(seen_rqs) = (*mutex_guard).as_mut() {
                let result = if seen_rqs.contains_key(&req.header().from()) {

                    // Check if we have the corresponding session id in the map get the corresponding seq no, and check if the req.message().sequence_number() is smaller or equal to it
                    //If so, we remove the value from the map and return true
                    let (result, should_remove) = {
                        let session_map = seen_rqs.get_mut(&req.header().from()).unwrap();

                        let res = if let Some(seq_no) = session_map.get(&req.message().session_id()).cloned() {
                            if req.message().sequence_number() > seq_no {
                                // We have now seen a more recent sequence number for that session, so a previous
                                // Operation will never be passed along again (due to the request pre processor)
                                session_map.remove(&req.message().session_id());

                                (false, session_map.is_empty())
                            } else {
                                // We have already seen a more recent message
                                (true, false)
                            }
                        } else {
                            (false, false)
                        };

                        res
                    };

                    if should_remove {
                        seen_rqs.remove(&req.header().from());
                    }

                    result
                } else {
                    // We have never seen this client, so it's fine
                    false
                };

                return result;
            }
        }

        false
    }
}
