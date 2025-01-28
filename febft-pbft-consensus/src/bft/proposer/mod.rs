use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

use atlas_common::channel::TryRecvError;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::serialization_helper::SerMsg;
use atlas_communication::message::StoredMessage;
use atlas_core::messages::{create_rq_correlation_id, ClientRqInfo, SessionBased};
use atlas_core::metric::{RQ_BATCH_TRACKING_ID, RQ_CLIENT_TRACKING_ID};
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_core::request_pre_processing::{BatchOutput, PreProcessorOutputMessage};
use atlas_core::timeouts::timeout::TimeoutModHandle;
use atlas_metrics::metrics::{
    metric_correlation_id_passed, metric_duration, metric_increment,
    metric_initialize_correlation_id, metric_store_count,
};

use crate::bft::config::ProposerConfig;
use crate::bft::consensus::ProposerConsensusGuard;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::metric::{
    CLIENT_POOL_BATCH_SIZE_ID, ENTERED_PRE_PROPOSER, PROPOSER_BATCHES_MADE_ID, PROPOSER_LATENCY_ID,
    PROPOSER_PROPOSE_TIME_ID, PROPOSER_REQUESTS_COLLECTED_ID, PROPOSER_REQUEST_PROCESSING_TIME_ID,
    PROPOSER_REQUEST_TIME_ITERATIONS_ID,
};
use crate::bft::sync::view::{is_request_in_hash_space, ViewInfo};
use crate::bft::PBFT;

use super::sync::{AbstractSynchronizer, Synchronizer};

//pub mod follower_proposer;

pub type BatchType<R> = Vec<StoredMessage<R>>;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct Proposer<RQ, NT>
where
    RQ: SerMsg,
{
    /// Channel for the reception of batches from the pre processing module
    batch_reception: BatchOutput<RQ>,
    /// Network Node
    node_ref: Arc<NT>,
    synchronizer: Arc<Synchronizer<RQ>>,
    timeouts: TimeoutModHandle,
    consensus_guard: Arc<ProposerConsensusGuard>,
    // Should we shut down?
    cancelled: AtomicBool,

    thread_pool: Option<ThreadPool>,

    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,
    max_batch_size: usize,
}

struct ProposeBuilder<RQ>
where
    RQ: SerMsg,
{
    currently_accumulated: Vec<StoredMessage<RQ>>,
    last_proposal: Instant,
}

impl<RQ> ProposeBuilder<RQ>
where
    RQ: SerMsg,
{
    pub fn new(target_size: usize) -> Self {
        Self {
            currently_accumulated: Vec::with_capacity(target_size),
            last_proposal: Instant::now(),
        }
    }
}

impl<RQ, NT> Proposer<RQ, NT>
where
    RQ: SerMsg + SessionBased,
{
    pub fn new(
        node: Arc<NT>,
        batch_input: BatchOutput<RQ>,
        sync: Arc<Synchronizer<RQ>>,
        timeouts: TimeoutModHandle,
        consensus_guard: Arc<ProposerConsensusGuard>,
        proposer_config: ProposerConfig,
    ) -> Arc<Self> {
        let ProposerConfig {
            target_batch_size,
            max_batch_size,
            batch_timeout,
            processing_threads,
        } = proposer_config;

        let thread_pool = if processing_threads > 1 {
            Some(
                ThreadPoolBuilder::default()
                    .num_threads(processing_threads as usize)
                    .build()
                    .expect("Failed to build proposer thread pool"),
            )
        } else {
            None
        };

        info!(
            "Proposer configuration: target_batch_size: {}, max_batch_size: {}, batch_timeout: {}, processing_threads: {}",
            target_batch_size,
            max_batch_size,
            batch_timeout,
            processing_threads
        );

        Arc::new(Self {
            batch_reception: batch_input,
            node_ref: node,
            synchronizer: sync,
            timeouts,
            cancelled: AtomicBool::new(false),
            consensus_guard,
            target_global_batch_size: target_batch_size as usize,
            global_batch_time_limit: batch_timeout as u128,
            max_batch_size: max_batch_size as usize,
            thread_pool,
        })
    }

    fn process_request_message_leader(
        &self,
        message: StoredMessage<RQ>,
        leader_set_size: usize,
        our_slice: Option<&(Vec<u8>, Vec<u8>)>,
    ) -> Option<StoredMessage<RQ>> {
        let digest = message.header().unique_digest();

        /*metric_correlation_id_passed(RQ_CLIENT_TRACKING_ID,
        create_rq_correlation_id(message.header().from(), message.message()),
        ENTERED_PRE_PROPOSER.clone());*/

        if leader_set_size > 1 {
            if is_request_in_hash_space(&digest, our_slice.as_ref().unwrap()) {
                // we know that these operations will always be proposed since we are a
                // Correct replica. We can therefore just add them to the latest op log
                Some(message)
            } else {
                None
            }
        } else {
            // we know that these operations will always be proposed since we are a
            // Correct replica. We can therefore just add them to the latest op log
            Some(message)
        }
    }

    fn process_request_message_non_leader(&self, message: StoredMessage<RQ>) -> ClientRqInfo {
        let digest = message.header().unique_digest();

        /*metric_correlation_id_passed(RQ_CLIENT_TRACKING_ID,
        create_rq_correlation_id(message.header().from(), message.message()),
        ENTERED_PRE_PROPOSER.clone());*/

        ClientRqInfo::new(
            digest,
            message.header().from(),
            message.message().sequence_number(),
            message.message().session_number(),
        )
    }

    fn process_received_messages(
        &self,
        view_info: ViewInfo,
        messages: Vec<StoredMessage<RQ>>,
        propose_builder: &mut ProposeBuilder<RQ>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        let is_leader = view_info.leader_set().contains(&self.node_ref.id());

        let leader_set_size = view_info.leader_set().len();

        let our_slice = view_info
            .hash_space_division()
            .get(&self.node_ref.id())
            .cloned()
            .clone();

        if is_leader {
            let mut messages = if let Some(thread_pool) = self.thread_pool.as_ref() {
                thread_pool.install(|| {
                    messages
                        .into_par_iter()
                        .filter_map(|message| {
                            self.process_request_message_leader(
                                message,
                                leader_set_size,
                                our_slice.as_ref(),
                            )
                        })
                        .collect()
                })
            } else {
                messages
                    .into_iter()
                    .filter_map(|message| {
                        self.process_request_message_leader(
                            message,
                            leader_set_size,
                            our_slice.as_ref(),
                        )
                    })
                    .collect()
            };

            propose_builder.currently_accumulated.append(&mut messages);
        } else {
            let digest_vec = if let Some(thread_pool) = self.thread_pool.as_ref() {
                thread_pool.install(|| {
                    messages
                        .into_par_iter()
                        .map(|message| self.process_request_message_non_leader(message))
                        .collect::<Vec<_>>()
                })
            } else {
                messages
                    .into_iter()
                    .map(|message| self.process_request_message_non_leader(message))
                    .collect::<Vec<_>>()
            };

            if !digest_vec.is_empty() {
                self.synchronizer
                    .watch_received_requests(digest_vec, &self.timeouts);
            }
        }
    }

    ///Start this work
    pub fn start(self: Arc<Self>) -> JoinHandle<()>
    where
        RQ: 'static,
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        std::thread::Builder::new()
            .name("Proposer thread".to_string())
            .spawn(move || {

                //The currently accumulated requests, accumulated while we wait for the next batch to propose
                let mut ordered_propose = ProposeBuilder::new(self.target_global_batch_size);

                loop {
                    if self.cancelled.load(Ordering::Relaxed) {
                        warn!("Cancelling proposer thread");
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
                    let mut collected_requests: Option<PreProcessorOutputMessage<RQ>> = None;

                    let info = self.synchronizer.view();

                    let is_leader = info.leader_set().contains(&self.node_ref.id());


                    if is_leader {
                        let time_until_next_propose = Duration::from_micros(self.get_time_until_next_propose(&ordered_propose) as u64);

                        while let Ok(mut message_batch) = self.batch_reception.recv_timeout(time_until_next_propose) {
                            if self.handle_received_message(&mut ordered_propose, &mut collected_requests, is_leader, message_batch) { break; }
                        }
                    } else {
                        while let Ok(mut message_batch) = self.batch_reception.recv() {
                            if self.handle_received_message(&mut ordered_propose, &mut collected_requests, is_leader, message_batch) { break; }
                        }
                    }
                    
                    let discovered_requests = if let Some(messages) = collected_requests {
                        metric_increment(PROPOSER_REQUESTS_COLLECTED_ID, Some(messages.len() as u64));

                        let start_time = Instant::now();

                        let counter = messages.len();

                        self.process_received_messages(info.clone(), messages.into(), &mut ordered_propose);

                        if counter > 0 {
                            metric_duration(PROPOSER_REQUEST_PROCESSING_TIME_ID, start_time.elapsed());
                            metric_increment(PROPOSER_REQUEST_TIME_ITERATIONS_ID, Some(1));
                        }

                        true
                    } else {
                        false
                    };

                    let start = Instant::now();

                    let ordered = self.propose_ordered(is_leader, &mut ordered_propose);

                    if ordered {
                        metric_duration(PROPOSER_PROPOSE_TIME_ID, start.elapsed());
                    }

                    if !discovered_requests {
                        self.sleep_for_appropriate_amount_of_time(&ordered_propose);
                    }
                }
            }).expect("Failed to launch proposer thread.")
    }

    fn handle_received_message(
        &self,
        mut ordered_propose: &mut ProposeBuilder<RQ>,
        mut collected_requests: &mut Option<PreProcessorOutputMessage<RQ>>,
        is_leader: bool,
        mut message_batch: PreProcessorOutputMessage<RQ>,
    ) -> bool {
        metric_store_count(CLIENT_POOL_BATCH_SIZE_ID, message_batch.len());

        if let Some(ref mut requests) = &mut collected_requests {
            requests.append(&mut message_batch);
        } else {
            *collected_requests = Some(message_batch);
        }

        let collected_request_count = collected_requests
            .as_ref()
            .map(|requests| requests.len())
            .unwrap_or(0);

        let micros_since_last_batch = ordered_propose.last_proposal.elapsed().as_micros();

        if is_leader
            && (collected_request_count >= self.target_global_batch_size
                || micros_since_last_batch >= self.global_batch_time_limit)
        {
            return true;
        }

        false
    }

    fn get_time_until_next_propose(&self, propose: &ProposeBuilder<RQ>) -> u128 {
        let time_until_next_proposal = std::cmp::min(
            self.global_batch_time_limit / 2,
            self.global_batch_time_limit - propose.last_proposal.elapsed().as_micros(),
        );

        time_until_next_proposal
    }

    /// attempt to propose the ordered requests that we have collected
    /// Returns true if a batch was proposed
    #[instrument(skip(self), level = "DEBUG")]
    fn propose_ordered(&self, is_leader: bool, propose: &mut ProposeBuilder<RQ>) -> bool
    where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
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

            let last_proposed_batch = propose.last_proposal;

            if self.consensus_guard.can_propose() {
                if let Some((seq, view)) = self.consensus_guard.next_seq_no() {
                    propose.last_proposal = Instant::now();

                    let next_batch = if propose.currently_accumulated.len() > self.max_batch_size {
                        //This now contains target_global_size requests. We want this to be our next batch
                        //Currently accumulated contains the remaining messages to be sent in the next batch
                        let mut next_batch = propose
                            .currently_accumulated
                            .split_off(propose.currently_accumulated.len() - self.max_batch_size);

                        //So we swap that memory with the other vector memory and we have it!
                        std::mem::swap(&mut next_batch, &mut propose.currently_accumulated);

                        Some(next_batch)
                    } else {
                        None
                    };

                    let current_batch = std::mem::replace(
                        &mut propose.currently_accumulated,
                        next_batch.unwrap_or_else(Vec::new),
                    );

                    self.propose(seq, &view, current_batch);

                    metric_duration(PROPOSER_LATENCY_ID, last_proposed_batch.elapsed());

                    return true;
                }
            }
        }

        false
    }

    /// Proposes a new batch.
    /// (Basically broadcasts it to all of the members)
    #[instrument(skip(self, currently_accumulated), level = "DEBUG")]
    fn propose(
        &self,
        seq: SeqNo,
        view: &ViewInfo,
        mut currently_accumulated: Vec<StoredMessage<RQ>>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
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
                    trace!(
                        "{:?} // Request {:?} has already been proposed, not retaining it",
                        info1,
                        self.node_ref.id()
                    );
                    return false;
                }
                true
            });

            view_change_msg.is_some_and(|msg| msg.as_ref().is_some_and(|msg| msg.is_empty()))
        };

        if is_view_change_empty {
            info!(
                "{:?} // View change messages have been processed, clearing them",
                self.node_ref.id()
            );
            // The messages are clear, we no longer need to keep checking them
            self.consensus_guard.sync_messages_clear();
        }

        let targets = view.quorum_members().clone();

        info!(
            "{:?} // Proposing new batch with {} request count {:?} to quorum: {:?}",
            self.node_ref.id(),
            currently_accumulated.len(),
            seq,
            targets
        );

        let message = PBFTMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::PrePrepare(currently_accumulated),
        ));

        let _ = self.node_ref.broadcast_signed(message, targets.into_iter());

        metric_initialize_correlation_id(
            RQ_BATCH_TRACKING_ID,
            seq.into_u32().to_string().as_str(),
            ENTERED_PRE_PROPOSER.clone(),
        );

        metric_increment(PROPOSER_BATCHES_MADE_ID, Some(1));
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if the given request has already appeared in a view change message
    /// Returns true if it has been seen previously (should not be proposed)
    /// Returns false if not
    #[instrument(skip_all, level = "DEBUG")]
    fn check_if_has_been_proposed(
        &self,
        req: &StoredMessage<RQ>,
        mutex_guard: &mut Option<MutexGuard<Option<BTreeMap<NodeId, BTreeMap<SeqNo, SeqNo>>>>>,
    ) -> bool {
        if let Some(mutex_guard) = mutex_guard {
            if let Some(seen_rqs) = (*mutex_guard).as_mut() {
                let result = if seen_rqs.contains_key(&req.header().from()) {
                    // Check if we have the corresponding session id in the map get the corresponding seq no, and check if the req.message().sequence_number() is smaller or equal to it
                    //If so, we remove the value from the map and return true
                    let (result, should_remove) = {
                        let session_map = seen_rqs.get_mut(&req.header().from()).unwrap();

                        let res = if let Some(seq_no) =
                            session_map.get(&req.message().session_number()).cloned()
                        {
                            if req.message().sequence_number() > seq_no {
                                // We have now seen a more recent sequence number for that session, so a previous
                                // Operation will never be passed along again (due to the request pre processor)
                                session_map.remove(&req.message().session_number());

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

    /// Sleep for a given small amount of time, relative to how long we still
    /// have until the next planned batch response
    fn sleep_for_appropriate_amount_of_time(&self, last_proposed: &ProposeBuilder<RQ>) {
        let time_until_next_proposal = std::cmp::min(
            self.global_batch_time_limit / 2,
            self.global_batch_time_limit - last_proposed.last_proposal.elapsed().as_micros(),
        );

        let sleep_duration = Duration::from_micros((time_until_next_proposal / 2) as u64);

        std::thread::sleep(sleep_duration);
    }
}

impl<RQ> Debug for ProposeBuilder<RQ>
where
    RQ: SerMsg,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProposeBuilder")
            .field("last_proposal", &self.last_proposal)
            .finish()
    }
}
