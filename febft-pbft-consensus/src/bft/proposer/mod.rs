pub mod follower_proposer;

use std::cmp::max;
use std::marker::PhantomData;
use log::{error, warn, debug, info, trace};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use febft_common::ordering::{Orderable, SeqNo};
use febft_common::threadpool;
use febft_communication::message::{NetworkMessage, NetworkMessageKind, StoredMessage};
use febft_communication::Node;
use febft_execution::app::{Request, Service, UnorderedBatch};
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::messages::{RequestMessage, SystemMessage};
use febft_messages::serialize::StateTransferMessage;
use febft_messages::timeouts::Timeouts;
use crate::bft::config::ProposerConfig;
use crate::bft::consensus::ProposerConsensusGuard;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, ObserverMessage, PBFTMessage};
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::observer::{ConnState, MessageType, ObserverHandle};
use crate::bft::PBFT;
use crate::bft::sync::view::{is_request_in_hash_space, ViewInfo};
use super::sync::{Synchronizer, AbstractSynchronizer};

pub type BatchType<R> = Vec<StoredMessage<RequestMessage<R>>>;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct Proposer<D, NT>
    where D: SharedData + 'static {
    node_ref: Arc<NT>,
    synchronizer: Arc<Synchronizer<D>>,
    timeouts: Timeouts,
    /// The log of pending requests
    pending_decision_log: Arc<PendingRequestLog<D>>,
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

struct DebugStats {
    collected_per_batch_total: u64,
    collections: u32,
    batches_made: u32,
}

struct ProposeBuilder<D> where D: SharedData {
    currently_accumulated: Vec<StoredMessage<RequestMessage<D::Request>>>,
    last_proposal: Instant,
}

impl<D> ProposeBuilder<D> where D: SharedData {
    pub fn new(target_size: usize) -> Self {
        Self { currently_accumulated: Vec::with_capacity(target_size), last_proposal: Instant::now() }
    }
}

///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<D, NT: 'static> Proposer<D, NT> where D: SharedData + 'static {
    pub fn new(
        node: Arc<NT>,
        sync: Arc<Synchronizer<D>>,
        pending_decision_log: Arc<PendingRequestLog<D>>,
        timeouts: Timeouts,
        executor_handle: ExecutorHandle<D>,
        consensus_guard: Arc<ProposerConsensusGuard>,
        proposer_config: ProposerConfig,
    ) -> Arc<Self> {
        let ProposerConfig {
            target_batch_size, max_batch_size, batch_timeout
        } = proposer_config;

        Arc::new(Self {
            node_ref: node,
            synchronizer: sync,
            timeouts,
            cancelled: AtomicBool::new(false),
            consensus_guard,
            target_global_batch_size: target_batch_size as usize,
            global_batch_time_limit: batch_timeout as u128,
            executor_handle,
            pending_decision_log,
            max_batch_size: max_batch_size as usize,
        })
    }

    ///Start this work
    pub fn start<ST>(self: Arc<Self>) -> JoinHandle<()>
        where ST: StateTransferMessage + 'static,
              NT: Node<PBFT<D, ST>> {
        std::thread::Builder::new()
            .name(format!("Proposer thread"))
            .spawn(move || {

                //DEBUGGING

                let mut debug_stats = DebugStats {
                    collected_per_batch_total: 0,
                    collections: 0,
                    batches_made: 0,
                };

                //END DEBUGGING

                //The currently accumulated requests, accumulated while we wait for the next batch to propose
                let mut ordered_propose = ProposeBuilder::new(self.target_global_batch_size);

                let mut unordered_propose = ProposeBuilder::new(self.target_global_batch_size);

                loop {
                    if self.cancelled.load(Ordering::Relaxed) {
                        break;
                    }

                    //We only want to produce batches to the channel if we are the leader, as
                    //Only the leader will propose thing

                    //TODO: Maybe not use this as it can spam the lock on synchronizer?
                    let info = self.synchronizer.view();

                    let is_leader = info.leader_set().contains(&self.node_ref.id());

                    let our_slice = info.hash_space_division()
                        .get(&self.node_ref.id()).cloned().clone();

                    //We do this as we don't want to get stuck waiting for requests that might never arrive
                    //Or even just waiting for any type of request. We want to minimize the amount of time the
                    //Consensus is waiting for new requests

                    //We don't need to do this for non leader replicas, as that would cause unnecessary strain as the
                    //Thread is in an infinite loop
                    // Receive the requests from the clients and process them
                    let opt_msgs = if is_leader {
                        match self.node_ref.try_recv_from_clients() {
                            Ok(msg) => msg,
                            Err(err) => {
                                error!("{:?} // Failed to receive requests from clients because {:?}", self.node_ref.id(), err);

                                continue;
                            }
                        }
                    } else {
                        match self.node_ref.receive_from_clients(Some(Duration::from_micros(self.global_batch_time_limit as u64))) {
                            Ok(msgs) => {
                                Some(msgs)
                            }
                            Err(err) => {
                                error!("{:?} // Failed to receive requests from clients because {:?}", self.node_ref.id(), err);

                                continue;
                            }
                        }
                    };

                    let discovered_requests = opt_msgs.is_some() &&
                        opt_msgs.as_ref().map(|msg| !msg.is_empty()).unwrap_or(false);

                    if let Some(messages) = opt_msgs {
                        debug_stats.collected_per_batch_total += messages.len() as u64;
                        debug_stats.collections += 1;

                        let mut digest_vec = Vec::with_capacity(messages.len());

                        for message in messages {
                            let NetworkMessage { header, message } = message;

                            let sysmsg = message.into();

                            match sysmsg {
                                SystemMessage::OrderedRequest(req) => {
                                    let rq_digest = header.unique_digest();

                                    if is_leader && is_request_in_hash_space(&rq_digest,
                                                                             our_slice.as_ref().unwrap()) {

                                        // we know that these operations will always be proposed since we are a
                                        // Correct replica. We can therefore just add them to the latest op log
                                        let stored_message = StoredMessage::new(header, req);

                                        ordered_propose.currently_accumulated.push(stored_message);
                                    } else {
                                        digest_vec.push((rq_digest, header.from(), req.sequence_number(), req.session_id()));

                                        // TODO: Maybe this should be handled by another thread in blocks?
                                        self.pending_decision_log.insert(header, req);
                                    }
                                }
                                SystemMessage::UnorderedRequest(req) => {
                                    let stored_message = StoredMessage::new(header, req);

                                    unordered_propose.currently_accumulated.push(stored_message);
                                }
                                _ => {}
                            }
                        }

                        self.synchronizer.watch_received_requests(digest_vec, &self.timeouts);
                    }

                    // Check if we have any forwarded requests that we need to propose
                    self.handle_forwarded_requests(is_leader,
                                                   our_slice.as_ref(),
                                                   &mut ordered_propose);

                    //Lets first deal with unordered requests since it should be much quicker and easier
                    self.propose_unordered(&mut unordered_propose);

                    self.propose_ordered(is_leader,
                                         &mut ordered_propose,
                                         &mut debug_stats);

                    if !discovered_requests {
                        //Yield to prevent active waiting

                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }).unwrap()
    }

    /// Attempt to propose an unordered request batch
    /// Fails if the batch is not large enough or the timeout
    /// Has not yet occurred
    fn propose_unordered<ST>(
        &self,
        propose: &mut ProposeBuilder<D>,
    ) where ST: StateTransferMessage + 'static,
            NT: Node<PBFT<D, ST>> {
        if !propose.currently_accumulated.is_empty() {
            let current_batch_size = propose.currently_accumulated.len();

            let should_exec = if current_batch_size < self.target_global_batch_size {
                let micros_since_last_batch = Instant::now()
                    .duration_since(propose.last_proposal)
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
                                      Vec::with_capacity(self.target_global_batch_size * 2));

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
            }
        }
    }

    /// attempt to propose the ordered requests that we have collected
    fn propose_ordered<ST>(&self,
                           is_leader: bool,
                           propose: &mut ProposeBuilder<D>,
                           debug: &mut DebugStats)
        where ST: StateTransferMessage + 'static,
              NT: Node<PBFT<D, ST>> {

        //Now let's deal with ordered requests
        if is_leader {
            let current_batch_size = propose.currently_accumulated.len();

            if current_batch_size < self.target_global_batch_size {
                let micros_since_last_batch = Instant::now().duration_since(propose.last_proposal).as_micros();

                if micros_since_last_batch <= self.global_batch_time_limit {

                    //Batch isn't large enough and time hasn't passed, don't even attempt to propose
                    return;
                }
            }

            if self.consensus_guard.can_propose() {

                if let Some((seq, view)) = self.consensus_guard.next_seq_no() {
                    propose.last_proposal = Instant::now();

                    debug.batches_made += 1;

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
                                                          next_batch.unwrap_or(Vec::with_capacity(self.max_batch_size * 2)));


                    self.propose(seq, &view, current_batch);

                    //Stats
                    if debug.batches_made % 10000 == 0 {
                        //let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                        /*println!("{:?} // {:?} // {}: batches made {}: message collections {}: total requests collected.",
                                 self.node_ref.id(), duration, debug.batches_made,
                                 debug.collections, debug.collected_per_batch_total);*/


                        debug.batches_made = 0;
                        debug.collections = 0;
                        debug.collected_per_batch_total = 0;
                    }
                }
            }
        }
    }

    /// Proposes a new batch.
    /// (Basically broadcasts it to all of the members)
    fn propose<ST>(
        &self,
        seq: SeqNo,
        view: &ViewInfo,
        currently_accumulated: Vec<StoredMessage<RequestMessage<D::Request>>>,
    ) where ST: StateTransferMessage + 'static, NT: Node<PBFT<D, ST>> {
        let currently_accumulated = self.pending_decision_log.filter_and_update_more_recent(currently_accumulated);

        let mut to_propose = Vec::with_capacity(currently_accumulated.len());

        for message in &currently_accumulated {
            to_propose.push((message.header().from(), message.message().session_id(), message.sequence_number(), message.header().unique_digest()));
        }

        info!("{:?} // Proposing new batch with {} request count {:?} ({:?})", self.node_ref.id(), currently_accumulated.len(), seq, to_propose);

        let message = PBFTMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::PrePrepare(currently_accumulated),
        ));

        let targets = view.quorum_members().iter().copied();

        self.node_ref.broadcast_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
    }

    /// Check if we have received forwarded requests. If so, then
    /// we want to add them to the next batch we are proposing
    fn handle_forwarded_requests(&self, is_leader: bool,
                                 our_slice: Option<&(Vec<u8>, Vec<u8>)>,
                                 ordered_propose_builder: &mut ProposeBuilder<D>) {
        let fwd_rqs = self.pending_decision_log.take_forwarded_requests(None);

        if let Some(fwd_rqs) = fwd_rqs {
            for req in fwd_rqs {
                let rq_digest = req.header().unique_digest();

                if is_leader && is_request_in_hash_space(&rq_digest, our_slice.unwrap()) {
                    // We can safely add this request to our batch since it is in our hash space and
                    // it will still be examined by the [`filter_and_update_more_recent`] method
                    ordered_propose_builder.currently_accumulated.push(req);
                } else {
                    let (header, message) = req.into_inner();

                    self.pending_decision_log.insert(header, message);
                }
            }
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn requests_received(
        &self,
        _t: DateTime<Utc>,
        reqs: Vec<StoredMessage<RequestMessage<D::Request>>>,
    ) {}
}
