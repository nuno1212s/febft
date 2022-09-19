pub mod follower_proposer;

use log::{error, warn, debug};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::Message::System;
use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, ObserverMessage, RequestMessage, StoredMessage,
    SystemMessage,
};
use crate::bft::communication::{channel, Node, NodeId};
use crate::bft::consensus::ConsensusGuard;
use chrono::{DateTime, Utc};

use crate::bft::consensus::log::Log;
use crate::bft::threadpool;

use crate::bft::core::server::observer::{ConnState, MessageType, ObserverHandle};
use crate::bft::executable::{ExecutorHandle, Reply, Request, Service, State, UnorderedBatch};
use crate::bft::ordering::Orderable;
use crate::bft::timeouts::TimeoutsHandle;

use super::consensus::log::persistent::PersistentLogModeTrait;
use super::core::server::ViewInfo;
use super::ordering::SeqNo;
use super::sync::{Synchronizer, AbstractSynchronizer};

pub type BatchType<S> = Vec<StoredMessage<RequestMessage<S>>>;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct Proposer<S: Service + 'static, T> where T: PersistentLogModeTrait{
    batch_channel: (ChannelSyncTx<BatchType<S>>, ChannelSyncRx<BatchType<S>>),
    node_ref: Arc<Node<S::Data>>,
    synchronizer: Arc<Synchronizer<S>>,
    timeouts: Arc<TimeoutsHandle<S>>,
    log: Arc<Log<S, T>>,
    //For unordered request execution
    executor_handle: ExecutorHandle<S>,
    consensus_guard: ConsensusGuard,
    cancelled: AtomicBool,
    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,

    //Observer related stuff
    observer_handle: ObserverHandle,
}

const TIMEOUT: Duration = Duration::from_micros(10);

///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<S: Service + 'static, T: PersistentLogModeTrait + 'static> Proposer<S, T> {
    pub fn new(
        node: Arc<Node<S::Data>>,
        sync: Arc<Synchronizer<S>>,
        log: Arc<Log<S, T>>,
        timeouts: Arc<TimeoutsHandle<S>>,
        executor_handle: ExecutorHandle<S>,
        consensus_guard: ConsensusGuard,
        target_global_batch_size: usize,
        global_batch_time_limit: u128,
        observer_handle: ObserverHandle,
    ) -> Arc<Self> {
        let (channel_tx, channel_rx) = channel::new_bounded_sync(BATCH_CHANNEL_SIZE);

        Arc::new(Self {
            batch_channel: (channel_tx, channel_rx),
            node_ref: node,
            synchronizer: sync,
            timeouts,
            log,
            cancelled: AtomicBool::new(false),
            consensus_guard,
            target_global_batch_size,
            global_batch_time_limit,
            observer_handle,
            executor_handle,
        })
    }

    pub fn receiver_channel(&self) -> &ChannelSyncRx<BatchType<S>> {
        &self.batch_channel.1
    }

    ///Start this work
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::Builder::new()
            .spawn(move || {
                let mut collected_per_batch_total: u64 = 0;
                let mut collections: u32 = 0;
                let mut batches_made: u32 = 0;

                //The currently accumulated requests, accumulated while we wait for the next batch to propose
                let mut currently_accumulated = Vec::with_capacity(self.target_global_batch_size * 10);

                let mut last_seq = None;
                let mut last_unordered_batch = Instant::now();

                let mut currently_accumulated_unordered = Vec::with_capacity(self.target_global_batch_size * 2);

                let mut last_proposed_batch = Instant::now();

                loop {
                    if self.cancelled.load(Ordering::Relaxed) {
                        break;
                    }

                    //We only want to produce batches to the channel if we are the leader, as
                    //Only the leader will propose thing

                    //TODO: Maybe not use this as it can spam the lock on synchronizer?
                    let mut is_leader = self.synchronizer.view().leader() == self.node_ref.id();

                    //We do this as we don't want to get stuck waiting for requests that might never arrive
                    //Or even just waiting for any type of request. We want to minimize the amount of time the
                    //Consensus is waiting for new requests

                    //We don't need to do this for non leader replicas, as that would cause unnecessary strain as the
                    //Thread is in an infinite loop
                    // Receive the requests from the clients and process them
                    let opt_msgs = if is_leader {
                        self.node_ref.try_recv_from_clients().unwrap()
                    } else {
                        Some(self.node_ref.receive_from_clients(None).unwrap())
                    };

                    //debug!("{:?} // Received batch of {} messages from clients, processing them, is_leader? {}",
                    //    self.node_ref.id(), messages.len(), is_leader);

                    //TODO: Handle timing out requests

                    if let Some(messages) = opt_msgs {
                        collected_per_batch_total += messages.len() as u64;
                        collections += 1;

                        //let mut to_log: Vec<T> = Vec::with_capacity(messages.len());

                        //let lock_guard = self.log.latest_op().lock();

                        for message in messages {
                            match message {
                                System(header, sysmsg) => {
                                    match sysmsg {
                                        SystemMessage::Request(req) => {
                                            /*let key = logg::operation_key(&header, &req);

                                            let current_seq_for_client = lock_guard.get(key)
                                                .copied()
                                                .unwrap_or(SeqNo::ZERO);

                                            if req.sequence_number() < current_seq_for_client {
                                                //Avoid repeating requests for clients
                                                continue;
                                            }*/

                                            if is_leader {
                                                currently_accumulated.push(StoredMessage::new(header, req));
                                            }

                                            //Store the message in the log in this thread.
                                            //to_log.push(StoredMessage::new(header, req));
                                        }
                                        SystemMessage::UnOrderedRequest(req) => {
                                            debug!("{:?} // Received unordered request", self.node_ref.id());

                                            currently_accumulated_unordered.push(StoredMessage::new(header, req));
                                        }
                                        SystemMessage::Reply(_) => {
                                            warn!("Received system reply msg")
                                        }
                                        SystemMessage::ObserverMessage(msg) => {
                                            match msg {
                                                ObserverMessage::ObserverRegister => {
                                                    //Avoid sending these messages to the main replica
                                                    //Processing thread and just process them here instead as it
                                                    //Does not delay the process
                                                    if let Err(_) = self.observer_handle.tx().send(
                                                        MessageType::Conn(ConnState::Connected(header.from()))
                                                    ) {
                                                        error!("Failed to send messages to the observer handle.");
                                                    }
                                                }
                                                _ => {
                                                    warn!("Received observer message that are not register?");
                                                }
                                            }
                                        }
                                        _ => {
                                            warn!("Received system message that was unexpected!");
                                        }
                                    }
                                }
                                _ => {
                                    warn!("Client sent a message that he should not have sent!");
                                }
                            }
                        }

                        //drop(lock_guard);

                        //self.requests_received(DateTime::from(SystemTime::now()), to_log);
                    }

                    //Lets first deal with unordered requests since it should be much quicker and easier
                    self.propose_unordered(&mut currently_accumulated_unordered, &mut last_unordered_batch);

                    //Now let's deal with ordered requests
                    if is_leader && !currently_accumulated.is_empty() {
                        let current_batch_size = currently_accumulated.len();

                        if current_batch_size < self.target_global_batch_size {
                            let micros_since_last_batch = Instant::now().duration_since(last_proposed_batch).as_micros();

                            if micros_since_last_batch <= self.global_batch_time_limit {
                                //Yield to prevent active waiting
                                std::thread::yield_now();

                                //Batch isn't large enough and time hasn't passed, don't even attempt to propose
                                continue;
                            }
                        }

                        //Attempt to propose new batch
                        match self.consensus_guard.consensus_guard().compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed) {
                            Ok(_) => {
                                last_proposed_batch = Instant::now();

                                batches_made += 1;
                                let guard = self.consensus_guard.consensus_info().lock().unwrap();

                                let (seq, view) = &*guard;

                                match &last_seq {
                                    None => {}
                                    Some(last_exec) => {
                                        if *last_exec >= *seq {
                                            //We are still in the same consensus instance,
                                            //Don't produce more pre prepares
                                            continue;
                                        }
                                    }
                                }

                                last_seq = Some(*seq);

                                let next_batch = if currently_accumulated.len() > self.target_global_batch_size {

                                    //TODO: just make processing these batches faster
                                    //If the batch is too large (120k requests for example, we can get stuck on processing (execution) them as they all come at once and
                                    //Producing the replies takes some time.)
                                    //So we will split it up here


                                    //This now contains target_global_size requests. We want this to be our next batch
                                    //Currently accumulated contains the remaining messages to be sent in the next batch
                                    let mut next_batch = currently_accumulated.split_off(currently_accumulated.len() - self.target_global_batch_size);

                                    //So we swap that memory with the other vector memory and we have it!
                                    std::mem::swap(&mut next_batch, &mut currently_accumulated);

                                    Some(next_batch)
                                } else {
                                    None
                                };

                                self.propose(*seq, view, currently_accumulated);
                                
                                currently_accumulated = next_batch.unwrap_or(Vec::with_capacity(self.node_ref.batch_size() * 2));

                                //Stats
                                if batches_made % 10000 == 0 {
                                    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();

                                    println!("{:?} // {:?} // {}: batches made {}: message collections {}: total requests collected.",
                                             self.node_ref.id(), duration, batches_made, collections, collected_per_batch_total);

                                    batches_made = 0;
                                    collections = 0;
                                    collected_per_batch_total = 0;
                                }
                            }
                            Err(_) => {}
                        }
                    }

                    //Yield to prevent active waiting
                    std::thread::yield_now();
                }
            }).unwrap()
    }


    /// Proposes a new batch.
    fn propose(
        &self,
        seq: SeqNo,
        view: &ViewInfo,
        currently_accumulated: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    ) {
        let message = SystemMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::PrePrepare(currently_accumulated),
        ));

        let targets = NodeId::targets(0..view.params().n());

        self.node_ref.broadcast(message, targets);
    }

    /// Attempt to propose an unordered request batch
    /// Fails if the batch is not large enough or the timeout
    /// Has not yet occurred
    fn propose_unordered(
        &self,
        currently_unordered_batch: &mut Vec<StoredMessage<RequestMessage<Request<S>>>>,
        last_unordered_batch: &mut Instant,
    ) {
        if !currently_unordered_batch.is_empty() {
            let current_batch_size = currently_unordered_batch.len();

            let should_exec = if current_batch_size < self.target_global_batch_size {
                let micros_since_last_batch = Instant::now()
                    .duration_since(*last_unordered_batch)
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
                let mut new_accumulated_vec = Vec::with_capacity(self.target_global_batch_size * 2);

                std::mem::swap(last_unordered_batch, &mut Instant::now());
                //swap in the new vec and take the previous one to the threadpool
                std::mem::swap(currently_unordered_batch, &mut new_accumulated_vec);

                let executor_handle = self.executor_handle.clone();

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

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn requests_received(
        &self,
        t: DateTime<Utc>,
        reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    ) {
        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer
            .watch_request(h.unique_digest(), &self.timeouts);

        // This was replaced with a batched log instead of a per message log to save hashing ops
        // self.log.insert(h, r);
    }
}
