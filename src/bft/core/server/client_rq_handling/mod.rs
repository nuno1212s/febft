use std::fmt::format;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use crossbeam_channel::{bounded, Receiver, Sender};
use log::{debug, info, trace};
use parking_lot::{Mutex, RawMutex};
use parking_lot::lock_api::MutexGuard;

use crate::bft::async_runtime as rt;
use crate::bft::communication::{Node, NodeId};
use crate::bft::communication::channel::{ChannelRx, ChannelTx, new_bounded};
use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, Header, Message, RequestMessage, StoredMessage, SystemMessage};
use crate::bft::communication::message::Message::System;
use crate::bft::communication::peer_handling::NodePeers;
use crate::bft::communication::serialize::SharedData;
use crate::bft::consensus::log as logg;
use crate::bft::consensus::log::Log;
use crate::bft::core::server::{Replica, ViewInfo};
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::sync::Synchronizer;
use crate::bft::timeouts::TimeoutsHandle;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct RqProcessor<S: Service + 'static> {
    batch_channel: (Sender<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
                    Receiver<Vec<StoredMessage<RequestMessage<Request<S>>>>>),
    node_ref: Arc<Node<S::Data>>,
    synchronizer: Arc<Synchronizer<S>>,
    timeouts: Arc<TimeoutsHandle<S>>,
    log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
    consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
    consensus_guard: Arc<AtomicBool>,
    cancelled: AtomicBool,
}

const TIMEOUT: Duration = Duration::from_micros(10);

///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<S: Service> RqProcessor<S> {
    pub fn new(node: Arc<Node<S::Data>>, sync: Arc<Synchronizer<S>>,
               log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
               timeouts: Arc<TimeoutsHandle<S>>,
               consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
               consensus_guard: Arc<AtomicBool>) -> Arc<Self> {
        let (channel_tx, channel_rx) = crossbeam_channel::bounded(BATCH_CHANNEL_SIZE);

        Arc::new(Self {
            batch_channel: (channel_tx, channel_rx),
            node_ref: node,
            synchronizer: sync,
            timeouts,
            log,
            cancelled: AtomicBool::new(false),
            consensus_lock,
            consensus_guard,
        })
    }

    pub fn receiver_channel(&self) -> &Receiver<Vec<StoredMessage<RequestMessage<Request<S>>>>> {
        &self.batch_channel.1
    }

    ///Start this work
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::Builder::new().name(format!("Client RQ Handling {:?}", self.node_ref.id())).spawn(move || {

            //The currently accumulated requests, accumulated while we wait for the next batch to propose
            let mut currently_accumulated = Vec::with_capacity(self.node_ref.batch_size() * 2);

            let mut last_seq = Option::None;

            let mut collected_per_batch_total: u64 = 0;
            let mut collections: u32 = 0;
            let mut batches_made: u32 = 0;

            loop {
                if self.cancelled.load(Ordering::Relaxed) {
                    break;
                }

                //We only want to produce batches to the channel if we are the leader, as
                //Only the leader will propose things
                let mut is_leader = self.synchronizer.view().leader() == self.node_ref.id();

                //We do this as we don't want to get stuck waiting for requests that might never arrive
                //Or even just waiting for any type of request. We want to minimize the amount of time the
                //Consensus is waiting for new requests

                //We don't need to do this for non leader replicas, as that would cause unnecessary strain as the
                //Thread is in an infinite loop
                ///Receive the requests from the clients and process them
                let opt_msgs = if is_leader {
                    self.node_ref.try_recv_from_clients().unwrap()
                } else {
                    Some(self.node_ref.receive_from_clients(None).unwrap())
                };

                //debug!("{:?} // Received batch of {} messages from clients, processing them, is_leader? {}",
                //    self.node_ref.id(), messages.len(), is_leader);

                if let Some(messages) = opt_msgs {
                    collected_per_batch_total += messages.len() as u64;
                    collections += 1;

                    let mut to_log = Vec::with_capacity(messages.len());

                    //let lock_guard = self.log.latest_op().lock();

                    for message in messages {
                        match message {
                            Message::System(header, sysmsg) => {
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
                                    SystemMessage::Reply(rep) => {
                                        panic!("Received system reply msg")
                                    }
                                    _ => {
                                        panic!("Received system message that was unexpected!");
                                    }
                                }
                            }
                            _ => {
                                panic!("Client sent a message that he should not have sent!");
                            }
                        }
                    }

                    //drop(lock_guard);

                    //TODO: If we are the leader, preemptively hash the preprepare message so we don't have
                    //To wait for that?
                    self.requests_received(DateTime::from(SystemTime::now()), to_log);
                }

                if is_leader && !currently_accumulated.is_empty() {
                    //Attempt to propose new batch
                    match self.consensus_guard.compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed) {
                        Ok(_) => {
                            batches_made += 1;
                            let guard = self.consensus_lock.lock();

                            let (seq, view) = *guard;

                            match &last_seq {
                                None => {}
                                Some(last_exec) => {
                                    if *last_exec >= seq {
                                        //We are still in the same consensus instance,
                                        //Don't produce more pre prepares
                                        continue;
                                    }
                                }
                            }

                            last_seq = Some(seq);

                            let message = SystemMessage::Consensus(ConsensusMessage::new(
                                seq,
                                view.sequence_number(),
                                ConsensusMessageKind::PrePrepare(currently_accumulated),
                            ));

                            let targets = NodeId::targets(0..view.params().n());

                            self.node_ref.broadcast(message, targets, Arc::clone(self.log.batch_meta()));

                            currently_accumulated = Vec::with_capacity(self.node_ref.batch_size() * 2);

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
            }
        }).unwrap()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn requests_received(&self, t: DateTime<Utc>, reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>) {
        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer.watch_request(
            h.unique_digest(),
            &self.timeouts,
        );

        // This was replaced with a batched log instead of a per message log to save hashing ops
        // self.log.insert(h, r);
    }
}
