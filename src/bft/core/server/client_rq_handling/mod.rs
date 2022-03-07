use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use log::debug;

use crate::bft::async_runtime as rt;
use crate::bft::communication::channel::{ChannelRx, ChannelTx, new_bounded};
use crate::bft::communication::message::{Header, Message, RequestMessage, StoredMessage, SystemMessage};
use crate::bft::communication::message::Message::System;
use crate::bft::communication::Node;
use crate::bft::communication::peer_handling::NodePeers;
use crate::bft::communication::serialize::SharedData;
use crate::bft::consensus::log::Log;
use crate::bft::core::server::Replica;
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::sync::Synchronizer;
use crate::bft::timeouts::TimeoutsHandle;

///Handles taking requests from the client pools and storing the requests in the log,
///as well as creating new batches and delivering them to the batch_channel
///Another thread will then take from this channel and propose the requests
pub struct RqProcessor<S: Service + 'static> {
    batch_channel: (ChannelTx<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
                    ChannelRx<Vec<StoredMessage<RequestMessage<Request<S>>>>>),
    node_ref: Arc<Node<S::Data>>,
    synchronizer: Arc<Synchronizer<S>>,
    timeouts: Arc<TimeoutsHandle<S>>,
    log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
    cancelled: AtomicBool,
}


///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<S: Service> RqProcessor<S> {
    pub fn new(node: Arc<Node<S::Data>>, sync: Arc<Synchronizer<S>>,
               log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
               timeouts: Arc<TimeoutsHandle<S>>) -> Arc<Self> {
        let (channel_tx, channel_rx) = new_bounded(BATCH_CHANNEL_SIZE);

        Arc::new(Self {
            batch_channel: (channel_tx, channel_rx),
            node_ref: node,
            synchronizer: sync,
            timeouts,
            log,
            cancelled: AtomicBool::new(false),
        })
    }

    pub fn receiver_channel(&self) -> ChannelRx<Vec<StoredMessage<RequestMessage<Request<S>>>>> {
        self.batch_channel.1.clone()
    }

    ///Start this work
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            loop {
                if self.cancelled.load(Ordering::Relaxed) {
                    break;
                }

                ///Receive the requests from the clients and process them
                let messages = self.node_ref.receive_from_clients().unwrap();

                //We only want to produce batches to the channel if we are the leader, as
                //Only the leader will propose things
                let mut is_leader = self.synchronizer.view().leader() == self.node_ref.id();

                debug!("{:?} // Received batch of {} messages from clients, processing them, is_leader? {}",
                    self.node_ref.id(), messages.len(), is_leader);

                //For now
                is_leader = true;

                let mut final_batch = if is_leader {
                    Some(Vec::with_capacity(messages.len()))
                } else {
                    None
                };

                let mut to_log = Vec::with_capacity(messages.len());

                for message in messages {
                    match message {
                        Message::System(header, sysmsg) => {
                            match sysmsg {
                                SystemMessage::Request(req) => {
                                    match &mut final_batch {
                                        None => {}
                                        Some(batch) => {
                                            batch.push(StoredMessage::new(header, req.clone()));
                                        }
                                    }

                                    //Store the message in the log in this thread.
                                    to_log.push(StoredMessage::new(header, req));
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

                //TODO: If we are the leader, preemptively hash the preprepare message so we don't have
                //To wait for that?
                self.requests_received(DateTime::from(SystemTime::now()), to_log);

                //Send the finished batches to the other thread
                if is_leader {
                    let tx = &self.batch_channel.0;

                    tx.send_sync(final_batch.unwrap());
                }
            }
        })
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn requests_received(&self, t: DateTime<Utc>, reqs: Vec<StoredMessage<RequestMessage<Request<S>>>>) {
        let mut batch_meta = self.log.batch_meta().lock();

        batch_meta.reception_time = t;
        batch_meta.batch_size = reqs.len();

        drop(batch_meta);

        for (h, r) in reqs.into_iter().map(StoredMessage::into_inner) {
            self.request_received(h, SystemMessage::Request(r))
        }
    }

    fn request_received(&self, h: Header, r: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        self.synchronizer.watch_request(
            h.unique_digest(),
            &self.timeouts,
        );

        // self.log.insert(h, r);
    }
}
