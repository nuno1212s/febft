use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use intmap::IntMap;
use crate::bft::communication::message::{RequestMessage, StoredMessage};
use crate::bft::communication::NodeId;
use crate::bft::communication::serialize::SharedData;

use crate::bft::consensus::log::{Info, Log, operation_key};
use crate::bft::core::server::client_replier::ReplyHandle;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::{ExecutorHandle, Reply, Request, Service, State, UpdateBatch};
use crate::bft::ordering::{Orderable, SeqNo};

type RequestToProcess<O> = (Info, Vec<StoredMessage<RequestMessage<Arc<O>>>>);

const REQ_BATCH_BUFF: usize = 1024;

pub struct RqFinalizer<S> where S: Service {
    node_id: NodeId,
    log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
    executor: ExecutorHandle<S>,
    channel: Receiver<RequestToProcess<Request<S>>>,
}

pub struct RqFinalizerHandle<S> where S: Service
{
    channel: Sender<RequestToProcess<Request<S>>>,
}

impl<S> RqFinalizerHandle<S> where S: Service {

    pub fn new(sender: Sender<RequestToProcess<Request<S>>>) -> Self {
        Self {
            channel: sender
        }
    }

    pub fn queue_finalize(&self, info: Info, rqs: Vec<StoredMessage<RequestMessage<Arc<Request<S>>>>>) {
        self.channel.send((info, rqs)).unwrap()
    }
}

impl<S> Deref for RqFinalizerHandle<S> where S: Service{
    type Target = Sender<RequestToProcess<Request<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

impl<S> Clone for RqFinalizerHandle<S> where S: Service {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone()
        }
    }
}

impl<S> RqFinalizer<S> where S: Service {
    pub fn new(node: NodeId,
               log: Arc<Log<State<S>, Request<S>, Reply<S>>>,
               executor_handle: ExecutorHandle<S>) ->
               RqFinalizerHandle<S> {
        let (ch_tx, ch_rx) = crossbeam_channel::bounded(REQ_BATCH_BUFF);

        let rq_finalizer =
            Self {
                node_id: node,
                log,
                executor: executor_handle,
                channel: ch_rx,
            };

        rq_finalizer.start();

        RqFinalizerHandle::new(ch_tx)
    }

    pub fn start(mut self) {
        std::thread::Builder::new().name(format!("{:?} // Request finalizer thread", self.node_id))
            .spawn(move || {
            loop {
                let (info, rqs) = self.channel.recv().unwrap();

                let mut batch = UpdateBatch::new_with_cap(rqs.len());

                let mut latest_op_update = BTreeMap::new();

                for x in rqs {
                    let (header, message) = x.into_inner();

                    let key = operation_key::<Arc<Request<S>>>(&header, &message);

                    let seq_no = latest_op_update
                        .get(&key)
                        .copied()
                        .unwrap_or(SeqNo::ZERO);

                    if message.sequence_number() > seq_no {
                        latest_op_update.insert(key, message.sequence_number());
                    }

                    let msg = message.into_inner_operation();

                    batch.add(
                        header.from(),
                        message.session_id(),
                        message.sequence_number(),
                        (*msg).clone(),
                    );
                }

                //Send the finalized rqs into the executor thread for execution
                match info {
                    Info::Nil => self.executor.queue_update(
                        self.log.batch_meta(),
                        batch,
                    ),
                    // execute and begin local checkpoint
                    Info::BeginCheckpoint => self.executor.queue_update_and_get_appstate(
                        self.log.batch_meta(),
                        batch,
                    ),
                }.unwrap();

                let mut latest_op_guard = self.log.latest_op().lock();

                for (key, seq) in latest_op_update.into_iter() {

                    //Update the latest operations
                    let current_seq = latest_op_guard.get(key);

                    match current_seq {
                        None => {
                            latest_op_guard.insert(key, seq);
                        }
                        Some(curr_seq) => {
                            if seq > *curr_seq {
                                latest_op_guard.insert(key, seq);
                            }
                        }
                    }
                }
            }
        });
    }
}