use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::{RequestMessage, StoredMessage};
use crate::bft::communication::{channel, NodeId};

use crate::bft::executable::{ExecutorHandle, Request, Service, UpdateBatch};
use crate::bft::msg_log::{Info, Log, operation_key};
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::ordering::{Orderable, SeqNo};

type RequestToProcess<O> = (
    Info,
    BatchMeta,
    SeqNo,
    Vec<StoredMessage<RequestMessage<O>>>,
);

const REQ_BATCH_BUFF: usize = 1024;

///Made to finish the requests and clean up the logging.
/// This is not currently in use
pub struct RqFinalizer<S>
where
    S: Service,
{
    node_id: NodeId,
    log: Arc<Log<S>>,
    executor: ExecutorHandle<S>,
    channel: ChannelSyncRx<RequestToProcess<Request<S>>>,
}

pub struct RqFinalizerHandle<S>
where
    S: Service + 'static,
{
    channel: ChannelSyncTx<RequestToProcess<Request<S>>>,
}

impl<S> RqFinalizerHandle<S>
where
    S: Service + 'static,
{
    pub fn new(sender: ChannelSyncTx<RequestToProcess<Request<S>>>) -> Self {
        Self { channel: sender }
    }

    pub fn queue_finalize(
        &self,
        info: Info,
        batch_meta: BatchMeta,
        seq: SeqNo,
        rqs: Vec<StoredMessage<RequestMessage<Request<S>>>>,
    ) {
        self.channel
            .send((info, batch_meta, seq, rqs))
            .expect("Failed to finalize")
    }
}

impl<S> Deref for RqFinalizerHandle<S>
where
    S: Service + 'static,
{
    type Target = ChannelSyncTx<RequestToProcess<Request<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

impl<S> Clone for RqFinalizerHandle<S>
where
    S: Service + 'static,
{
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<S> RqFinalizer<S>
where
    S: Service + 'static,
{
    pub fn new(
        node: NodeId,
        log: Arc<Log<S>>,
        executor_handle: ExecutorHandle<S>,
    ) -> RqFinalizerHandle<S> {
        let (ch_tx, ch_rx) = channel::new_bounded_sync(REQ_BATCH_BUFF);

        let _rq_finalizer = Self {
            node_id: node,
            log,
            executor: executor_handle,
            channel: ch_rx,
        };

        //TODO: Currently disabled
        //rq_finalizer.start();

        RqFinalizerHandle::new(ch_tx)
    }

    pub fn start(self) {
        std::thread::Builder::new()
            .name(format!("{:?} // Request finalizer thread", self.node_id))
            .spawn(move || {
                loop {
                    let (info, batch_meta, seq, rqs) = self.channel.recv().unwrap();

                    let mut batch = UpdateBatch::new_with_cap(seq, rqs.len());

                    let mut latest_op_update = BTreeMap::new();

                    for x in rqs {
                        let (header, message) = x.into_inner();

                        let key = operation_key::<Request<S>>(&header, &message);

                        let seq_no = latest_op_update.get(&key).copied().unwrap_or(SeqNo::ZERO);

                        if message.sequence_number() > seq_no {
                            latest_op_update.insert(key, message.sequence_number());
                        }

                        batch.add(
                            header.from(),
                            message.session_id(),
                            message.sequence_number(),
                            message.into_inner_operation(),
                        );
                    }

                    //Send the finalized rqs into the executor thread for execution
                    match info {
                        Info::Nil => self.executor.queue_update(batch_meta, batch),
                        // execute and begin local checkpoint
                        Info::BeginCheckpoint => self
                            .executor
                            .queue_update_and_get_appstate(batch_meta, batch),
                    }
                    .unwrap();

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
            })
            .expect("Failed to start rq finalizer thread");
    }
}
