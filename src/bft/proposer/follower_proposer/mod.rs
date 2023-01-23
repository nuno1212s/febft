use std::sync::{atomic::AtomicBool, Arc};

use crate::bft::{
    communication::{
        channel::{self, ChannelSyncRx, ChannelSyncTx},
        message::{RequestMessage, StoredMessage},
        Node,
    },
    executable::{ExecutorHandle, Service},
};
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;

pub type BatchType<S> = Vec<StoredMessage<RequestMessage<S>>>;


///TODO:
pub struct FollowerProposer<S: Service + 'static> {
    batch_channel: (ChannelSyncTx<BatchType<S>>, ChannelSyncRx<BatchType<S>>),

    log: Arc<PendingRequestLog<S>>,
    //For request execution
    executor_handle: ExecutorHandle<S>,
    cancelled: AtomicBool,

    //Reference to the network node
    node_ref: Arc<Node<S::Data>>,

    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,
}


///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 128;

impl<S: Service + 'static> FollowerProposer<S> {
    pub fn new(
        node: Arc<Node<S::Data>>,
        log: Arc<PendingRequestLog<S>>,
        executor: ExecutorHandle<S>,
        target_global_batch_size: usize,
        global_batch_time_limit: u128,
    ) -> Arc<Self> {
        let (channel_tx, channel_rx) = channel::new_bounded_sync(BATCH_CHANNEL_SIZE);

        Arc::new(Self {
            batch_channel: (channel_tx, channel_rx),
            log,
            executor_handle: executor,
            cancelled: AtomicBool::new(false),
            node_ref: node,
            target_global_batch_size,
            global_batch_time_limit,
        })
    }
}
