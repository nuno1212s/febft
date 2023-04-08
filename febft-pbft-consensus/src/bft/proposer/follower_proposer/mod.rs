use std::sync::{atomic::AtomicBool, Arc};
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};
use febft_communication::message::StoredMessage;
use febft_communication::Node;
use febft_execution::app::Service;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::messages::RequestMessage;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::PBFT;

pub type BatchType<D: SharedData> = Vec<StoredMessage<RequestMessage<D::Request>>>;

///TODO:
pub struct FollowerProposer<D: SharedData + 'static, NT: Node<PBFT<D>>> {
    batch_channel: (ChannelSyncTx<BatchType<D>>, ChannelSyncRx<BatchType<D>>),

    log: Arc<PendingRequestLog<D>>,
    //For request execution
    executor_handle: ExecutorHandle<D>,
    cancelled: AtomicBool,

    //Reference to the network node
    node_ref: Arc<NT>,

    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,
}


///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 1024;


impl<D: SharedData + 'static, NT: Node<PBFT<D>>> FollowerProposer<D, NT> {
    pub fn new(
        node: Arc<NT>,
        log: Arc<PendingRequestLog<D>>,
        executor: ExecutorHandle<D>,
        target_global_batch_size: usize,
        global_batch_time_limit: u128,
    ) -> Arc<Self> {
        todo!();
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
