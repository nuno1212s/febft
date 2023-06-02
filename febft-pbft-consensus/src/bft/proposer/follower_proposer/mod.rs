use std::marker::PhantomData;
use std::sync::{atomic::AtomicBool, Arc};
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_communication::message::StoredMessage;
use atlas_communication::Node;
use atlas_execution::app::Service;
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::SharedData;
use atlas_core::messages::{RequestMessage, StoredRequestMessage};
use atlas_core::serialize::StateTransferMessage;
use crate::bft::PBFT;

pub type BatchType<D: SharedData> = Vec<StoredRequestMessage<D::Request>>;

///TODO:
pub struct FollowerProposer<D, ST, NT>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static,
          NT: Node<PBFT<D, ST>> {
    batch_channel: (ChannelSyncTx<BatchType<D>>, ChannelSyncRx<BatchType<D>>),
    //For request execution
    executor_handle: ExecutorHandle<D>,
    cancelled: AtomicBool,

    //Reference to the network node
    node_ref: Arc<NT>,

    //The target
    target_global_batch_size: usize,
    //Time limit for generating a batch with target_global_batch_size size
    global_batch_time_limit: u128,
    _phantom: PhantomData<ST>
}


///The size of the batch channel
const BATCH_CHANNEL_SIZE: usize = 1024;


impl<D, ST, NT> FollowerProposer<D, ST, NT>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static,
          NT: Node<PBFT<D, ST>> {
    pub fn new(
        node: Arc<NT>,
        executor: ExecutorHandle<D>,
        target_global_batch_size: usize,
        global_batch_time_limit: u128,
    ) -> Arc<Self> {
        todo!();
        let (channel_tx, channel_rx) = channel::new_bounded_sync(BATCH_CHANNEL_SIZE);

        Arc::new(Self {
            batch_channel: (channel_tx, channel_rx),
            executor_handle: executor,
            cancelled: AtomicBool::new(false),
            node_ref: node,
            target_global_batch_size,
            global_batch_time_limit,
            _phantom: Default::default(),
        })
    }
}
