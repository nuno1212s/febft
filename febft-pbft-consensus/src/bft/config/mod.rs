use std::sync::Arc;
use std::time::Duration;
use febft_common::node_id::NodeId;
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::serialize::{OrderingProtocolMessage, StateTransferMessage, System};
use febft_messages::timeouts::Timeouts;
use crate::bft::follower::FollowerHandle;
use crate::bft::observer::ObserverHandle;
use crate::bft::sync::view::ViewInfo;

pub struct PBFTConfig<D: SharedData, P: OrderingProtocolMessage, ST: StateTransferMessage, NT: Node<System<D, P, ST>>> {
    pub node_id: NodeId,
    pub node: Arc<NT>,
    pub executor: ExecutorHandle<D>,
    pub observer_handle: ObserverHandle,
    pub follower_handle: Option<FollowerHandle<D>>,
    pub view: ViewInfo,
    pub timeouts: Timeouts,
    pub timeout_dur: Duration,
    pub db_path: String,
    pub proposer_config: ProposerConfig
}

impl<D: SharedData, P: OrderingProtocolMessage, ST: StateTransferMessage, NT: Node<System<D, P, ST>>> PBFTConfig<D, P, ST, NT> {
    pub fn new(node_id: NodeId, node: Arc<NT>, executor: ExecutorHandle<D>, 
               observer_handle: ObserverHandle, follower_handle: Option<FollowerHandle<D>>, 
               view: ViewInfo, timeouts: Timeouts, timeout_dur: Duration, 
               db_path: String, proposer_config: ProposerConfig) -> Self {
        Self { node_id, node, executor, observer_handle, follower_handle, view, timeouts, timeout_dur, db_path, proposer_config }
    }
}

pub struct ProposerConfig {
    pub target_batch_size: u64,
    pub max_batch_size: u64,
    pub batch_timeout: u64,
}

impl ProposerConfig {
    pub fn new(target_batch_size: u64, max_batch_size: u64, batch_timeout: u64) -> Self {
        Self { target_batch_size, max_batch_size, batch_timeout }
    }
}
