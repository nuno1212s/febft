use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::followers::FollowerHandle;
use febft_messages::serialize::{OrderingProtocolMessage, StateTransferMessage, ServiceMsg};
use febft_messages::state_transfer::Checkpoint;
use febft_messages::timeouts::Timeouts;
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::observer::ObserverHandle;
use crate::bft::sync::view::ViewInfo;

pub struct PBFTConfig<D: SharedData, ST> {
    pub node_id: NodeId,
    // pub observer_handle: ObserverHandle,
    pub follower_handle: Option<FollowerHandle<PBFTConsensus<D>>>,
    pub view: ViewInfo,
    pub timeout_dur: Duration,
    pub db_path: String,
    pub proposer_config: ProposerConfig,
    pub _phantom_data: PhantomData<ST>,
}

impl<D: SharedData + 'static,
    ST: StateTransferMessage + 'static> PBFTConfig<D, ST> {
    pub fn new(node_id: NodeId,
               follower_handle: Option<FollowerHandle<PBFTConsensus<D>>>,
               view: ViewInfo, timeout_dur: Duration,
               db_path: String, proposer_config: ProposerConfig) -> Self {
        Self {
            node_id,
            // observer_handle,
            follower_handle,
            view,
            timeout_dur,
            db_path,
            proposer_config,
            _phantom_data: Default::default(),
        }
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
