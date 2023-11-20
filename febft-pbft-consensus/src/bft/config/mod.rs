use std::time::Duration;

use atlas_common::node_id::NodeId;
use atlas_core::followers::FollowerHandle;
use atlas_smr_application::serialize::ApplicationData;

use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::sync::view::ViewInfo;

pub struct PBFTConfig<D>
    where D: ApplicationData + 'static {
    pub node_id: NodeId,
    // pub observer_handle: ObserverHandle,
    pub follower_handle: Option<FollowerHandle<D, PBFTConsensus<D>, PBFTConsensus<D>>>,
    pub view: ViewInfo,
    pub timeout_dur: Duration,
    pub proposer_config: ProposerConfig,
    pub watermark: u32,
}

impl<D: ApplicationData + 'static, > PBFTConfig<D> {
    pub fn new(node_id: NodeId,
               follower_handle: Option<FollowerHandle<D, PBFTConsensus<D>, PBFTConsensus<D>>>,
               view: ViewInfo, timeout_dur: Duration,
               watermark: u32, proposer_config: ProposerConfig) -> Self {
        Self {
            node_id,
            // observer_handle,
            follower_handle,
            view,
            timeout_dur,
            proposer_config,
            watermark,
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
