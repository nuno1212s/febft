use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct PBFTConfig {
    pub timeout_dur: Duration,
    pub proposer_config: ProposerConfig,
    pub watermark: u32,
}

impl PBFTConfig {
    pub fn new(timeout_dur: Duration, watermark: u32, proposer_config: ProposerConfig) -> Self {
        Self {
            timeout_dur,
            proposer_config,
            watermark,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ProposerConfig {
    pub target_batch_size: u64,
    pub max_batch_size: u64,
    pub batch_timeout: u64,
    pub processing_threads: u32,
}

impl ProposerConfig {
    pub fn new(
        target_batch_size: u64,
        max_batch_size: u64,
        batch_timeout: u64,
        processing_threads: u32,
    ) -> Self {
        Self {
            target_batch_size,
            max_batch_size,
            batch_timeout,
            processing_threads,
        }
    }
}
