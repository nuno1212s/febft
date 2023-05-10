use std::time::Instant;
use febft_metrics::metrics::{metric_duration, metric_duration_end, metric_store_count, MetricKind};

/// Consensus will take the ID range 1XX, for now
///
/// 100 - 109: Proposer
pub const PROPOSER_BATCHES_MADE: &str = "BATCHES_MADE";
pub const PROPOSER_BATCHES_MADE_ID: usize = 100;

pub const PROPOSER_REQUESTS_COLLECTED: &str = "REQUESTS_COLLECTED";
pub const PROPOSER_REQUESTS_COLLECTED_ID: usize = 101;

pub const PROPOSER_REQUEST_PROCESSING_TIME: &str = "REQUEST_PROCESSING";
pub const PROPOSER_REQUEST_PROCESSING_TIME_ID: usize = 102;

pub const PROPOSER_REQUEST_FILTER_TIME: &str = "REQUEST_FILTER_TIME";
pub const PROPOSER_REQUEST_FILTER_TIME_ID: usize = 103;

pub const PROPOSER_FWD_REQUESTS : &str = "FWD_REQUEST_HANDLING";
pub const PROPOSER_FWD_REQUESTS_ID : usize = 104;

/// 110-119: Consensus

pub const PROPOSE_LATENCY: &str = "PROPOSE_LATENCY";
pub const PROPOSE_LATENCY_ID: usize = 110;

pub const CONSENSUS_PRE_PREPARE_LATENCY: &str = "PRE_PREPARE_LATENCY";
pub const CONSENSUS_PRE_PREPARE_LATENCY_ID: usize = 111;

pub const CONSENSUS_PREPARE_LATENCY: &str = "PREPARE_LATENCY";
pub const CONSENSUS_PREPARE_LATENCY_ID: usize = 112;

pub const CONSENSUS_COMMIT_LATENCY: &str = "COMMIT_LATENCY";
pub const CONSENSUS_COMMIT_LATENCY_ID: usize = 113;

pub const BATCH_SIZE: &str = "BATCH_SIZE";
pub const BATCH_SIZE_ID: usize = 114;

pub const PRE_PREPARE_ANALYSIS : &str = "PRE_PREPARE_RQ_ANALYSIS";
pub const PRE_PREPARE_ANALYSIS_ID: usize = 115;

pub const PRE_PREPARE_LOG_ANALYSIS : &str = "PRE_PREPARE_LOG_ANALYSIS";
pub const PRE_PREPARE_LOG_ANALYSIS_ID: usize = 116;

pub const OPERATIONS_PROCESSED: &str = "OPS_PER_SECOND";
pub const OPERATIONS_PROCESSED_ID: usize = 117;

pub fn metrics() -> Vec<(usize, String, MetricKind)> {
    
    vec![
        (PROPOSER_BATCHES_MADE_ID, PROPOSER_BATCHES_MADE.to_string(), MetricKind::Counter),
        (PROPOSER_REQUESTS_COLLECTED_ID, PROPOSER_REQUESTS_COLLECTED.to_string(), MetricKind::Counter),
        (PROPOSER_REQUEST_PROCESSING_TIME_ID, PROPOSER_REQUEST_PROCESSING_TIME.to_string(), MetricKind::Duration),
        (PROPOSER_REQUEST_FILTER_TIME_ID, PROPOSER_REQUEST_FILTER_TIME.to_string(), MetricKind::Duration),
        (PROPOSER_FWD_REQUESTS_ID, PROPOSER_FWD_REQUESTS.to_string(), MetricKind::Duration),
        (CONSENSUS_PRE_PREPARE_LATENCY_ID, CONSENSUS_PRE_PREPARE_LATENCY.to_string(), MetricKind::Duration),
        (PROPOSE_LATENCY_ID, PROPOSE_LATENCY.to_string(), MetricKind::Duration),
        (CONSENSUS_PRE_PREPARE_LATENCY_ID, CONSENSUS_PRE_PREPARE_LATENCY.to_string(), MetricKind::Duration),
        (CONSENSUS_PREPARE_LATENCY_ID, CONSENSUS_PREPARE_LATENCY.to_string(), MetricKind::Duration),
        (CONSENSUS_COMMIT_LATENCY_ID, CONSENSUS_COMMIT_LATENCY.to_string(), MetricKind::Duration),
        (BATCH_SIZE_ID, BATCH_SIZE.to_string(), MetricKind::Count),
        (PRE_PREPARE_ANALYSIS_ID, PRE_PREPARE_ANALYSIS.to_string(), MetricKind::Duration),
        (PRE_PREPARE_LOG_ANALYSIS_ID, PRE_PREPARE_LOG_ANALYSIS.to_string(), MetricKind::Duration),
        (OPERATIONS_PROCESSED_ID, OPERATIONS_PROCESSED.to_string(), MetricKind::Count),
    ]
    
}

#[derive(Debug, Clone)]
pub struct ConsensusMetrics {
    // At which time did we start the first consensus
    pub consensus_start_time: Instant,
    // At which time did we receive the first pre-prepare message
    pub first_pre_prepare_time: Instant,
    // At which time did we receive the quorum of pre-prepare message
    pub pre_prepare_recvd_time: Instant,
    // At which time did we receive the first prepare message
    pub first_prepare_rcvd_time: Instant,
    // At which time did we receive the quorum of prepare messages
    pub prepare_quorum_time: Instant,
    // At which time did we receive the first commit message
    pub first_commit_rcvd_time: Instant,
    // At which time did we receive the quorum of commit messages
    pub commit_quorum_time: Instant,
}

impl ConsensusMetrics {
    pub fn new() -> Self {
        ConsensusMetrics {
            consensus_start_time: Instant::now(),
            first_pre_prepare_time: Instant::now(),
            pre_prepare_recvd_time: Instant::now(),
            first_prepare_rcvd_time: Instant::now(),
            prepare_quorum_time: Instant::now(),
            first_commit_rcvd_time: Instant::now(),
            commit_quorum_time: Instant::now(),
        }
    }

    /// Methods to set each of the variables to the current instant
    pub fn consensus_started(&mut self) {
        self.consensus_start_time = Instant::now();
    }

    pub fn first_pre_prepare_recvd(&mut self) {
        self.first_pre_prepare_time = Instant::now();

        metric_duration(PROPOSE_LATENCY_ID,
                        self.consensus_start_time.elapsed());
    }

    pub fn all_pre_prepares_recvd(&mut self, batch_size: usize) {
        self.pre_prepare_recvd_time = Instant::now();

        metric_duration(CONSENSUS_PRE_PREPARE_LATENCY_ID,
                        self.first_pre_prepare_time.elapsed());
        metric_store_count(BATCH_SIZE_ID, batch_size);
    }

    pub fn first_prepare_recvd(&mut self) {
        self.first_prepare_rcvd_time = Instant::now();
    }

    pub fn prepare_quorum_recvd(&mut self) {
        self.prepare_quorum_time = Instant::now();

        metric_duration(CONSENSUS_PREPARE_LATENCY_ID,
                        self.first_prepare_rcvd_time.elapsed())
    }

    pub fn first_commit_recvd(&mut self) {
        self.first_commit_rcvd_time = Instant::now();
    }

    pub fn commit_quorum_recvd(&mut self) {
        self.commit_quorum_time = Instant::now();

        metric_duration(CONSENSUS_COMMIT_LATENCY_ID,
                        self.first_commit_rcvd_time.elapsed())
    }
}