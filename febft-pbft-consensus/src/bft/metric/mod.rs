use atlas_metrics::metrics::{
    metric_duration, metric_store_count, MetricKind,
};
use atlas_metrics::{MetricLevel, MetricRegistry};
use std::time::Instant;

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

pub const PROPOSER_FWD_REQUESTS: &str = "FWD_REQUEST_HANDLING";
pub const PROPOSER_FWD_REQUESTS_ID: usize = 104;

pub const CLIENT_POOL_BATCH_SIZE: &str = "CLIENT_POOL_BATCH_SIZE";
pub const CLIENT_POOL_BATCH_SIZE_ID: usize = 105;

/// How long between proposes is being taken
pub const PROPOSER_LATENCY: &str = "PROPOSER_LATENCY";
pub const PROPOSER_LATENCY_ID: usize = 106;

pub const PROPOSER_PROPOSE_TIME: &str = "PROPOSER_PROPOSE_TIME";
pub const PROPOSER_PROPOSE_TIME_ID: usize = 107;

pub const PROPOSER_REQUEST_TIME_ITERATIONS: &str = "PROPOSER_REQUEST_TIME_ITERATIONS";
pub const PROPOSER_REQUEST_TIME_ITERATIONS_ID: usize = 108;
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

pub const PRE_PREPARE_ANALYSIS: &str = "PRE_PREPARE_RQ_ANALYSIS";
pub const PRE_PREPARE_ANALYSIS_ID: usize = 115;

pub const PRE_PREPARE_LOG_ANALYSIS: &str = "PRE_PREPARE_LOG_ANALYSIS";
pub const PRE_PREPARE_LOG_ANALYSIS_ID: usize = 116;

pub const OPERATIONS_ORDERED: &str = "OPERATIONS_ORDERED";
pub const OPERATIONS_ORDERED_ID: usize = 117;

pub const CONSENSUS_INSTALL_STATE_TIME: &str = "CONSENSUS_INSTALL_STATE_TIME";
pub const CONSENSUS_INSTALL_STATE_TIME_ID: usize = 118;

pub const MSG_LOG_INSTALL_TIME: &str = "MSG_LOG_INSTALL_TIME";
pub const MSG_LOG_INSTALL_TIME_ID: usize = 119;

/// 120-129: Synchronizer
pub const SYNC_WATCH_REQUESTS: &str = "SYNC_WATCH_REQUESTS";
pub const SYNC_WATCH_REQUESTS_ID: usize = 120;

pub const SYNC_BATCH_RECEIVED: &str = "SYNC_BATCH_RECEIVED";
pub const SYNC_BATCH_RECEIVED_ID: usize = 121;

pub const SYNC_STOPPED_REQUESTS: &str = "SYNC_STOPPED_REQUESTS";
pub const SYNC_STOPPED_REQUESTS_ID: usize = 122;

pub const SYNC_STOPPED_COUNT: &str = "SYNC_REQUESTS_COUNT";
pub const SYNC_STOPPED_COUNT_ID: usize = 123;

pub const SYNC_FORWARDED_REQUESTS: &str = "SYNC_FORWARDED_REQUESTS";
pub const SYNC_FORWARDED_REQUESTS_ID: usize = 124;

pub const SYNC_FORWARDED_COUNT: &str = "SYNC_FORWARDED_COUNT";
pub const SYNC_FORWARDED_COUNT_ID: usize = 125;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (
            PROPOSER_BATCHES_MADE_ID,
            PROPOSER_BATCHES_MADE.to_string(),
            MetricKind::Counter,
            MetricLevel::Info,
            1,
        )
            .into(),
        (
            PROPOSER_REQUESTS_COLLECTED_ID,
            PROPOSER_REQUESTS_COLLECTED.to_string(),
            MetricKind::Counter,
            MetricLevel::Info,
            1,
        )
            .into(),
        (
            PROPOSER_REQUEST_PROCESSING_TIME_ID,
            PROPOSER_REQUEST_PROCESSING_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSER_REQUEST_FILTER_TIME_ID,
            PROPOSER_REQUEST_FILTER_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSER_FWD_REQUESTS_ID,
            PROPOSER_FWD_REQUESTS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSER_PROPOSE_TIME_ID,
            PROPOSER_PROPOSE_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSER_REQUEST_TIME_ITERATIONS_ID,
            PROPOSER_REQUEST_TIME_ITERATIONS.to_string(),
            MetricKind::Counter,
        )
            .into(),
        (
            CLIENT_POOL_BATCH_SIZE_ID,
            CLIENT_POOL_BATCH_SIZE.to_string(),
            MetricKind::Count,
        )
            .into(),
        (
            CONSENSUS_PRE_PREPARE_LATENCY_ID,
            CONSENSUS_PRE_PREPARE_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSER_LATENCY_ID,
            PROPOSER_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PROPOSE_LATENCY_ID,
            PROPOSE_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            CONSENSUS_PRE_PREPARE_LATENCY_ID,
            CONSENSUS_PRE_PREPARE_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            CONSENSUS_PREPARE_LATENCY_ID,
            CONSENSUS_PREPARE_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            CONSENSUS_COMMIT_LATENCY_ID,
            CONSENSUS_COMMIT_LATENCY.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (BATCH_SIZE_ID, BATCH_SIZE.to_string(), MetricKind::Count).into(),
        (
            PRE_PREPARE_ANALYSIS_ID,
            PRE_PREPARE_ANALYSIS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            PRE_PREPARE_LOG_ANALYSIS_ID,
            PRE_PREPARE_LOG_ANALYSIS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            OPERATIONS_ORDERED_ID,
            OPERATIONS_ORDERED.to_string(),
            MetricKind::Counter,
        )
            .into(),
        (
            CONSENSUS_INSTALL_STATE_TIME_ID,
            CONSENSUS_INSTALL_STATE_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            MSG_LOG_INSTALL_TIME_ID,
            MSG_LOG_INSTALL_TIME.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            SYNC_WATCH_REQUESTS_ID,
            SYNC_WATCH_REQUESTS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            SYNC_BATCH_RECEIVED_ID,
            SYNC_BATCH_RECEIVED.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            SYNC_STOPPED_REQUESTS_ID,
            SYNC_STOPPED_REQUESTS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            SYNC_STOPPED_COUNT_ID,
            SYNC_STOPPED_COUNT.to_string(),
            MetricKind::Counter,
        )
            .into(),
        (
            SYNC_FORWARDED_REQUESTS_ID,
            SYNC_FORWARDED_REQUESTS.to_string(),
            MetricKind::Duration,
        )
            .into(),
        (
            SYNC_FORWARDED_COUNT_ID,
            SYNC_FORWARDED_COUNT.to_string(),
            MetricKind::Counter,
        )
            .into(),
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

impl Default for ConsensusMetrics {
    fn default() -> Self {
        Self::new()
    }
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

        metric_duration(PROPOSE_LATENCY_ID, self.consensus_start_time.elapsed());
    }

    pub fn all_pre_prepares_recvd(&mut self, batch_size: usize) {
        self.pre_prepare_recvd_time = Instant::now();

        metric_duration(
            CONSENSUS_PRE_PREPARE_LATENCY_ID,
            self.first_pre_prepare_time.elapsed(),
        );
        metric_store_count(BATCH_SIZE_ID, batch_size);
    }

    pub fn first_prepare_recvd(&mut self) {
        self.first_prepare_rcvd_time = Instant::now();
    }

    pub fn prepare_quorum_recvd(&mut self) {
        self.prepare_quorum_time = Instant::now();

        metric_duration(
            CONSENSUS_PREPARE_LATENCY_ID,
            self.first_prepare_rcvd_time.elapsed(),
        )
    }

    pub fn first_commit_recvd(&mut self) {
        self.first_commit_rcvd_time = Instant::now();
    }

    pub fn commit_quorum_recvd(&mut self) {
        self.commit_quorum_time = Instant::now();

        metric_duration(
            CONSENSUS_COMMIT_LATENCY_ID,
            self.first_commit_rcvd_time.elapsed(),
        )
    }
}
