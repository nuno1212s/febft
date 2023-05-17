use febft_metrics::{MetricLevel, MetricRegistry};
use febft_metrics::metrics::MetricKind;

/// Replica will get the 5XX metrics codes

pub const ORDERING_PROTOCOL_POLL_TIME: &str = "ORDERING_PROTOCOL_POLL_TIME";
pub const ORDERING_PROTOCOL_POLL_TIME_ID: usize = 500;

pub const ORDERING_PROTOCOL_PROCESS_TIME: &str = "ORDERING_PROTOCOL_PROCESS_TIME";
pub const ORDERING_PROTOCOL_PROCESS_TIME_ID: usize = 501;

pub const STATE_TRANSFER_PROCESS_TIME: &str = "STATE_TRANSFER_PROCESS_TIME";
pub const STATE_TRANSFER_PROCESS_TIME_ID: usize = 502;

pub const TIMEOUT_PROCESS_TIME: &str = "TIMEOUT_PROCESS_TIME";
pub const TIMEOUT_PROCESS_TIME_ID: usize = 503;

pub const APP_STATE_DIGEST_TIME: &str = "APP_STATE_DIGEST_TIME";
pub const APP_STATE_DIGEST_TIME_ID: usize = 504;

pub const EXECUTION_LATENCY_TIME: &str = "EXECUTION_LATENCY";
pub const EXECUTION_LATENCY_TIME_ID: usize = 505;

pub const EXECUTION_TIME_TAKEN: &str = "EXECUTION_TIME_TAKEN";
pub const EXECUTION_TIME_TAKEN_ID: usize = 506;

pub const REPLIES_SENT_TIME: &str = "REPLY_SENT_TIME";
pub const REPLIES_SENT_TIME_ID: usize = 507;

pub const REPLIES_PASSING_TIME: &str = "REPLIES_PASSING_TIME";
pub const REPLIES_PASSING_TIME_ID: usize = 508;

pub fn metrics() -> Vec<MetricRegistry> {

    vec![
        (ORDERING_PROTOCOL_POLL_TIME_ID, ORDERING_PROTOCOL_POLL_TIME.to_string(), MetricKind::Duration, MetricLevel::Trace).into(),
        (ORDERING_PROTOCOL_PROCESS_TIME_ID, ORDERING_PROTOCOL_PROCESS_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (STATE_TRANSFER_PROCESS_TIME_ID, STATE_TRANSFER_PROCESS_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (TIMEOUT_PROCESS_TIME_ID, TIMEOUT_PROCESS_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (APP_STATE_DIGEST_TIME_ID, APP_STATE_DIGEST_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (EXECUTION_LATENCY_TIME_ID, EXECUTION_LATENCY_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (EXECUTION_TIME_TAKEN_ID, EXECUTION_TIME_TAKEN.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (REPLIES_SENT_TIME_ID, REPLIES_SENT_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
        (REPLIES_PASSING_TIME_ID, REPLIES_PASSING_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
    ]

}