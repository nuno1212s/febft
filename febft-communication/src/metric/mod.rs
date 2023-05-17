use febft_metrics::{MetricLevel, MetricRegistry};
use febft_metrics::metrics::MetricKind;

/// The communication system will have the IDs 4XX

pub const COMM_RQS_RECVD: &str = "COMM_RQS_RECVD";
pub const COMM_RQS_RECVD_ID: usize = 400;

pub const COMM_RQS_SENT: &str = "COMM_RQS_SENT";
pub const COMM_RQS_SENT_ID: usize = 401;

pub const COMM_SERIALIZE_SIGN_TIME: &str = "COMM_SERIALIZE_AND_SIGN_TIME";
pub const COMM_SERIALIZE_SIGN_TIME_ID: usize = 402;

pub const COMM_DESERIALIZE_VERIFY_TIME: &str = "COMM_DESERIALIZE_AND_VERIFY_TIME";
pub const COMM_DESERIALIZE_VERIFY_TIME_ID: usize = 403;

pub const COMM_REQUEST_SEND_TIME: &str = "REQUEST_SEND_TIME";
pub const COMM_REQUEST_SEND_TIME_ID: usize = 407;

pub const COMM_RQ_SEND_PASSING_TIME : &str = "COMM_RQ_SEND_PASSING_TIME";
pub const COMM_RQ_SEND_PASSING_TIME_ID: usize = 409;

pub const COMM_RQ_TIME_SPENT_IN_MOD: &str = "COMM_RQ_TIME_SPENT_IN_COMM";
pub const COMM_RQ_TIME_SPENT_IN_MOD_ID: usize = 410;

pub const COMM_RQ_SEND_CLI_PASSING_TIME: &str = "COMM_RQ_SEND_CLI_PASSING_TIME";
pub const COMM_RQ_SEND_CLI_PASSING_TIME_ID: usize = 411;

pub const CLIENT_POOL_COLLECT_TIME: &str = "CLIENT_POOL_COLLECT_TIME";
pub const CLIENT_POOL_COLLECT_TIME_ID: usize = 404;

pub const CLIENT_POOL_BATCH_PASSING_TIME: &str = "CLIENT_POOL_BATCH_PASSING_TIME";
pub const CLIENT_POOL_BATCH_PASSING_TIME_ID: usize = 405;

pub const REPLICA_RQ_PASSING_TIME: &str = "REPLICA_RQ_PASSING_TIME";
pub const REPLICA_RQ_PASSING_TIME_ID: usize = 406;

pub const THREADPOOL_PASS_TIME: &str = "THREADPOOL_PASS_TIME";
pub const THREADPOOL_PASS_TIME_ID: usize = 408;


pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (COMM_RQS_RECVD_ID, COMM_RQS_RECVD.to_string(), MetricKind::Counter, MetricLevel::Trace, 8).into(),
        (COMM_RQS_SENT_ID, COMM_RQS_SENT.to_string(), MetricKind::Counter, MetricLevel::Trace, 8).into(),
        (COMM_SERIALIZE_SIGN_TIME_ID, COMM_SERIALIZE_SIGN_TIME.to_string(), MetricKind::Duration).into(),
        (COMM_DESERIALIZE_VERIFY_TIME_ID, COMM_DESERIALIZE_VERIFY_TIME.to_string(), MetricKind::Duration).into(),
        (CLIENT_POOL_COLLECT_TIME_ID, CLIENT_POOL_COLLECT_TIME.to_string(), MetricKind::Duration).into(),
        (CLIENT_POOL_BATCH_PASSING_TIME_ID, CLIENT_POOL_BATCH_PASSING_TIME.to_string(), MetricKind::Duration).into(),
        (REPLICA_RQ_PASSING_TIME_ID, REPLICA_RQ_PASSING_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug, 8).into(),
        (COMM_REQUEST_SEND_TIME_ID, COMM_REQUEST_SEND_TIME.to_string(), MetricKind::Duration, MetricLevel::Trace, 8).into(),
        (THREADPOOL_PASS_TIME_ID, THREADPOOL_PASS_TIME.to_string(), MetricKind::Duration, MetricLevel::Trace, 16).into(),
        (COMM_RQ_SEND_PASSING_TIME_ID, COMM_RQ_SEND_PASSING_TIME.to_string(), MetricKind::Duration, MetricLevel::Trace, 8).into(),
        (COMM_RQ_TIME_SPENT_IN_MOD_ID, COMM_RQ_TIME_SPENT_IN_MOD.to_string(), MetricKind::Duration, MetricLevel::Trace, 8).into(),
        (COMM_RQ_SEND_CLI_PASSING_TIME_ID, COMM_RQ_SEND_CLI_PASSING_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug, 8).into()
    ]
}