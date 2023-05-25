use febft_metrics::{MetricLevel, MetricRegistry};
use febft_metrics::metrics::MetricKind;

/// Core frameworks will get 0XX metric ID

/// Request pre processing (010-019)
pub const RQ_PP_CLIENT_MSG: &str = "RQ_PRE_PROCESSING_CLIENT_MSGS";
pub const RQ_PP_CLIENT_MSG_ID: usize = 010;

pub const RQ_PP_CLIENT_COUNT: &str = "RQ_PRE_PROCESSING_CLIENT_COUNT";
pub const RQ_PP_CLIENT_COUNT_ID: usize = 011;

pub const RQ_PP_FWD_RQS: &str = "RQ_PRE_PROCESSING_FWD_RQS";
pub const RQ_PP_FWD_RQS_ID: usize = 012;

pub const RQ_PP_DECIDED_RQS: &str = "RQ_PRE_PROCESSING_DECIDED_RQS";
pub const RQ_PP_DECIDED_RQS_ID: usize = 013;

pub const RQ_PP_TIMEOUT_RQS: &str = "RQ_PRE_PROCESSING_TIMEOUT_RQS";
pub const RQ_PP_TIMEOUT_RQS_ID: usize = 014;

pub const RQ_PP_COLLECT_PENDING: &str = "RQ_PRE_PROCESSING_COLLECT_PENDING";
pub const RQ_PP_COLLECT_PENDING_ID: usize = 015;

pub const RQ_PP_CLONE_RQS: &str = "RQ_PRE_PROCESSING_CLONE_RQS";
pub const RQ_PP_CLONE_RQS_ID: usize = 016;

pub const RQ_PP_WORKER_ORDER_PROCESS: &str = "RQ_PRE_PROCESSING_WORKER_ORDERED_PROCESS";
pub const RQ_PP_WORKER_ORDER_PROCESS_ID: usize = 017;

pub const RQ_PP_WORKER_ORDER_PROCESS_COUNT: &str = "RQ_PRE_PROCESSING_WORKER_ORDERED_PROCESS_TIME";
pub const RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID: usize = 018;

pub const RQ_PP_WORKER_DECIDED_PROCESS_TIME: &str = "RQ_PRE_PROCESSING_WORKER_DECIDED_PROCESS_TIME";
pub const RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID: usize = 019;

pub const RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME: &str = "RQ_PRE_PROCESSING_ORCHESTRATOR_WORKER_PASSING_TIME";
pub const RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID: usize = 020;

pub const RQ_PP_WORKER_PROPOSER_PASSING_TIME: &str = "RQ_PRE_PROCESSING_WORKER_PROPOSER_PASSING_TIME";
pub const RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID: usize = 021;

pub const RQ_PP_WORKER_STOPPED_TIME: &str = "RQ_PRE_PROCESSING_WORKER_STOPPED_TIME";
pub const RQ_PP_WORKER_STOPPED_TIME_ID: usize = 022;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (RQ_PP_CLIENT_MSG_ID, RQ_PP_CLIENT_MSG.to_string(), MetricKind::Duration).into(),
        (RQ_PP_CLIENT_COUNT_ID, RQ_PP_CLIENT_COUNT.to_string(), MetricKind::Counter).into(),
        (RQ_PP_FWD_RQS_ID, RQ_PP_FWD_RQS.to_string(), MetricKind::Duration).into(),
        (RQ_PP_DECIDED_RQS_ID, RQ_PP_DECIDED_RQS.to_string(), MetricKind::Duration).into(),
        (RQ_PP_TIMEOUT_RQS_ID, RQ_PP_TIMEOUT_RQS.to_string(), MetricKind::Duration).into(),
        (RQ_PP_COLLECT_PENDING_ID, RQ_PP_COLLECT_PENDING.to_string(), MetricKind::Duration).into(),
        (RQ_PP_CLONE_RQS_ID, RQ_PP_CLONE_RQS.to_string(), MetricKind::Duration).into(),
        (RQ_PP_WORKER_ORDER_PROCESS_ID, RQ_PP_WORKER_ORDER_PROCESS.to_string(), MetricKind::Duration).into(),
        (RQ_PP_WORKER_ORDER_PROCESS_COUNT_ID, RQ_PP_WORKER_ORDER_PROCESS_COUNT.to_string(), MetricKind::Counter).into(),
        (RQ_PP_WORKER_DECIDED_PROCESS_TIME_ID, RQ_PP_WORKER_DECIDED_PROCESS_TIME.to_string(), MetricKind::Duration).into(),
        (RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME_ID, RQ_PP_ORCHESTRATOR_WORKER_PASSING_TIME.to_string(), MetricKind::Duration).into(),
        (RQ_PP_WORKER_PROPOSER_PASSING_TIME_ID, RQ_PP_WORKER_PROPOSER_PASSING_TIME.to_string(), MetricKind::Duration).into(),
        (RQ_PP_WORKER_STOPPED_TIME_ID, RQ_PP_WORKER_STOPPED_TIME.to_string(), MetricKind::Duration, MetricLevel::Debug).into(),
    ]
}