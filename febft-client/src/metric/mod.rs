use febft_metrics::MetricRegistry;
use febft_metrics::metrics::MetricKind;

/// Client Metrics module
/// We will take the 3XX range of metric IDs

pub const CLIENT_RQ_PER_SECOND: &str = "CLIENT_RQ_PER_SECOND";
pub const CLIENT_RQ_PER_SECOND_ID: usize = 300;

pub const CLIENT_RQ_LATENCY: &str = "CLIENT_RQ_LATENCY";
pub const CLIENT_RQ_LATENCY_ID: usize = 301;

pub const CLIENT_RQ_SEND_TIME: &str = "CLIENT_RQ_SEND_TIME";
pub const CLIENT_RQ_SEND_TIME_ID: usize = 302;

pub const CLIENT_RQ_RECV_TIME: &str = "CLIENT_RQ_RECV_TIME";
pub const CLIENT_RQ_RECV_TIME_ID: usize = 303;

pub const CLIENT_RQ_RECV_PER_SECOND: &str = "CLIENT_RQ_RECV_PER_SECOND";
pub const CLIENT_RQ_RECV_PER_SECOND_ID: usize = 304;

pub const CLIENT_RQ_DELIVER_RESPONSE: &str = "CLIENT_RQ_DELIVER_RESPONSE";
pub const CLIENT_RQ_DELIVER_RESPONSE_ID: usize = 305;

pub fn metrics() -> Vec<MetricRegistry> {

    vec! [
        (CLIENT_RQ_PER_SECOND_ID, CLIENT_RQ_PER_SECOND.to_string(), MetricKind::Counter),
        (CLIENT_RQ_LATENCY_ID, CLIENT_RQ_LATENCY.to_string(), MetricKind::Duration),
        (CLIENT_RQ_SEND_TIME_ID, CLIENT_RQ_SEND_TIME.to_string(), MetricKind::Duration),
        (CLIENT_RQ_RECV_TIME_ID, CLIENT_RQ_RECV_TIME.to_string(), MetricKind::Duration),
        (CLIENT_RQ_RECV_PER_SECOND_ID, CLIENT_RQ_RECV_PER_SECOND.to_string(), MetricKind::Counter),
        (CLIENT_RQ_DELIVER_RESPONSE_ID, CLIENT_RQ_DELIVER_RESPONSE.to_string(), MetricKind::Duration),
    ]
}