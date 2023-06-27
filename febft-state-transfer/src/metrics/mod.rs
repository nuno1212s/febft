use atlas_metrics::{MetricLevel, MetricRegistry};
use atlas_metrics::metrics::MetricKind;

/// State transfer will take the
/// 6XX metric ID range
pub const STATE_TRANSFER_STATE_INSTALL_CLONE_TIME : &str = "LT_STATE_CLONE_TIME";
pub const STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID : usize = 600;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID, STATE_TRANSFER_STATE_INSTALL_CLONE_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
    ]
}