use febft_common::node_id::NodeId;
use crate::metrics::MetricKind;
use crate::metrics::metrics_thread::launch_metrics;

pub mod benchmarks;
pub mod metrics;

pub type MetricsArgsFunc = Box<dyn FnOnce(&mut MetricsArgs)>;

/// Arguments to the metrics module
pub struct MetricsArgs {
    pub metrics_registers: Vec<MetricRegistry>,
    pub concurrency: usize,
    pub metric_level: MetricLevel,
}

#[derive(Debug, Clone)]
pub struct InfluxDBArgs {
    pub ip: String,
    pub db_name: String,
    pub user: String,
    pub password: String,
    pub node_id: NodeId,
    pub extra: Option<String>
}

impl InfluxDBArgs {
    pub fn new(ip: String, db_name: String, user: String, password: String, node_id: NodeId, extra: Option<String>) -> Self {
        Self { ip, db_name, user, password, node_id, extra }
    }
}

/// The levels of metrics available for this
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub enum MetricLevel {
    Trace,
    Debug,
    Info
}

pub struct MetricRegistryInfo {
    index: usize,
    name: String,
    kind: MetricKind,
    level: MetricLevel,
    concurrency_override: Option<usize>
}

pub type MetricRegistry = MetricRegistryInfo;

fn default_metrics_args() -> MetricsArgs {
    MetricsArgs {
        metrics_registers: Vec::new(),
        concurrency: 2,
        metric_level: MetricLevel::Info,
    }
}

pub fn with_metrics(mut vec: Vec<MetricRegistry>) -> MetricsArgsFunc {
    Box::new(move |args| {
        args.metrics_registers.append(&mut vec);
    })
}

pub fn with_concurrency(concurrency: usize) -> MetricsArgsFunc {
    Box::new(move |args| {
        args.concurrency = concurrency;
    })
}

pub fn with_metric_level(level: MetricLevel) -> MetricsArgsFunc {
    Box::new(move |args| {
        args.metric_level = level;
    })
}

pub fn initialize_metrics(config: Vec<MetricsArgsFunc>, influxdb: InfluxDBArgs) {
    let mut args = default_metrics_args();

    for register in config {
        register(&mut args);
    }

    let metric_level = args.metric_level;

    metrics::init(args.metrics_registers, args.concurrency, metric_level);

    launch_metrics(influxdb.clone(), metric_level);

    metrics::os_mon::launch_os_mon(influxdb);
}

impl From<(usize, String, MetricKind)> for MetricRegistryInfo {
    fn from((index, name, kind): (usize, String, MetricKind)) -> Self {
        Self {
            index,
            name,
            kind,
            level: MetricLevel::Info,
            concurrency_override: None
        }
    }
}

impl From<(usize, String, MetricKind, MetricLevel)> for MetricRegistryInfo {
    fn from((index, name, kind, level): (usize, String, MetricKind, MetricLevel)) -> Self {
        Self {
            index,
            name,
            kind,
            level,
            concurrency_override: None
        }
    }
}

impl From<(usize, String, MetricKind, MetricLevel, usize)> for MetricRegistryInfo {
    fn from((index, name, kind, level, concurrency_override): (usize, String, MetricKind, MetricLevel, usize)) -> Self {
        Self {
            index,
            name,
            kind,
            level,
            concurrency_override: Some(concurrency_override)
        }
    }
}