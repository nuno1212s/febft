use febft_common::node_id::NodeId;
use crate::metrics::MetricKind;
use crate::metrics::metrics_thread::launch_metrics;

pub mod benchmarks;
pub mod metrics;

pub type MetricsArgsFunc = Box<dyn FnOnce(&mut MetricsArgs)>;

/// Arguments to the metrics module
pub struct MetricsArgs {
    pub metrics_registers: Vec<(usize, String, MetricKind)>,
    pub concurrency: usize,
}

#[derive(Debug)]
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

fn default_metrics_args() -> MetricsArgs {
    MetricsArgs {
        metrics_registers: Vec::new(),
        concurrency: 1,
    }
}

pub fn with_metrics(mut vec: Vec<(usize, String, MetricKind)>) -> MetricsArgsFunc {
    Box::new(move |args| {
        args.metrics_registers.append(&mut vec);
    })
}

pub fn with_concurrency(concurrency: usize) -> MetricsArgsFunc {
    Box::new(move |args| {
        args.concurrency = concurrency;
    })
}

pub fn initialize_metrics(config: Vec<MetricsArgsFunc>, influxdb: InfluxDBArgs) {
    let mut args = default_metrics_args();

    for register in config {
        register(&mut args);
    }

    metrics::init(args.metrics_registers, args.concurrency);

    launch_metrics(influxdb);
}

