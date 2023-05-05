use std::cell::Cell;
use std::iter;
use std::sync::Mutex;
use rand::Rng;
use thread_local::ThreadLocal;
use febft_common::globals::Global;

static mut METRICS: Global<Metrics> = Global::new();

macro_rules! metrics {
    () => {
        match unsafe { POOL.get() } {
	        Some(ref pool) => pool,
            None => panic!("Client thread pool wasn't initialized"),
        }
    }
}

/// Metrics for the general library.
pub struct Metrics {
    metrics: Vec<Metric>,
}

/// A metric statistic to be used. This will be collected every X seconds and sent to InfluxDB
pub struct Metric {
    name: String,
    values: Vec<Mutex<MetricType>>,
    round_robin: ThreadLocal<Cell<usize>>,
}

enum MetricType {
    Duration(Vec<u64>),
    Counter(u64),
}

pub enum MetricKind {
    Duration,
    Counter,
}

impl Metrics {
    fn new(registered_metrics: Vec<(String, MetricKind)>, concurrency: usize) -> Self {
        let mut metrics = Vec::with_capacity(registered_metrics.len());

        for (name, kind) in registered_metrics {
            metrics.push(Metric::new(name, kind, concurrency));
        }

        Self {
            metrics,
        }
    }
}

impl MetricKind {
    fn gen_metric_type(&self) -> MetricType {
        match self {
            MetricKind::Duration => {
                MetricType::Duration(Vec::new())
            }
            MetricKind::Counter => {
                MetricType::Counter(0)
            }
        }
    }
}

impl Metric {
    fn new(name: String, kind: MetricKind, concurrency: usize) -> Self {

        let values = iter::repeat_with(||
            Mutex::new(kind.gen_metric_type()))
            .take(concurrency)
            .collect();

        Self {
            name,
            values,
            round_robin: Default::default(),
        }
    }

    fn take_values(&self) -> Vec<MetricType> {
        let mut collected_values = Vec::with_capacity(self.values.len());

        for i in 0..self.values.len() {
            let mut value = self.values[i].lock().unwrap();

            let mt = match &*value {
                MetricType::Duration(vals) => {
                    MetricType::Duration(Vec::with_capacity(vals.len()))
                }
                MetricType::Counter(_) => {
                    MetricType::Counter(0)
                }
            };

            let metric_type = std::mem::replace(&mut *value, mt);

            collected_values.push(metric_type);
        }

        collected_values
    }
}

pub fn init(registered_metrics: Vec<(String, MetricKind)>, concurrency: usize) {
    unsafe {
        METRICS.set(Metrics::new(registered_metrics, concurrency));
    }
}

pub fn enqueue_duration_measurement(metric: &Metric, duration: u64) {
    let current = metric.round_robin.get_or(|| Cell::new(0));

    let current_val = current.get();

    let mut values = metric.values[current_val % metric.values.len()].lock().unwrap();

    current.set(current_val + 1);

    if let MetricType::Duration(ref mut v) = *values {
        v.push(duration);
    }
}

pub fn increment_counter_measurement(metric: &Metric) {
    let current = metric.round_robin.get_or(|| Cell::new(rand::thread_rng().gen_range(0..metric.values.len())));

    let current_val = current.get();

    let mut values = metric.values[current_val % metric.values.len()].lock().unwrap();

    current.set(current_val + 1);

    if let MetricType::Counter(ref mut v) = *values {
        *v = *v + 1;
    }
}

fn collect_measurements(metric: &Metric) -> Vec<MetricType> {
    let metric1 = metric.take_values();

    metric1
}