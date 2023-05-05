pub mod metrics_thread;

use std::cell::Cell;
use std::iter;
use std::sync::Mutex;
use log::{error, info};
use rand::Rng;
use thread_local::ThreadLocal;
use febft_common::globals::Global;

static mut METRICS: Global<Metrics> = Global::new();

/// Metrics for the general library.
pub struct Metrics {
    live_indexes: Vec<usize>,
    metrics: Vec<Option<Metric>>,
}

/// A metric statistic to be used. This will be collected every X seconds and sent to InfluxDB
pub struct Metric {
    name: String,
    values: Vec<Mutex<MetricData>>,
    round_robin: ThreadLocal<Cell<usize>>,
}

/// Data for a given metric
#[derive(Debug)]
enum MetricData {
    Duration(Vec<u64>),
    Counter(u64),
}

/// The possible kinds of metrics
pub enum MetricKind {
    Duration,
    Counter,
}

impl Metrics {
    fn new(registered_metrics: Vec<(usize, String, MetricKind)>, concurrency: usize) -> Self {

        let mut largest_ind = 0;

        // Calculate the necessary size for the vector
        for (index, _, _) in &registered_metrics {
            largest_ind = std::cmp::max(largest_ind, *index);
        }

        let mut metrics = iter::repeat_with(|| None)
            .take(largest_ind + 1)
            .collect::<Vec<_>>();

        let mut live_indexes = Vec::with_capacity(registered_metrics.len());

        for (index, name, kind) in registered_metrics {
            metrics[index] = Some(Metric::new(name, kind, concurrency));

            live_indexes.push(index);
        }

        Self {
            live_indexes,
            metrics,
        }
    }
}

impl MetricKind {
    fn gen_metric_type(&self) -> MetricData {
        match self {
            MetricKind::Duration => {
                MetricData::Duration(Vec::new())
            }
            MetricKind::Counter => {
                MetricData::Counter(0)
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

    fn take_values(&self) -> Vec<MetricData> {
        let mut collected_values = Vec::with_capacity(self.values.len());

        for i in 0..self.values.len() {

            let metric_type = {
                let mut value = self.values[i].lock().unwrap();

                let mt = match &*value {
                    MetricData::Duration(vals) => {
                        MetricData::Duration(Vec::with_capacity(vals.len()))
                    }
                    MetricData::Counter(_) => {
                        MetricData::Counter(0)
                    }
                };
                std::mem::replace(&mut *value, mt)
            };

            collected_values.push(metric_type);
        }

        collected_values
    }
}

impl MetricData {
    fn merge(&mut self, other: Self) {
        match (self, other) {
            (MetricData::Duration(dur), MetricData::Duration(mut dur2)) => {
                dur.append(&mut dur2);
            }
            (MetricData::Counter(c), MetricData::Counter(c2)) => {
                *c += c2;
            }
            _ => panic!("Can't merge metrics of different types"),
        }
    }
}


/// Initialize the metrics module
pub(super) fn init(registered_metrics: Vec<(usize, String, MetricKind)>, concurrency: usize) {
    unsafe {
        METRICS.set(Metrics::new(registered_metrics, concurrency));
    }
}

/// Enqueue a duration measurement
pub(super) fn enqueue_duration_measurement(metric: &Metric, duration: u64) {
    let current = metric.round_robin.get_or(|| Cell::new(0));

    let current_val = current.get();

    let mut values = metric.values[current_val % metric.values.len()].lock().unwrap();

    current.set(current_val + 1);

    if let MetricData::Duration(ref mut v) = *values {
        v.push(duration);
    }
}

/// Enqueue a counter measurement
pub(super) fn increment_counter_measurement(metric: &Metric, counter: Option<u64>) {
    let current = metric.round_robin.get_or(|| Cell::new(rand::thread_rng().gen_range(0..metric.values.len())));

    let current_val = current.get();

    let mut values = metric.values[current_val % metric.values.len()].lock().unwrap();

    current.set(current_val + 1);

    if let MetricData::Counter(ref mut v) = *values {
        *v = *v + counter.unwrap_or(1);
    }
}

/// Collect all measurements from a given metric
fn collect_measurements(metric: &Metric) -> Vec<MetricData> {
    let metric1 = metric.take_values();

    metric1
}

/// Collect all measurements from all metrics
fn collect_all_measurements() -> Vec<(String, MetricData)> {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let mut collected_metrics = Vec::with_capacity(metrics.metrics.len());

            for index in &metrics.live_indexes {
                let metric = metrics.metrics[*index].as_ref().unwrap();

                collected_metrics.push((metric.name.clone(), collect_measurements(metric)));
            }

            let mut final_metric_data = Vec::with_capacity(collected_metrics.len());

            for (name, metric_data) in collected_metrics {
                let joined_metrics = join_metrics(metric_data);

                final_metric_data.push((name, joined_metrics));
            }

            final_metric_data
        },
        None => panic!("Metrics were not initialized"),
    }

}

/// Join all metrics into a single metric so we can push them into InfluxDB
fn join_metrics(mut metrics: Vec<MetricData>) -> MetricData {
    if metrics.is_empty() {
        panic!("No metrics to join")
    }

    let mut first = metrics.swap_remove(0);

    while !metrics.is_empty() {
        let metric = metrics.swap_remove(0);

        first.merge(metric)
    }

    first
}

pub fn metric_increment(metric: usize, counter: Option<u64>) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            increment_counter_measurement(&metric, counter);
        }
        None => {
            error!("Metrics not initialized");
        }
    }
}
