use std::cell::Cell;
use std::iter;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use log::{error, info};
use rand::Rng;
use thread_local::ThreadLocal;

use febft_common::globals::Global;

pub mod metrics_thread;

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
    additional_data: Mutex<AdditionalMetricData>,
    round_robin: ThreadLocal<Cell<usize>>,
}

/// Data for a given metric
#[derive(Debug)]
enum MetricData {
    Duration(Vec<u64>),
    Counter(u64),
    Count(Vec<usize>)
}

#[derive(Debug)]
/// Additional data that might be useful for a metric
/// For example, duration metrics need to know when they started for
/// when we are measuring durations that are not in the same scope (without having to
/// pass ugly arguments and such around)
pub enum AdditionalMetricData {
    Duration(Option<Instant>),
    // Counter does not need any additional data storage
    Counter,
    Count
}

/// The possible kinds of metrics
#[derive(Debug)]
pub enum MetricKind {
    Duration,
    /// A counter is a metric that is incremented by X every time it is called and in the end is combined
    Counter,
    /// A count is to be used to store various independent counts and then average them together
    Count
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
            MetricKind::Count => {
                MetricData::Count(Vec::new())
            }
        }
    }

    fn gen_additional_data(&self) -> AdditionalMetricData {
        match self {
            MetricKind::Duration => {
                AdditionalMetricData::Duration(None)
            }
            MetricKind::Counter => {
                AdditionalMetricData::Counter
            }
            MetricKind::Count => {
                AdditionalMetricData::Count
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
            additional_data: Mutex::new(kind.gen_additional_data()),
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
                    MetricData::Count(vals) => {
                        MetricData::Count(Vec::with_capacity(vals.len()))
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

#[inline]
fn get_current_round_robin(metric: &Metric) -> usize {
    let current = metric.round_robin.get_or(|| Cell::new(rand::thread_rng().gen_range(0..metric.values.len())));

    let result = current.get();

    current.set(result + 1);

    result
}

/// Enqueue a duration measurement
#[inline]
fn enqueue_duration_measurement(metric: &Metric, duration: u64) {
    let round_robin = get_current_round_robin(metric);

    let mut values = metric.values[round_robin % metric.values.len()].lock().unwrap();

    if let MetricData::Duration(ref mut v) = *values {
        v.push(duration);
    }
}


#[inline]
fn enqueue_counter_measurement(metric: &Metric, count: usize) {
    let round_robin = get_current_round_robin(metric);

    let mut values = metric.values[round_robin % metric.values.len()].lock().unwrap();

    if let MetricData::Count(ref mut v) = *values {
        v.push(count)
    }
}

#[inline]
fn start_duration_measurement(metric: &Metric) {
    let mut metric_guard = metric.additional_data.lock().unwrap();

    match &mut *metric_guard {
        AdditionalMetricData::Duration(start) => {
            start.replace(Instant::now());
        }
        AdditionalMetricData::Counter => {}
        AdditionalMetricData::Count => {}
    }
}

#[inline]
fn end_duration_measurement(metric: &Metric) {
    let start_instant = {
        let mut metric_guard = metric.additional_data.lock().unwrap();

        match &mut *metric_guard {
            AdditionalMetricData::Duration(start) => {
                start.take()
            }
            AdditionalMetricData::Counter => {
                None
            }
            AdditionalMetricData::Count => {
                None
            }
        }
    };

    if let Some(start_instant) = start_instant {
        enqueue_duration_measurement(metric, start_instant.elapsed().as_nanos() as u64);
    }
}

/// Enqueue a counter measurement
#[inline]
fn increment_counter_measurement(metric: &Metric, counter: Option<u64>) {
    let round_robin = get_current_round_robin(metric);

    let mut values = metric.values[round_robin % metric.values.len()].lock().unwrap();

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
        }
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

#[inline]
pub fn metric_local_duration_start() -> Instant {
    Instant::now()
}

#[inline]
pub fn metric_local_duration_end(metric: usize, start: Instant) {
    metric_duration(metric, start.elapsed());
}

#[inline]
pub fn metric_duration_start(metric: usize) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            start_duration_measurement(metric)
        }
        None => {
            //TODO: Remove this
            error!("Metrics not initialized");
        }
    }
}

#[inline]
pub fn metric_duration_end(metric: usize) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            end_duration_measurement(metric)
        }
        None => {
            //TODO: Remove this
            error!("Metrics not initialized");
        }
    }
}

#[inline]
pub fn metric_duration(metric: usize, duration: Duration) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            enqueue_duration_measurement(&metric, duration.as_nanos() as u64);
        }
        None => {
            //TODO: Remove this
            error!("Metrics not initialized");
        }
    }
}

#[inline]
pub fn metric_increment(metric: usize, counter: Option<u64>) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            increment_counter_measurement(&metric, counter);
        }
        None => {
            //TODO: Remove this
            error!("Metrics not initialized");
        }
    }
}

#[inline]
pub fn metric_store_count(metric: usize, amount: usize) {
    match unsafe { METRICS.get() } {
        Some(ref metrics) => {
            let metric = metrics.metrics[metric].as_ref().unwrap();

            enqueue_counter_measurement(metric, amount);
        }
        None => {
            //TODO: Remove this
            error!("Metrics not initialized");
        }
    }
}
