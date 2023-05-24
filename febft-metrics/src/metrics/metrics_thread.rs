use std::time::Duration;

use chrono::{DateTime, Utc};
use influxdb::InfluxDbWriteable;
use log::info;

use febft_common::async_runtime as rt;
use febft_common::node_id::NodeId;

use crate::{InfluxDBArgs, MetricLevel};
use crate::metrics::{collect_all_measurements, MetricData};

#[derive(InfluxDbWriteable)]
pub struct MetricCounterReading {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    value: i64,
}

#[derive(InfluxDbWriteable)]
pub struct MetricDurationReading {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    value: f64,
    std_dev: f64,
}

#[derive(InfluxDbWriteable)]
pub struct MetricCountReading {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    value: f64,
    std_dev: f64,
}

pub fn launch_metrics(influx_args: InfluxDBArgs, metric_level: MetricLevel) {
    std::thread::spawn(move || {
        metric_thread_loop(influx_args, metric_level);
    });
}

/// The metrics thread. Collects all values from the metrics and sends them to influx db
pub fn metric_thread_loop(influx_args: InfluxDBArgs, metric_level: MetricLevel) {
    let InfluxDBArgs {
        ip, db_name, user, password, node_id, extra
    } = influx_args;

    let mut client = influxdb::Client::new(format!("{}", ip), db_name);

    client = client.with_auth(user, password);

    let host_name = format!("{:?}", node_id);

    let extra = extra.unwrap_or(String::from("None"));

    loop {
        let measurements = collect_all_measurements(&metric_level);

        let time = Utc::now();

        let mut readings = Vec::with_capacity(measurements.len());

        for (metric_name, results) in measurements {
            let query = match results {
                MetricData::Duration(dur) => {
                    if dur.is_empty() {
                        continue;
                    }

                    let duration_avg = mean(&dur[..]).unwrap_or(0.0);

                    let dur = std_deviation(&dur[..]).unwrap_or(0.0);

                    MetricDurationReading {
                        time,
                        host: host_name.clone(),
                        extra: extra.clone(),
                        value: duration_avg,
                        std_dev: dur,
                    }.into_query(metric_name)
                }
                MetricData::Counter(count) => {
                    MetricCounterReading {
                        time,
                        host: host_name.clone(),
                        extra: extra.clone(),
                        // Could lose some information, but the driver did NOT like u64
                        value: count as i64,
                    }.into_query(metric_name)
                }
                MetricData::Count(counts) => {
                    if counts.is_empty() {
                        continue;
                    }

                    let count_avg = mean_usize(&counts[..]).unwrap_or(0.0);

                    let dur = std_deviation_usize(&counts[..]).unwrap_or(0.0);

                    MetricCountReading {
                        time,
                        host: host_name.clone(),
                        extra: extra.clone(),
                        value: count_avg,
                        std_dev: dur,
                    }.into_query(metric_name)
                }
            };

            readings.push(query);
        }

        let result = rt::block_on(client.query(readings)).expect("Failed to write metrics to influxdb");

        info!("Result of writing metrics: {:?}", result);

        std::thread::sleep(Duration::from_millis(1000));
    }
}

fn mean(data: &[u64]) -> Option<f64> {
    let sum = data.iter().sum::<u64>() as f64;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation(data: &[u64]) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f64);

                diff * diff
            }).sum::<f64>() / count as f64;

            Some(variance.sqrt())
        },
        _ => None
    }
}

fn mean_usize(data: &[usize]) -> Option<f64> {
    let sum = data.iter().sum::<usize>() as f64;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation_usize(data: &[usize]) -> Option<f64> {
    match (mean_usize(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f64);

                diff * diff
            }).sum::<f64>() / count as f64;

            Some(variance.sqrt())
        },
        _ => None
    }
}