use std::time::Duration;
use chrono::{DateTime, Utc};
use influxdb::InfluxDbWriteable;
use febft_common::async_runtime as rt;
use febft_common::node_id::NodeId;
use crate::InfluxDBArgs;

/// OS Metrics
pub const OS_CPU_USER: &str = "OS_CPU_USER";

pub const OS_RAM_USAGE: &str = "OS_RAM_USAGE";

pub const OS_NETWORK_UP: &str = "OS_NETWORK_UP";

pub const OS_NETWORK_DOWN: &str = "OS_NETWORK_DOWN";

#[derive(InfluxDbWriteable)]
pub struct MetricCPUReading {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    #[influxdb(tag)] cpu: i32,
    value: f64,
}

#[derive(InfluxDbWriteable)]
pub struct MetricRAMUsage {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    value: i64,
}


#[derive(InfluxDbWriteable)]
pub struct MetricNetworkSpeed {
    time: DateTime<Utc>,
    #[influxdb(tag)] host: String,
    #[influxdb(tag)] extra: String,
    value: f64,
}


pub fn launch_os_mon(influx_args: InfluxDBArgs) {
    std::thread::spawn(move || {
        metric_thread_loop(influx_args);
    });
}

/// The metrics thread. Collects all values from the
pub fn metric_thread_loop(influx_args: InfluxDBArgs) {
    let InfluxDBArgs {
        ip, db_name, user, password, node_id, extra
    } = influx_args;

    let mut client = influxdb::Client::new(format!("{}", ip), db_name);

    client = client.with_auth(user, password);

    let host_name = format!("{:?}", node_id);

    let extra = extra.unwrap_or(String::from("None"));

    loop {
        let time = Utc::now();
        let mut readings = Vec::new();

        let result = mprober_lib::cpu::get_all_cpu_utilization_in_percentage(false,
                                                                             Duration::from_millis(250)).unwrap();

        let mut curr_cpu = 0;

        for usage in result {
            let reading = MetricCPUReading {
                time,
                host: host_name.clone(),
                extra: extra.clone(),
                cpu: curr_cpu,
                value: usage,
            }.into_query(OS_CPU_USER);

            readings.push(reading);

            curr_cpu += 1;
        }

        let network_speed = mprober_lib::network::get_networks_with_speed(Duration::from_millis(250)).unwrap();

        let mut tx_speed = 0.0;
        let mut rx_speed = 0.0;

        for (_network, speed) in network_speed {
            //Only capture the most used one.
            rx_speed += speed.receive;
            tx_speed += speed.transmit;
        }

        readings.push(MetricNetworkSpeed {
            time,
            host: host_name.clone(),
            extra: extra.clone(),
            value: tx_speed,
        }.into_query(OS_NETWORK_UP));

        readings.push(MetricNetworkSpeed {
            time,
            host: host_name.clone(),
            extra: extra.clone(),
            value: rx_speed,
        }.into_query(OS_NETWORK_DOWN));

        let memory_stats = procinfo::pid::statm_self().unwrap();

        readings.push(MetricRAMUsage {
            time,
            host: host_name.clone(),
            extra: extra.clone(),
            value: (memory_stats.data * 4096) as i64,
        }.into_query(OS_RAM_USAGE));

        let result = rt::block_on(client.query(readings)).expect("Failed to write metrics to influxdb");

        std::thread::sleep(Duration::from_millis(250));
    }
}



