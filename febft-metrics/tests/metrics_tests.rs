#[cfg(test)]
mod metrics_tests {
    use std::time::Duration;
    use febft_common::{init, InitConfig};
    use febft_common::node_id::NodeId;
    use febft_metrics::{InfluxDBArgs, initialize_metrics, with_metrics};
    use febft_metrics::metrics::{metric_increment, MetricKind};

    const INFLUX_DB_IP: &str = "localhost:8086";
    const INFLUX_DB_NAME: &str = "atlas";
    const INFLUX_DB_USER: &str = "admin";

    const TEST_DATA_COLLECTION_POINT: &str = "TEST_DATA";
    const TEST_DATA_COLLECTION_POINT_ID: usize = 0;


    #[test]
    fn test_data_collection() {
        let option = unsafe {
            init(InitConfig {
                async_threads: 1,
                threadpool_threads: 1,
                id: None,
            }).expect("panic")
        };

        let influx_db_password = std::env::var("INFLUX_DB_PASSWORD").unwrap();

        let nodeId = NodeId::from(0u32);

        let influx_args = InfluxDBArgs::new(
            INFLUX_DB_IP.to_string(),
            INFLUX_DB_NAME.to_string(),
            INFLUX_DB_USER.to_string(),
            influx_db_password,
            nodeId,
        );

        println!("Connecting to InfluxDB with {:?}", influx_args);

        initialize_metrics(vec![with_metrics(vec![(TEST_DATA_COLLECTION_POINT_ID,
                                                   TEST_DATA_COLLECTION_POINT.to_string(), MetricKind::Counter)])], influx_args);

        metric_increment(TEST_DATA_COLLECTION_POINT_ID, None);

        std::thread::sleep(Duration::from_secs(2))
    }
}