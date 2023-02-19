use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

use crate::bft::communication::NodeId;

pub struct Measurements {
    pub total_latency: BenchmarkHelper,
    pub consensus_latency: BenchmarkHelper,
    pub pre_cons_latency: BenchmarkHelper,
    pub pos_cons_latency: BenchmarkHelper,
    pub pre_prepare_latency: BenchmarkHelper,
    pub prepare_latency: BenchmarkHelper,
    pub commit_latency: BenchmarkHelper,
    pub batch_size: BenchmarkHelper,
    pub prepare_msg_latency: BenchmarkHelper,
    pub propose_time_latency: BenchmarkHelper,
    pub message_recv_latency: BenchmarkHelper,
    //Time taken since the first prepare message was received until the prepare phase is done
    pub prepare_time_taken: BenchmarkHelper,
    //Time taken since the first commit message was received until the consensus is finished
    pub commit_time_taken: BenchmarkHelper,
}

pub struct CommStats {
    client_comm: Option<CommStatsHelper>,
    replica_comm: CommStatsHelper,
    first_cli: NodeId,
    node_id: NodeId
}

struct CommStatsHelper {
    node_id: NodeId,
    requests_received: AtomicUsize,
    requests_sent: AtomicUsize,
    requests_last_mark: Mutex<(Instant, usize, usize)>,
    measurement_interval: usize,
    info: String,

    //Time since instructed to send the request and the actual sending of the request
    pub message_passing_time_taken: Vec<Mutex<BenchmarkHelper>>,
    //Time since instructed to send the request and the actual sending of the request
    pub message_passing_time_taken_own: Vec<Mutex<BenchmarkHelper>>,
    //Time taken to send the message
    pub message_sending_time_taken: Vec<Mutex<BenchmarkHelper>>,
    //Time taken to send the message to ourselves
    pub message_sending_time_taken_own: Vec<Mutex<BenchmarkHelper>>,
    //Time taken to sign the requests
    pub message_signing_time_taken: Vec<Mutex<BenchmarkHelper>>,
    //Time taken to create send to objects
    pub message_send_to_create: Vec<Mutex<BenchmarkHelper>>,
    //Time taken to pass from threadpool to each individual thread
    pub message_passing_to_send_thread: Vec<Mutex<BenchmarkHelper>>
}

impl CommStats {
    pub fn new(owner_id: NodeId, first_cli: NodeId, measurement_interval: usize) -> Self {
        Self {
            client_comm: if owner_id < first_cli { Some(CommStatsHelper::new(owner_id, String::from("Clients"), measurement_interval)) } else { None },
            replica_comm: CommStatsHelper::new(owner_id, String::from("Replicas"), measurement_interval),
            first_cli,
            node_id: owner_id
        }
    }

    pub fn insert_message_signing_time(&self, dest: NodeId, time: u128) {
        if dest > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.insert_message_signing_time(time)
                }
            }
        } else {
            self.replica_comm.insert_message_signing_time(time)
        }
    }

    pub fn insert_message_send_to_create_time(&self, dest: NodeId, time: u128) {
        if dest > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.insert_message_send_to_create(time);
                }
            }
        } else {
            self.replica_comm.insert_message_send_to_create(time)
        }
    }

    pub fn insert_message_passing_latency_own(&self, time: u128) {

        if self.node_id > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.insert_message_passing_time_own(time);
                }
            }
        } else {
            self.replica_comm.insert_message_passing_time_own(time)
        }

    }


    pub fn insert_message_passing_latency(&self, dest: NodeId, time: u128) {

        if dest > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.insert_message_passing_time(time);
                }
            }
        } else {
            self.replica_comm.insert_message_passing_time(time)
        }

    }

    pub fn insert_message_sending_time_own(&self, time: u128) {

        if self.node_id > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.insert_message_sending_time_own(time);
                }
            }
        } else {
            self.replica_comm.insert_message_sending_time_own(time)
        }

    }

    pub fn insert_message_sending_time(&self, dest: NodeId, time: u128) {

        if dest > self.first_cli {
            match &self.client_comm {
                None => {
                }
                Some(stats) => {
                    stats.insert_message_sending_time(time);
                }
            }
        } else {
            self.replica_comm.insert_message_sending_time(time)
        }

    }

    pub fn insert_message_passing_to_send_thread(&self, dest: NodeId, time: u128) {
        if dest > self.first_cli {
            match &self.client_comm {
                None => {
                }
                Some(stats) => {
                    stats.insert_message_passing_to_send_thread(time);
                }
            }
        } else {
            self.replica_comm.insert_message_passing_to_send_thread(time)
        }

    }

    pub fn register_rq_received(&self, sender: NodeId) {
        if sender > self.first_cli {
            match &self.client_comm {
                None => {

                }
                Some(stats) => {
                    stats.register_rq_received();
                }
            }
        } else {
            self.replica_comm.register_rq_received();
        }
    }

    pub fn register_rq_sent(&self, destination: NodeId) {
        if destination > self.first_cli {
            match &self.client_comm {
                None => {}
                Some(stats) => {
                    stats.register_rq_sent();
                }
            }
        } else {
            self.replica_comm.register_rq_sent();
        }
    }
}

impl CommStatsHelper {
    const CONCURRENCY_LEVEL: u128 = 10;

    pub fn new(owner_id: NodeId, info: String, measurement_interval: usize) -> Self {
        let concurrency_level = Self::CONCURRENCY_LEVEL as usize;

        Self {
            node_id: owner_id,
            requests_received: AtomicUsize::new(0),
            requests_sent: AtomicUsize::new(0),
            requests_last_mark: Mutex::new((Instant::now(), 0, 0)),
            info,
            measurement_interval,
            message_passing_time_taken: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_passing_time_taken_own: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_sending_time_taken: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_sending_time_taken_own: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_signing_time_taken: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_send_to_create: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
            message_passing_to_send_thread: std::iter::repeat_with(|| {
                Mutex::new(BenchmarkHelper::new(owner_id, measurement_interval / concurrency_level))
            }
            ).take(concurrency_level).collect(),
        }
    }

    fn insert_value(dest: &Vec<Mutex<BenchmarkHelper>>, time: u128) {
        let bucket = (time % Self::CONCURRENCY_LEVEL) as usize;

        dest[bucket].lock().values.push(time);
    }

    pub fn insert_message_passing_time(&self, time: u128) {
        Self::insert_value(&self.message_passing_time_taken, time);
    }

    pub fn insert_message_passing_time_own(&self, time: u128) {
        Self::insert_value(&self.message_passing_time_taken_own, time);
    }

    pub fn insert_message_passing_to_send_thread(&self, time: u128) {
        Self::insert_value(&self.message_passing_to_send_thread, time);
    }

    pub fn insert_message_sending_time(&self, time: u128) {
        Self::insert_value(&self.message_sending_time_taken, time);
    }

    pub fn insert_message_sending_time_own(&self, time: u128) {
        Self::insert_value(&self.message_sending_time_taken_own, time);
    }

    pub fn insert_message_signing_time(&self, time: u128) {
        Self::insert_value(&self.message_signing_time_taken, time);
    }

    pub fn insert_message_send_to_create(&self, time: u128) {
        Self::insert_value(&self.message_send_to_create, time);
    }

    fn gather_rqs(to_gather: &Vec<Mutex<BenchmarkHelper>>) {
        let mut first_elem = to_gather[0].lock();

        for i in 1..to_gather.len() {
            first_elem.merge(&mut *to_gather[i].lock());
        }
    }

    pub fn gather_all_rqs(&self) {
        Self::gather_rqs(&self.message_signing_time_taken);
        Self::gather_rqs(&self.message_send_to_create);
        Self::gather_rqs(&self.message_sending_time_taken);
        Self::gather_rqs(&self.message_sending_time_taken_own);
        Self::gather_rqs(&self.message_passing_time_taken);
        Self::gather_rqs(&self.message_passing_time_taken_own);
        Self::gather_rqs(&self.message_passing_to_send_thread);
    }

    fn register_rq(&self, counter: &AtomicUsize) -> usize {
        let requests = counter.fetch_add(1, Ordering::Relaxed);
        requests
    }

    pub fn register_rq_received(&self) {
        self.register_rq(&self.requests_received);
    }

    pub fn register_rq_sent(&self) {
        let requests = self.register_rq(&self.requests_sent);

        if requests % self.measurement_interval == 0 {
            let current_instant = Instant::now();

            let current_rcved_requests = self.requests_received.load(Ordering::Relaxed);

            let (previous_instant, prev_sent_rqs, prev_recvd_rqs) = {
                let mut guard = self.requests_last_mark.lock();

                let instant_replica = current_instant.clone();

                std::mem::replace(&mut *guard, (instant_replica, requests, current_rcved_requests))
            };

            let duration = current_instant.duration_since(previous_instant).as_micros();

            let sent_rqs = requests - prev_sent_rqs;
            let rcved_rqs = current_rcved_requests - prev_recvd_rqs;

            let sent_rq_per_second = (sent_rqs as f64 / duration as f64) * 1000.0 * 1000.0;
            let recv_rq_per_second = (rcved_rqs as f64 / duration as f64) * 1000.0 * 1000.0;

            self.gather_all_rqs();

            println!("{:?} // {} // --- Measurements after {}  ({} samples) ---",
                     self.node_id, self.info, requests, self.measurement_interval);


            let time = Utc::now().timestamp_millis();

            println!("{:?} // {:?} // {} requests {} per second", self.node_id, time,
                     sent_rq_per_second, "sent");

            println!("{:?} // {:?} // {} requests {} per second", self.node_id, time,
                     recv_rq_per_second, "received");

            self.message_passing_time_taken_own[0].lock().log_latency("Message passing (Own)");
            self.message_passing_time_taken[0].lock().log_latency("Message passing");
            self.message_sending_time_taken[0].lock().log_latency("Message sending");
            self.message_sending_time_taken_own[0].lock().log_latency("Message sending (Own)");
            self.message_signing_time_taken[0].lock().log_latency("Message signing");
            self.message_send_to_create[0].lock().log_latency("Create send to objects");
            self.message_passing_to_send_thread[0].lock().log_latency("Message passing send thread");
        }

    }
}

const CAP: usize = 2048;

impl Measurements {
    pub fn new(id: NodeId) -> Self {
        Measurements {
            total_latency: BenchmarkHelper::new(id, CAP),
            consensus_latency: BenchmarkHelper::new(id, CAP),
            pre_cons_latency: BenchmarkHelper::new(id, CAP),
            pos_cons_latency: BenchmarkHelper::new(id, CAP),
            pre_prepare_latency: BenchmarkHelper::new(id, CAP),
            prepare_latency: BenchmarkHelper::new(id, CAP),
            commit_latency: BenchmarkHelper::new(id, CAP),
            batch_size: BenchmarkHelper::new(id, CAP),
            prepare_msg_latency: BenchmarkHelper::new(id, CAP),
            propose_time_latency: BenchmarkHelper::new(id, CAP),
            message_recv_latency: BenchmarkHelper::new(id, CAP),
            prepare_time_taken: BenchmarkHelper::new(id, CAP),
            commit_time_taken: BenchmarkHelper::new(id, CAP),
        }
    }
}

#[derive(Clone)]
pub struct BatchMeta {
    pub batch_size: usize,
    //The time at which the consensus instance was started
    pub consensus_start_time: DateTime<Utc>,
    pub message_received_time: DateTime<Utc>,
    //
    pub started_propose: DateTime<Utc>,
    pub done_propose: DateTime<Utc>,
    //The time at which the consensus was finished
    pub consensus_decision_time: DateTime<Utc>,
    //The time at which the pre_prepare message was received
    pub pre_prepare_received_time: DateTime<Utc>,
    //The time at which we have sent the prepare message
    pub prepare_sent_time: DateTime<Utc>,
    //The time at which we have sent the commit message
    pub commit_sent_time: DateTime<Utc>,
    //The time at which a batch was received
    pub reception_time: DateTime<Utc>,
    pub execution_time: DateTime<Utc>,
    //The time at which the reply was done
    pub replied_time: DateTime<Utc>,
    //The time at which we have received the first prepare message
    pub first_prepare_received: DateTime<Utc>,
    //The time at which we have received the first commit message
    pub first_commit_received: DateTime<Utc>,
}

impl BatchMeta {
    pub fn new() -> Self {
        Self::new_with_cap(None)
    }

    pub fn new_with_cap(_cap: Option<usize>) -> Self {
        let now = Utc::now();
        Self {
            batch_size: 0,
            consensus_start_time: now,
            message_received_time: now,
            started_propose: now,
            done_propose: now,
            consensus_decision_time: now,
            pre_prepare_received_time: now,
            prepare_sent_time: now,
            commit_sent_time: now,
            reception_time: now,
            execution_time: now,
            replied_time: now,
            first_prepare_received: now,
            first_commit_received: now,
        }
    }
}

pub struct BenchmarkHelper {
    values: Vec<u128>,
    node: NodeId,
}

pub trait BenchmarkHelperStore {
    fn store(self, bench: &mut BenchmarkHelper);
}

// this code was more or less 1:1 translated from BFT-SMaRt,
// even its oddities, such as cloning the values array,
// and bugs (overflowing standard deviation)
impl BenchmarkHelper {
    pub fn new(id: NodeId, capacity: usize) -> Self {
        Self {
            node: id,
            values: Vec::with_capacity(capacity),
        }
    }

    ///Consumes the elements of the other benchmark helper into this one
    /// Maintains the other benchmark helper
    pub fn merge(&mut self, other: &mut BenchmarkHelper) {
        self.values.append(&mut other.values)
    }

    pub fn reset(&mut self) {
        self.values.clear();
    }

    pub fn max(&self, percent: bool) -> u128 {
        let mut values = self.values.clone();
        let limit = if percent { values.len() / 10 } else { 0 };

        values.sort_unstable();

        (&values[limit..(values.len() - limit)])
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn average(&mut self, percent: bool, sorted: bool) -> f64 {
        let limit = if percent { self.values.len() / 10 } else { 0 };

        if !sorted {
            self.values.sort_unstable();
        }

        let count = (&self.values[limit..(self.values.len() - limit)])
            .iter()
            .copied()
            .reduce(|x, y| x.wrapping_add(y))
            .unwrap_or(0);

        (count as f64) / ((self.values.len() - 2 * limit) as f64)
    }

    pub fn standard_deviation(&mut self, percent: bool, sorted: bool) -> f64 {
        if self.values.len() <= 1 {
            return 0.0;
        }

        if !sorted {
            self.values.sort_unstable();
        }

        let limit = if percent { self.values.len() / 10 } else { 0 };
        let num = (self.values.len() - (limit << 1)) as f64;

        let med = self.average(percent, true);

        let quad = (&self.values[limit..(self.values.len() - limit)])
            .iter()
            .copied()
            .map(|x| x.wrapping_mul(x))
            .reduce(|x, y| x.wrapping_add(y))
            .unwrap_or(0);

        let quad = quad as f64;
        let var = (quad - (num * (med * med))) / (num - 1.0);

        var.sqrt()
    }

    #[inline(always)]
    ///Returns the average and the standard deviation
    pub fn log_latency(&mut self, name: &str) -> (f64, f64) {
        let id = self.node.clone();

        let average = self.average(false, false) / 1000.0;
        let std_dev = self.standard_deviation(false, true) / 1000.0;

        println!("{:?} // {:?} // {} latency = {} (+/- {}) us",
                 id,
                 Utc::now().timestamp_millis(),
                 name,
                 average,
                 std_dev,
        );

        self.reset();

        (average, std_dev)
    }

    #[inline(always)]
    pub fn log_batch(&mut self) -> (f64, f64) {
        let id = self.node.clone();

        let avg = self.average(false, false);
        let std_dev = self.standard_deviation(false, true);

        println!("{:?} // {:?} // Batch average size = {} (+/- {}) requests",
                 id,
                 Utc::now().timestamp_millis(),
                 avg,
                 std_dev,
        );

        self.reset();

        (avg, std_dev)
    }
}

impl BenchmarkHelperStore for (DateTime<Utc>, DateTime<Utc>) {
    fn store(self, bench: &mut BenchmarkHelper) {
        let (end, start) = self;
        let duration = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(i64::MAX);

        bench.values.push(duration as u128);
    }
}

impl BenchmarkHelperStore for usize {
    fn store(self, bench: &mut BenchmarkHelper) {
        const MAX: usize = u128::MAX as usize;

        bench.values.push((self & MAX) as u128);
    }
}

impl BenchmarkHelperStore for Vec<u128> {
    fn store(mut self, bench: &mut BenchmarkHelper) {
        bench.values.append(&mut self);
    }
}