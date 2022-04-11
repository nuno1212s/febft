use std::default::Default;

use chrono::DateTime;
use chrono::offset::Utc;

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
    //Time since instructed to send the request and the actual sending of the request
    pub message_sending_time_taken: BenchmarkHelper,
    //Time since instructed to send the request and the actual sending of the request
    pub message_sending_time_taken_own: BenchmarkHelper,
    //Time taken to sign the requests
    pub message_signing_time_taken: BenchmarkHelper,
    //Time taken to create send to objects
    pub message_send_to_create: BenchmarkHelper,
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
            message_sending_time_taken: BenchmarkHelper::new(id, CAP),
            message_sending_time_taken_own: BenchmarkHelper::new(id, CAP),
            message_signing_time_taken: BenchmarkHelper::new(id, CAP),
            message_send_to_create: BenchmarkHelper::new(id, CAP)
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
    pub message_passing_latencies_own: Vec<u128>,
    //Stores times taken since instructing the message to be sent and the message
    //Actually being sent
    pub message_passing_latencies: Vec<u128>,
    //Stores times taken signing the request
    pub message_signing_latencies: Vec<u128>,
    //Stores the times taken to create the send tos objects
    pub message_send_to_create: Vec<u128>,
}

impl BatchMeta {
    pub fn new() -> Self {
        Self::new_with_cap(None)
    }

    pub fn new_with_cap(cap: Option<usize>) -> Self {
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
            message_passing_latencies_own: match cap {
                None => { Vec::new() }
                Some(cap) => { Vec::with_capacity(cap) }
            },
            message_passing_latencies: match cap {
                None => { Vec::new() }
                Some(cap) => { Vec::with_capacity(cap) }
            },
            message_signing_latencies: match cap {
                None => { Vec::new() }
                Some(cap) => { Vec::with_capacity(cap) }
            },
            message_send_to_create: match cap {
                None => {Vec::new()}
                Some(cap) => { Vec::with_capacity(cap) }
            },
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

        println!("{:?} // {} latency = {} (+/- {}) us",
                 id,
                 name,
                 average,
                 std_dev,
        );

        (average, std_dev)
    }

    #[inline(always)]
    pub fn log_batch(&mut self) -> (f64, f64){
        let id = self.node.clone();

        let avg = self.average(false, false);
        let std_dev = self.standard_deviation(false, true);

        println!("{:?} // Batch average size = {} (+/- {}) requests",
                 id,
                 avg,
                 std_dev,
        );

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