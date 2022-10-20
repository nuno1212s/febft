use chrono::offset::Utc;
use chrono::DateTime;

use super::communication::NodeId;

pub struct Measurements {
    pub total_latency: BenchmarkHelper,
    pub consensus_latency: BenchmarkHelper,
    pub pre_cons_latency: BenchmarkHelper,
    pub pos_cons_latency: BenchmarkHelper,
    pub pre_prepare_latency: BenchmarkHelper,
    pub prepare_latency: BenchmarkHelper,
    pub commit_latency: BenchmarkHelper,
    pub batch_size: BenchmarkHelper,
}

impl Measurements {

    pub fn new(owner_id: NodeId) -> Self {
        Self {
            total_latency: BenchmarkHelper::new(owner_id, 2000),
            consensus_latency: BenchmarkHelper::new(owner_id, 2000),
            pre_cons_latency: BenchmarkHelper::new(owner_id, 2000),
            pos_cons_latency: BenchmarkHelper::new(owner_id, 2000),
            pre_prepare_latency: BenchmarkHelper::new(owner_id, 2000),
            prepare_latency: BenchmarkHelper::new(owner_id, 2000),
            commit_latency: BenchmarkHelper::new(owner_id, 2000),
            batch_size: BenchmarkHelper::new(owner_id, 2000),
        }
    }

}

#[derive(Copy, Clone)]
pub struct BatchMeta {
    pub batch_size: usize,
    pub consensus_start_time: DateTime<Utc>,
    pub consensus_decision_time: DateTime<Utc>,
    pub prepare_sent_time: DateTime<Utc>,
    pub commit_sent_time: DateTime<Utc>,
    pub reception_time: DateTime<Utc>,
    pub execution_time: DateTime<Utc>,
}

impl BatchMeta {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            batch_size: 0,
            consensus_start_time: now,
            consensus_decision_time: now,
            prepare_sent_time: now,
            commit_sent_time: now,
            reception_time: now,
            execution_time: now,
        }
    }
}

//#[derive(Default)]
pub struct BenchmarkHelper {
    values: Vec<i64>,
    node: NodeId,
}

pub trait BenchmarkHelperStore {
    fn store(self, bench: &mut BenchmarkHelper);
}

// this code was more or less 1:1 translated from BFT-SMaRt,
// even its oddities, such as cloning the values array,
// and bugs (overflowing standard deviation)
impl BenchmarkHelper {
    pub fn new(owner_id: NodeId, capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            node: owner_id,
        }
    }

    pub fn reset(&mut self) {
        self.values.clear();
    }

    pub fn max(&self, percent: bool) -> i64 {
        let mut values = self.values.clone();
        let limit = if percent { values.len() / 10 } else { 0 };

        values.sort_unstable();

        (&values[limit..(values.len() - limit)])
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
    }

    pub fn average(&self, percent: bool) -> f64 {
        let mut values = self.values.clone();
        let limit = if percent { values.len() / 10 } else { 0 };

        values.sort_unstable();

        let count: i64 = (&values[limit..(values.len() - limit)])
            .iter()
            .copied()
            .reduce(|x, y| x.wrapping_add(y))
            .unwrap_or(0);

        (count as f64) / ((values.len() - 2 * limit) as f64)
    }

    pub fn standard_deviation(&mut self, percent: bool) -> f64 {
        if self.values.len() <= 1 {
            return 0.0;
        }

        self.values.sort_unstable();

        let limit = if percent { self.values.len() / 10 } else { 0 };
        let num = (self.values.len() - (limit << 1)) as f64;
        let med = self.average(percent);
        let quad: i64 = (&self.values[limit..(self.values.len() - limit)])
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
    pub fn log_latency(&mut self, name: &str) {

        let id = self.node.clone();

        println!(
            "{:?} // {:?} // {} latency = {} (+/- {}) us",
            id,
            Utc::now().timestamp_millis(),
            name,
            self.average(false) / 1000.0,
            self.standard_deviation(false) / 1000.0,
        );
        
        self.reset();
    }

    #[inline(always)]
    pub fn log_batch(&mut self) {
        let id = self.node.clone();
        println!(
            "{:?} // {:?} // Batch average size = {} (+/- {}) requests",
            id,
            Utc::now().timestamp_millis(),
            self.average(false),
            self.standard_deviation(false),
        );
        self.reset();
    }
}

impl BenchmarkHelperStore for (DateTime<Utc>, DateTime<Utc>) {
    fn store(self, bench: &mut BenchmarkHelper) {
        let (end, start) = self;
        let duration = end
            .signed_duration_since(start)
            .num_nanoseconds()
            .unwrap_or(i64::MAX);

        bench.values.push(duration);
    }
}

impl BenchmarkHelperStore for usize {
    fn store(self, bench: &mut BenchmarkHelper) {
        const MAX: usize = i64::MAX as usize;

        bench.values.push((self & MAX) as i64);
    }
}
