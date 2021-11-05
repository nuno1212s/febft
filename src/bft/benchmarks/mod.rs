use std::default::Default;
use std::time::{Instant, SystemTime};

#[derive(Default)]
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

#[derive(Copy, Clone)]
pub struct BatchMeta {
    pub batch_size: usize,
    pub consensus_start_time: SystemTime,
    pub consensus_decision_time: SystemTime,
    pub prepare_sent_time: SystemTime,
    pub commit_sent_time: SystemTime,
    pub reception_time: SystemTime,
    pub execution_time: SystemTime,
}

impl BatchMeta {
    pub fn new() -> Self {
        let now = SystemTime::now();
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

#[derive(Default)]
pub struct BenchmarkHelper {
    values: Vec<i64>,
}

pub trait BenchmarkHelperStore {
    fn store(self, bench: &mut BenchmarkHelper);
}

// this code was more or less 1:1 translated from BFT-SMaRt,
// even its oddities, such as cloning the values array,
// and bugs (overflowing standard deviation)
impl BenchmarkHelper {
    pub fn new(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn reset(&mut self) {
        self.values.clear();
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

        (count as f64) / ((values.len() - 2*limit) as f64)
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
        let var = (quad - (num*(med*med)))/(num-1.0);

        var.sqrt()
    }

    #[inline(always)]
    pub fn log_latency(&mut self, name: &str) {
        println!("{} latency = {} (+/- {}) us",
            name,
            self.average(false) / 1000.0,
            self.standard_deviation(false) / 1000.0,
        );
        self.reset();
    }

    #[inline(always)]
    pub fn log_batch(&mut self) {
        println!("Batch average size = {} (+/- {}) requests",
            self.average(false),
            self.standard_deviation(false),
        );
        self.reset();
    }
}

impl BenchmarkHelperStore for (SystemTime, SystemTime) {
    fn store(self, bench: &mut BenchmarkHelper) {
        const MAX: u128 = i64::MAX as u128;

        let (end, start) = self;
        let duration = end
            .duration_since(start)
            .expect("Non-monotonic time detected!")
            .as_nanos();

        bench.values.push((duration & MAX) as i64);
    }
}

impl BenchmarkHelperStore for (Instant, Instant) {
    fn store(self, bench: &mut BenchmarkHelper) {
        const MAX: u128 = i64::MAX as u128;

        let (end, start) = self;
        let duration = end
            .duration_since(start)
            .as_nanos();

        bench.values.push((duration & MAX) as i64);
    }
}

impl BenchmarkHelperStore for usize {
    fn store(self, bench: &mut BenchmarkHelper) {
        const MAX: usize = i64::MAX as usize;

        bench.values.push((self & MAX) as i64);
    }
}
