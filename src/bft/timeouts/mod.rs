//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

use std::marker::PhantomData;
use std::collections::BinaryHeap;
use std::cmp::{PartialOrd, Ordering};

use futures_timer::Delay;

use crate::bft::async_runtime as rt;
use crate::bft::communication::channel::{
    self,
    ChannelTx,
    ChannelRx,
    MessageChannelTx,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

type SeqNo = u64;
type AtomicSeqNo = AtomicU64;
type Timestamp = u64;

#[derive(Eq, PartialEq)]
pub struct Timeout {
    seq: SeqNo,
    when: Timestamp,
    kind: TimeoutKind,
}

impl PartialOrd for Timeout {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.when.cmp(&other.when).reverse())
    }

    fn lt(&self, other: &Self) -> bool {
        self.when >= other.when
    }

    fn le(&self, other: &Self) -> bool {
        self.when > other.when
    }

    fn gt(&self, other: &Self) -> bool {
        self.when <= other.when
    }

    fn ge(&self, other: &Self) -> bool {
        self.when < other.when
    }
}

impl Ord for Timeout {
    fn cmp(&self, other: &Self) -> Ordering {
        self.when.cmp(&other.when).reverse()
    }
}

pub enum TimeoutKind {
    // TODO: fill in some items here
}

enum TimeoutOp {
    Tick,
    Requested(Timeout),
    Canceled(SeqNo),
}

/*
struct TimeoutsHandleShared {
    current_seq_no: AtomicSeqNo,
    timestamp_generator: Instant,
}
*/

pub struct TimeoutsHandle {
    //shared: Arc<TimeoutsHandleShared>,
    tx: ChannelTx<TimeoutOp>,
}

pub struct Timeouts<S: Service> {
    _marker: PhantomData<S>,
}

impl<S: Service> Timeouts<S> {
    const CHAN_BOUND: usize = 128;

    pub fn new(
        granularity: Duration,
        system_tx: MessageChannelTx<State<S>, Request<S>, Reply<S>>,
    ) -> TimeoutsHandle {
        let (tx, rx) = channel::new_bounded(Self::CHAN_BOUND);
        let timeouts = BinaryHeap::new();

        let mut ticker = tx.clone();
        rt::spawn(async move {
            let delay = Delay::new(granularity);
            loop {
                delay.await;
                ticker.send(TimeoutOp::Tick).await.unwrap();
            }
        });

        rt::spawn(async move {
            // TODO: save an `Instant`, and use that to generate
            // timestamps for timeouts
            while let Ok(op) = rx.recv().await {
                // TODO: handle timeout ops
                match op {
                    TimeoutOp::Tick => {
                        loop {
                            match timeouts.peek() {
                                Some(timeout) if ...
                            }
                        }
                    },
                    TimeoutOp::Requested(_) => unimplemented!(),
                    TimeoutOp::Canceled(_) => unimplemented!(),
                }
            }
        });

        TimeoutsHandle { tx }
    }
}
