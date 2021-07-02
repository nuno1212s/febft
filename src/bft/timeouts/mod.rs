//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

use std::marker::PhantomData;
use std::cmp::{PartialOrd, Ordering};

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
    current_seq_no: SeqNo,
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
        let (handler_tx, handler_rx) = channel::new_bounded(Self::CHAN_BOUND);

        rt::spawn(async move {
            // this task simply ticks every `granularity` duration,
            // generating a chan msg
        });
        rt::spawn(async move {
            // TODO: save an `Instant`, and use that to generate
            // timestamps for timeouts
            while let Ok(op) = handler_rx.recv().await {
                // TODO: handle timeout ops
                match op {
                    TimeoutOp::Tick => unimplemented!(),
                    TimeoutOp::Requested(_) => unimplemented!(),
                    TimeoutOp::Canceled(_) => unimplemented!(),
                }
            }
        });

        TimeoutsHandle { tx: handler_tx }
    }
}
