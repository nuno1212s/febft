//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

use std::marker::PhantomData;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};
use std::sync::atomic::{self, AtomicU64};
use std::cmp::{PartialOrd, Ordering, PartialEq, Eq};
use std::sync::Arc;

use intmap::IntMap;
use futures_timer::Delay;
use either::{
    Left,
    Right,
    Either,
};

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::channel::{
    self,
    ChannelTx,
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
type Timestamp = u128;

struct Timeout {
    seq: SeqNo,
    when: Timestamp,
    kind: TimeoutKind,
}

impl PartialEq for Timeout {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq
    }

    fn ne(&self, other: &Self) -> bool {
        self.seq != other.seq
    }
}

impl Eq for Timeout { }

// NOTE: the ord impl is reversed, because `BinaryHeap` is a max heap
impl PartialOrd for Timeout {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.seq == other.seq {
            Some(Ordering::Equal)
        } else {
            Some(self.when.cmp(&other.when).reverse())
        }
    }

    fn lt(&self, other: &Self) -> bool {
        self.when >= other.when
    }

    fn le(&self, other: &Self) -> bool {
        self.seq == other.seq || self.when > other.when
    }

    fn gt(&self, other: &Self) -> bool {
        self.when <= other.when
    }

    fn ge(&self, other: &Self) -> bool {
        self.seq == other.seq || self.when < other.when
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

struct TimeoutsHandleShared {
    current_seq_no: AtomicSeqNo,
    timestamp_generator: Instant,
}

pub struct TimeoutsHandle {
    shared: Arc<TimeoutsHandleShared>,
    tx: ChannelTx<TimeoutOp>,
}

pub struct TimeoutHandle {
    seq: SeqNo,
    tx: ChannelTx<TimeoutOp>,
}

pub struct Timeouts<S: Service> {
    _marker: PhantomData<S>,
}

impl TimeoutHandle {
    /// Cancels the timeout associated with this handle.
    pub async fn cancel(mut self) -> Result<()> {
        self.tx.send(TimeoutOp::Canceled(self.seq)).await
    }
}

impl TimeoutsHandleShared {
    fn duration_since(&self) -> Duration {
        Instant::now().duration_since(self.timestamp_generator)
    }

    fn curr_timestamp(&self) -> Timestamp {
        self.duration_since().as_nanos()
    }

    fn gen_timestamp(&self, dur: Duration) -> Timestamp {
        (self.duration_since() + dur).as_nanos()
    }

    fn gen_seq_no(&self) -> SeqNo {
        self.current_seq_no.fetch_add(1, atomic::Ordering::Relaxed)
    }
}

impl TimeoutsHandle {
    /// Creates a new timeout event, that will fire after a duration of `dur`.
    pub async fn timeout(&mut self, dur: Duration, kind: TimeoutKind) -> Result<()> {
        match self.timeout_impl(false, dur, kind).await? {
            Left(_) => Ok(()),
            Right(_) => unreachable!(),
        }
    }

    /// Creates a new timeout event, that will fire after a duration of `dur`.
    ///
    /// Different from `timeout()`, this method returns a handle that allows the user
    /// to cancel the timeout before it is triggered.
    pub async fn timeout_with_cancel(&mut self, dur: Duration, kind: TimeoutKind) -> Result<TimeoutHandle> {
        match self.timeout_impl(true, dur, kind).await? {
            Left(_) => unreachable!(),
            Right(h) => Ok(h),
        }
    }

    async fn timeout_impl(
        &mut self,
        can_cancel: bool,
        dur: Duration,
        kind: TimeoutKind,
    ) -> Result<Either<(), TimeoutHandle>> {
        let seq = self.shared.gen_seq_no();
        let when = self.shared.gen_timestamp(dur);
        let timeout = Timeout { seq, when, kind };

        self.tx.send(TimeoutOp::Requested(timeout)).await?;

        Ok(if can_cancel {
            let tx = self.tx.clone();
            Right(TimeoutHandle { seq, tx })
        } else {
            Left(())
        })
    }
}

impl<S: Service> Timeouts<S> {
    const CHAN_BOUND: usize = 128;

    pub fn new(
        granularity: Duration,
        mut system_tx: MessageChannelTx<State<S>, Request<S>, Reply<S>>,
    ) -> TimeoutsHandle {
        let (tx, mut rx) = channel::new_bounded(Self::CHAN_BOUND);

        let mut to_trigger = BinaryHeap::<Timeout>::new();
        let mut evaluating = IntMap::<()>::new();

        let mut ticker = tx.clone();
        rt::spawn(async move {
            // TODO: exit condition
            loop {
                Delay::new(granularity).await;
                ticker.send(TimeoutOp::Tick).await.unwrap();
            }
        });

        let shared = Arc::new(TimeoutsHandleShared {
            current_seq_no: AtomicSeqNo::new(0),
            timestamp_generator: Instant::now(),
        });
        let shared_clone = Arc::clone(&shared);

        rt::spawn(async move {
            let shared = shared_clone;
            while let Ok(op) = rx.recv().await {
                match op {
                    TimeoutOp::Tick => {
                        let mut triggered = Vec::new();
                        loop {
                            let timestamp = shared.curr_timestamp();
                            match to_trigger.peek() {
                                // timeout should be fired, if it hasn't been canceled yet
                                Some(t) if timestamp >= t.when => {
                                    let t = to_trigger.pop().unwrap();
                                    if evaluating.remove(t.seq).is_some() {
                                        triggered.push(t.kind);
                                    }
                                },
                                // this is a min priority queue, so no more timeouts should be
                                // triggered if the first timeout is after the current time
                                _ => break,
                            }
                        }
                        // TODO: system_tx.send() ...
                        drop(triggered);
                    },
                    TimeoutOp::Requested(timeout) => {
                        to_trigger.push(timeout);
                    },
                    TimeoutOp::Canceled(seq_no) => {
                        evaluating.remove(seq_no);
                    },
                }
            }
        });

        TimeoutsHandle { tx, shared }
    }
}
