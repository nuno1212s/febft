//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

use std::marker::PhantomData;

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

pub enum Timeout {
    // TODO: fill in some items here
}

enum TimeoutKind {
    Request(Timeout),
    Reply(Timeout),
}

pub struct TimeoutsHandle {
    tx: ChannelTx<TimeoutKind>,
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
        let (resolver_tx, resolver_rx) = channel::new_bounded(Self::CHAN_BOUND);

        rt::spawn(async move {
            asd
        });
        rt::spawn(async move {
            while let Ok(request) = handler_rx.recv().await {
                // TODO: handle timeouts
                drop(request);
            }
        });

        TimeoutsHandle { tx: handler_tx }
    }
}
