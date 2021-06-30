//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

//use crate::bft::communication::message::Message;
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

pub enum TimeoutRequest {
    // TODO: fill in some items here
}

pub struct TimeoutsHandle {
    tx: (),
}

pub struct Timeouts<S: Service> {
    rx: ChannelRx<TimeoutRequest>,
    system_tx: MessageChannelTx<State<S>, Request<S>, Reply<S>>,
}
