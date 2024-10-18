use crate::bft::message::ObserveEventKind;
use atlas_common::channel::mixed::ChannelMixedTx;
use atlas_common::node_id::NodeId;

pub type ObserverType = NodeId;

pub enum ConnState<T> {
    Connected(T),
    Disconnected(T),
}

pub enum MessageType<T> {
    Conn(ConnState<T>),
    Event(ObserveEventKind),
}

///This refers to the observer of the system
///
/// It receives updates from the replica it's currently on and then
#[derive(Clone)]
pub struct ObserverHandle {
    tx: ChannelMixedTx<MessageType<ObserverType>>,
}

impl ObserverHandle {
    pub fn new(tx: ChannelMixedTx<MessageType<ObserverType>>) -> Self {
        ObserverHandle { tx }
    }

    pub fn tx(&self) -> &ChannelMixedTx<MessageType<ObserverType>> {
        &self.tx
    }
}
