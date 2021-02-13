#[cfg(not(feature = "expose_impl"))]
mod socket;

#[cfg(feature = "expose_impl")]
pub mod socket;

pub mod serialize;
pub mod message;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::bft::communication::Socket;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct NodeId(u32);

pub struct MessageBus {
    message_bus: Receiver<SystemMessage>,
    notifier_consensus: Sender<ConsensusMessage>,
}

impl Node {
    fn subscribe(&self, notifier: Notifier);
    fn queue_send<M: Serialize>(&self, message: M);
}
