use std::sync::Arc;
use febft_communication::message::{Header, NetworkMessage, StoredMessage};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use crate::messages::{Protocol, SystemMessage};
use crate::serialize::{OrderingProtocolMessage, StateTransferMessage, System};

pub trait OrderingProtocolImpl<P: OrderingProtocolMessage> {
    type Serialization: OrderingProtocolMessage;

    type Config;

    fn initialize(config: Self::Config) -> Result<Self> where Self: Sized;

    fn poll(&mut self) -> OrderProtocolPoll<P::ProtocolMessage>;

    fn process_message(&mut self, message: StoredMessage<Protocol<P::ProtocolMessage>>) -> Result<OrderProtocolExecResult>;
}

pub enum OrderProtocolPoll<P> {
    RunCst,
    ReceiveFromReplicas,
    Exec(StoredMessage<Protocol<P>>),
    RePoll,
}

pub enum OrderProtocolExecResult {
    Success,
    RunCst,
}