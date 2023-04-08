use febft_communication::message::{Header, NetworkMessage, StoredMessage};
use febft_common::error::*;
use febft_execution::serialize::SharedData;
use crate::messages::{Protocol, SystemMessage};
use crate::serialize::{OrderingProtocolMessage, System};

pub trait OrderingProtocolImpl<P: OrderingProtocolMessage> {

    fn poll(&mut self) -> OrderProtocolPoll<P::ProtocolMessage>;

    fn process_message(&mut self, message: StoredMessage<Protocol<P::ProtocolMessage>>) -> Result<OrderProtocolExecResult>;

}

pub enum OrderProtocolPoll<P> {

    RunCst,
    ReceiveFromReplicas,
    Exec(StoredMessage<Protocol<P>>),
    RePoll

}

pub enum OrderProtocolExecResult {
    Success,
    RunCst
}