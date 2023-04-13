use std::sync::Arc;
use febft_communication::message::{Header, NetworkMessage, StoredMessage, System};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::Orderable;
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use crate::messages::{Protocol, SystemMessage};
use crate::serialize::{OrderingProtocolMessage, StateTransferMessage, ServiceMsg, NetworkView};
use crate::timeouts::Timeouts;

pub trait OrderingProtocol<D, NT>: Orderable where D: SharedData + 'static {

    type Serialization: OrderingProtocolMessage + 'static;

    type Config;

    /// Initialize this ordering protocol with the given configuration, executor, timeouts and node
    fn initialize(config: Self::Config, executor: ExecutorHandle<D>,
                  timeouts: Timeouts, node: Arc<NT>) -> Result<Self> where
        Self: Sized;

    /// Handle a protocol message that was received while we are executing the state transfer protocol
    fn handle_off_ctx_message(&mut self, message: StoredMessage<Protocol<<Self::Serialization as OrderingProtocolMessage>::ProtocolMessage>>);

    /// Poll from the ordering protocol in order to know what we should do next
    /// We do this to check if there are already
    fn poll(&mut self) -> OrderProtocolPoll<<Self::Serialization as OrderingProtocolMessage>::ProtocolMessage>;

    /// Process a protocol message that we have received
    fn process_message(&mut self, message: StoredMessage<Protocol<<Self::Serialization as OrderingProtocolMessage>::ProtocolMessage>>) -> Result<OrderProtocolExecResult>;
}

/// result from polling the ordering protocol
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