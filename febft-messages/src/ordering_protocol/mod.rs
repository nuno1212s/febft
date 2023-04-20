use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use febft_communication::message::{Header, NetworkMessage, StoredMessage, System};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::Orderable;
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use crate::messages::{ForwardedRequestsMessage, Protocol, SystemMessage};
use crate::serialize::{OrderingProtocolMessage, StateTransferMessage, ServiceMsg, NetworkView};
use crate::timeouts::{ClientRqInfo, Timeout, Timeouts};

pub type View<OP> = <OP as OrderingProtocolMessage>::ViewInfo;

pub type ProtocolMessage<OP> = <OP as OrderingProtocolMessage>::ProtocolMessage;

pub trait OrderingProtocol<D, NT>: Orderable where D: SharedData + 'static {

    type Serialization: OrderingProtocolMessage + 'static;

    type Config;

    /// Initialize this ordering protocol with the given configuration, executor, timeouts and node
    fn initialize(config: Self::Config, executor: ExecutorHandle<D>,
                  timeouts: Timeouts, node: Arc<NT>) -> Result<Self> where
        Self: Sized;

    /// Handle a protocol message that was received while we are executing another protocol
    fn handle_off_ctx_message(&mut self, message: StoredMessage<Protocol<ProtocolMessage<Self::Serialization>>>);

    /// Handle the protocol being executed having changed (for example to the state transfer protocol)
    /// This is important for some of the protocols, which need to know when they are being executed or not
    fn handle_execution_changed(&mut self, is_executing: bool) -> Result<()>;

    /// Poll from the ordering protocol in order to know what we should do next
    /// We do this to check if there are already
    fn poll(&mut self) -> OrderProtocolPoll<ProtocolMessage<Self::Serialization>>;

    /// Process a protocol message that we have received
    fn process_message(&mut self, message: StoredMessage<Protocol<ProtocolMessage<Self::Serialization>>>) -> Result<OrderProtocolExecResult>;

    /// Handle a timeout received from the timeouts layer
    fn handle_timeout(&mut self, timeout: Vec<ClientRqInfo>) -> Result<OrderProtocolExecResult>;

    /// Handle having received a forwarded requests message from another node
    fn handle_forwarded_requests(&mut self, requests: StoredMessage<ForwardedRequestsMessage<D::Request>>) -> Result<()>;

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

impl<P> Debug for OrderProtocolPoll<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderProtocolPoll::RunCst => {
                write!(f, "RunCst")
            }
            OrderProtocolPoll::ReceiveFromReplicas => {
                write!(f, "Receive From Replicas")
            }
            OrderProtocolPoll::Exec(_) => {
                write!(f, "Exec")
            }
            OrderProtocolPoll::RePoll => {
                write!(f, "RePoll")
            }
        }
    }
}