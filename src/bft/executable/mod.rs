//! User application execution business logic.

use crate::bft::error::*;
use crate::bft::communication::NodeId;
use crate::bft::communication::serialize::{
    ReplicaData,
    SharedData,
};
use crate::bft::communication::channel::{
    self,
    ChannelRx,
    ChannelTx,
    MessageChannelTx,
};
use crate::bft::communication::message::{
    SystemMessage,
    ReplyMessage,
    RequestMessage,
};

enum ExecutionRequest<P> {
    // process the state of the service
    ReadWrite(NodeId, RequestMessage<P>),
    // read the state of the service
    Read(NodeId),
}

/// State type of the `Service`.
pub type State<S> = <<S as Service>::Data as ReplicaData>::State;

/// Request type of the `Service`.
pub type Request<S> = <<S as Service>::Data as SharedData>::Request;

/// Reply type of the `Service`.
pub type Reply<S> = <<S as Service>::Data as SharedData>::Reply;

/// A user defined `Service`.
///
/// Application logic is implemented by this trait.
pub trait Service {
    /// The types used by the application.
    type Data: ReplicaData;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<State<Self>>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn process(
        &mut self,
        state: &mut State<Self>,
        request: Request<Self>,
    ) -> Result<Reply<Self>>;
}

/// Stateful data of the task responsible for executing
/// client requests.
pub struct Executor<S: Service> {
    my_rx: ChannelRx<ExecutionRequest<Request<S>>>,
    system_tx: MessageChannelTx<Request<S>, Reply<S>>,
    state: State<S>,
    service: S,
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<S: Service> {
    my_tx: ChannelTx<ExecutionRequest<Request<S>>>,
}

impl<S> Executor<S>
where
    S: Service + Send + 'static,
{
    /// Spawns a new service executor into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(
        system_tx: MessageChannelTx<Request<S>, Reply<S>>,
        service: S,
    ) -> ExecutorHandle<S> {
        unimplemented!()
    }
}
