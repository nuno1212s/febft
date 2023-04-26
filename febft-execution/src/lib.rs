use febft_common::channel::ChannelSyncTx;
use febft_common::error::*;
use febft_common::node_id::NodeId;
use crate::app::{Reply, Request, Service, State, UnorderedBatch, UpdateBatch};
use crate::serialize::SharedData;

pub mod serialize;
pub mod app;
pub mod system_params;

pub enum ExecutionRequest<S, O> {
    // install state from state transfer protocol
    InstallState(S, Vec<O>),
    // update the state of the service
    Update(UpdateBatch<O>),
    // same as above, and include the application state
    // in the reply, used for local checkpoints
    UpdateAndGetAppstate(UpdateBatch<O>),

    //Execute an un ordered batch of requests
    ExecuteUnordered(UnorderedBatch<O>),

    // read the state of the service
    Read(NodeId),
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<D: SharedData> {
    e_tx: ChannelSyncTx<ExecutionRequest<D::State, D::Request>>,
}

impl<D: SharedData> ExecutorHandle<D>
{

    pub fn new(tx: ChannelSyncTx<ExecutionRequest<D::State, D::Request>>) -> Self {
        ExecutorHandle { e_tx: tx }
    }

    /// Sets the current state of the execution layer to the given value.
    pub fn install_state(&self, state: D::State, after: Vec<D::Request>) -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::InstallState(state, after))
            .simple(ErrorKind::Executable)
    }

    /// Queues a batch of requests `batch` for execution.
    pub fn queue_update(&self, batch: UpdateBatch<D::Request>)
                        -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::Update(batch))
            .simple(ErrorKind::Executable)
    }

    /// Queues a batch of unordered requests for execution
    pub fn queue_update_unordered(&self, requests: UnorderedBatch<D::Request>)
                                  -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::ExecuteUnordered(requests))
            .simple(ErrorKind::Executable)
    }

    /// Same as `queue_update()`, additionally reporting the serialized
    /// application state.
    ///
    /// This is useful during local checkpoints.
    pub fn queue_update_and_get_appstate(
        &self,
        batch: UpdateBatch<D::Request>,
    ) -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::UpdateAndGetAppstate(batch))
            .simple(ErrorKind::Executable)
    }
}

impl<D: SharedData> Clone for ExecutorHandle<D> {
    fn clone(&self) -> Self {
        let e_tx = self.e_tx.clone();
        Self { e_tx }
    }
}