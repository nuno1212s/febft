//! User application execution business logic.

use std::thread;
use std::sync::mpsc;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::crypto::hash::Digest;
use crate::bft::communication::NodeId;
use crate::bft::communication::message::Message;
use crate::bft::communication::channel::MessageChannelTx;
use crate::bft::communication::serialize::{
    //ReplicaDurability,
    ReplicaData,
    SharedData,
};

/// Represents a single client update request, to be executed.
pub struct Update<O> {
    from: NodeId,
    digest: Digest,
    operation: O,
}

/// Represents a single client update reply.
pub struct UpdateReply<P> {
    to: NodeId,
    digest: Digest,
    payload: P,
}

/// Storage for a batch of client update requests to be executed.
pub struct UpdateBatch<O> {
    inner: Vec<Update<O>>,
}

/// Storage for a batch of client update replies.
pub struct UpdateBatchReplies<P> {
    inner: Vec<UpdateReply<P>>,
}

enum ExecutionRequest<O> {
    // update the state of the service
    Update(UpdateBatch<O>),
    // same as above, and include the application state
    // in the reply, used for local checkpoints
    UpdateAndGetAppstate(UpdateBatch<O>),
    // read the state of the service
    Read(NodeId),
}

macro_rules! serialize_st {
    (Service: $S:ty, $w:expr, $s:expr) => {
        <<$S as Service>::Data as ReplicaData>::serialize_state($w, $s)
    }
}

/* NOTE: unused for now

macro_rules! deserialize_st {
    ($S:ty, $r:expr) => {
        <<$S as Service>::Data as ReplicaData>::deserialize_state($r)
    }
}

*/

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
    /// The data types used by the application and the SMR protocol.
    ///
    /// This includes their respective serialization routines.
    type Data: ReplicaData;

    ///// Routines used by replicas to persist data into permanent
    ///// storage.
    //type Durability: ReplicaDurability;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<State<Self>>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn update(
        &mut self,
        state: &mut State<Self>,
        request: Request<Self>,
    ) -> Reply<Self>;
}

/// Stateful data of the task responsible for executing
/// client requests.
pub struct Executor<S: Service> {
    service: S,
    state: State<S>,
    e_rx: mpsc::Receiver<ExecutionRequest<Request<S>>>,
    system_tx: MessageChannelTx<Request<S>, Reply<S>>,
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<S: Service> {
    e_tx: mpsc::Sender<ExecutionRequest<Request<S>>>,
}

impl<S: Service> ExecutorHandle<S>
where
    S: Service + Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Queues a batch of requests `batch` for execution.
    pub fn queue_update(&mut self, batch: UpdateBatch<Request<S>>) -> Result<()> {
        self.e_tx.send(ExecutionRequest::Update(batch))
            .simple(ErrorKind::Executable)
    }

    /// Same as `queue_update()`, additionally reporting the serialized
    /// application state.
    ///
    /// This is useful during local checkpoints.
    pub fn queue_update_and_get_appstate(
        &mut self,
        batch: UpdateBatch<Request<S>>,
    ) -> Result<()> {
        self.e_tx.send(ExecutionRequest::UpdateAndGetAppstate(batch))
            .simple(ErrorKind::Executable)
    }
}

impl<S: Service> Clone for ExecutorHandle<S> {
    fn clone(&self) -> Self {
        let e_tx = self.e_tx.clone();
        Self { e_tx }
    }
}

impl<S> Executor<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Spawns a new service executor into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(
        system_tx: MessageChannelTx<Request<S>, Reply<S>>,
        mut service: S,
    ) -> Result<ExecutorHandle<S>> {
        let (e_tx, e_rx) = mpsc::channel();

        let state = service.initial_state()?;
        let mut exec = Executor {
            e_rx,
            system_tx,
            service,
            state,
        };

        // this thread is responsible for actually executing
        // requests, avoiding blocking the async runtime
        //
        // FIXME: maybe use threadpool to execute instead
        // FIXME: serialize data on exit
        thread::spawn(move || {
            while let Ok(exec_req) = exec.e_rx.recv() {
                match exec_req {
                    ExecutionRequest::Update(batch) => {
                        let reply_batch = UpdateBatchReplies::with_capacity(batch.len());

                        for update in batch.into_inner() {
                            let (peer_id, dig, req) = update.into_inner();
                            let reply = exec.service.update(&mut exec.state, req);
                            reply_batch.add(peer_id, dig, reply);
                        }

                        // deliver replies
                        let mut system_tx = exec.system_tx.clone();
                        rt::spawn(async move {
                            let m = Message::ExecutionFinished(reply_batch);
                            system_tx.send(m).await.unwrap();
                        });
                    },
                    ExecutionRequest::UpdateAndGetAppstate(peer_id, dig, req) => {
                        let reply_batch = UpdateBatchReplies::with_capacity(batch.len());

                        for update in batch.into_inner() {
                            let (peer_id, dig, req) = update.into_inner();
                            let reply = exec.service.update(&mut exec.state, req);
                            reply_batch.add(peer_id, dig, reply);
                        }
                        let serialized_appstate = {
                            // TODO: make this a config param?
                            const SERIALIZED_APPSTATE_BUFSIZ: usize = 8192;
                            let mut b = Vec::with_capacity(SERIALIZED_APPSTATE_BUFSIZ);
                            serialize_st!(Service: S, &mut b, &exec.state).unwrap();
                            b
                        };

                        // deliver replies
                        let mut system_tx = exec.system_tx.clone();
                        rt::spawn(async move {
                            let m = Message::ExecutionFinishedWithAppstate(
                                reply_batch,
                                serialized_appstate,
                            );
                            system_tx.send(m).await.unwrap();
                        });
                    },
                    ExecutionRequest::Read(_peer_id) => {
                        unimplemented!()
                    },
                }
            }
        });

        Ok(ExecutorHandle { e_tx })
    }
}

impl<O> UpdateBatch<O> {
    /// Returns a new, empty batch of requests.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    /// Adds a new update request to the batch.
    pub fn add(&mut self, from: NodeId, digest: Digest, operation: O) {
        self.inner.push(Update { from, digest, operation });
    }

    /// Returns the inner storage.
    pub fn into_inner(self) -> Vec<Update<O>> {
        self.inner
    }

    /// Returns the length of the batch.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<O> Update<O> {
    /// Returns the inner types stored in this `Update`.
    pub fn into_inner(self) -> (NodeId, Digest, O) {
        (self.from, self.digest, self.operation)
    }
}

impl<P> UpdateBatchReplies<P> {
/*
    /// Returns a new, empty batch of replies.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }
*/

    /// Returns a new, empty batch of replies, with the given capacity.
    pub fn with_capacity(n: usize) -> Self {
        Self { inner: Vec::with_capacity(n) }
    }

    /// Adds a new update reply to the batch.
    pub fn add(&mut self, to: NodeId, digest: Digest, payload: P) {
        self.inner.push(UpdateReply { to, digest, payload });
    }

    /// Returns the inner storage.
    pub fn into_inner(self) -> Vec<UpdateReply<P>> {
        self.inner
    }

    /// Returns the length of the batch.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<P> UpdateReply<P> {
    /// Returns the inner types stored in this `UpdateReply`.
    pub fn into_inner(self) -> (NodeId, Digest, P) {
        (self.to, self.digest, self.payload)
    }
}
