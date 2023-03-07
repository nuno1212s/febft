//! User application execution business logic.

use log::error;
use std::marker::PhantomData;


use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::{Message, ObserveEventKind, ReplyMessage, SystemMessage};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::{channel, NodeId, SendNode};

use crate::bft::core::server::client_replier::ReplyHandle;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::error::*;
use crate::bft::ordering::SeqNo;


use super::ordering::Orderable;

/// Represents a single client update request, to be executed.
#[derive(Clone)]
pub struct Update<O> {
    from: NodeId,
    session_id: SeqNo,
    operation_id: SeqNo,
    operation: O,
}

/// Represents a single client update reply.
#[derive(Clone)]
pub struct UpdateReply<P> {
    to: NodeId,
    session_id: SeqNo,
    operation_id: SeqNo,
    payload: P,
}

/// Storage for a batch of client update requests to be executed.
#[derive(Clone)]
pub struct UnorderedBatch<O> {
    inner: Vec<Update<O>>,
}

/// Storage for a batch of client update requests to be executed.
#[derive(Clone)]
pub struct UpdateBatch<O> {
    seq_no: SeqNo,
    inner: Vec<Update<O>>,
    meta: Option<BatchMeta>
}

/// Storage for a batch of client update replies.
#[derive(Clone)]
pub struct BatchReplies<P> {
    inner: Vec<UpdateReply<P>>,
}

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

/* NOTE: unused

macro_rules! serialize_st {
    (Service: $S:ty, $w:expr, $s:expr) => {
        <<$S as Service>::Data as SharedData>::serialize_state($w, $s)
    }
}

macro_rules! deserialize_st {
    ($S:ty, $r:expr) => {
        <<$S as Service>::Data as SharedData>::deserialize_state($r)
    }
}

*/

/// State type of the `Service`.
pub type State<S> = <<S as Service>::Data as SharedData>::State;

/// Request type of the `Service`.
pub type Request<S> = <<S as Service>::Data as SharedData>::Request;

/// Reply type of the `Service`.
pub type Reply<S> = <<S as Service>::Data as SharedData>::Reply;

/// A user defined `Service`.
///
/// Application logic is implemented by this trait.
pub trait Service: Send {
    /// The data types used by the application and the SMR protocol.
    ///
    /// This includes their respective serialization routines.
    type Data: SharedData;

    ///// Routines used by replicas to persist data into permanent
    ///// storage.
    //type Durability: ReplicaDurability;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<State<Self>>;

    /// Process an unordered client request, and produce a matching reply
    /// Cannot alter the application state
    fn unordered_execution(&self, state: &State<Self>, request: Request<Self>) -> Reply<Self>;

    /// Much like [`unordered_execution()`], but processes a batch of requests.
    ///
    /// If [`unordered_batched_execution()`] is defined by the user, then [`unordered_execution()`] may
    /// simply be defined as such:
    ///
    /// ```rust
    /// fn unordered_execution(&self,
    /// state: State<Self>,
    /// request: Request<Self>) -> Reply<Self> {
    ///     unimplemented!()
    /// }
    /// ```
    fn unordered_batched_execution(
        &self,
        state: &State<Self>,
        requests: UnorderedBatch<Request<Self>>,
    ) -> BatchReplies<Reply<Self>> {
        let mut reply_batch = BatchReplies::with_capacity(requests.len());

        for unordered_req in requests.into_inner() {
            let (peer_id, sess, opid, req) = unordered_req.into_inner();
            let reply = self.unordered_execution(&state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn update(&mut self, state: &mut State<Self>, request: Request<Self>) -> Reply<Self>;

    /// Much like `update()`, but processes a batch of requests.
    ///
    /// If `update_batch()` is defined by the user, then `update()` may
    /// simply be defined as such:
    ///
    /// ```rust
    /// fn update(
    ///     &mut self,
    ///     state: &mut State<Self>,
    ///     request: Request<Self>,
    /// ) -> Reply<Self> {
    ///     unimplemented!()
    /// }
    /// ```
    fn update_batch(
        &mut self,
        state: &mut State<Self>,
        batch: UpdateBatch<Request<Self>>,
    ) -> BatchReplies<Reply<Self>> {
        let mut reply_batch = BatchReplies::with_capacity(batch.len());

        for update in batch.into_inner() {
            let (peer_id, sess, opid, req) = update.into_inner();
            let reply = self.update(state, req);
            reply_batch.add(peer_id, sess, opid, reply);
        }

        reply_batch
    }
}

const EXECUTING_BUFFER: usize = 8096;

pub trait ExecutorReplier: Send {
    fn execution_finished<S: Service>(
        node: SendNode<S::Data>,
        seq: Option<SeqNo>,
        batch: BatchReplies<Reply<S>>,
    );
}

pub struct FollowerReplier;

impl ExecutorReplier for FollowerReplier {
    fn execution_finished<S: Service>(
        node: SendNode<<S as Service>::Data>,
        seq: Option<SeqNo>,
        batch: BatchReplies<Reply<S>>,
    ) {
        if let None = seq {
            //Followers only deliver replies to the unordered requests, since it's not part of the quorum
            // And the requests it executes are only forwarded to it

            ReplicaReplier::execution_finished::<S>(node, seq, batch);
        }
    }
}

pub struct ReplicaReplier;

impl ExecutorReplier for ReplicaReplier {
    fn execution_finished<S: Service>(
        mut send_node: SendNode<<S as Service>::Data>,
        _seq: Option<SeqNo>,
        batch: BatchReplies<Reply<S>>,
    ) {
        if batch.len() == 0 {
            //Ignore empty batches.
            return;
        }

        crate::bft::threadpool::execute(move || {
            let mut batch = batch.into_inner();

            batch.sort_unstable_by_key(|update_reply| update_reply.to());

            // keep track of the last message and node id
            // we iterated over
            let mut curr_send = None;

            for update_reply in batch {
                let (peer_id, session_id, operation_id, payload) = update_reply.into_inner();

                // NOTE: the technique used here to peek the next reply is a
                // hack... when we port this fix over to the production
                // branch, perhaps we can come up with a better approach,
                // but for now this will do
                if let Some((message, last_peer_id)) = curr_send.take() {
                    let flush = peer_id != last_peer_id;
                    send_node.send(message, last_peer_id, flush);
                }

                // store previous reply message and peer id,
                // for the next iteration
                let message =
                    SystemMessage::Reply(ReplyMessage::new(session_id, operation_id, payload));

                curr_send = Some((message, peer_id));
            }

            // deliver last reply
            if let Some((message, last_peer_id)) = curr_send {
                send_node.send(message, last_peer_id, true);
            } else {
                // slightly optimize code path;
                // the previous if branch will always execute
                // (there is always at least one request in the batch)
                unreachable!();
            }
        });
    }
}

/// Stateful data of the task responsible for executing
/// client requests.
pub struct Executor<S: Service + 'static, T: ExecutorReplier> {
    service: S,
    state: State<S>,
    e_rx: ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>,
    reply_worker: ReplyHandle<S>,
    send_node: SendNode<S::Data>,
    observer_handle: Option<ObserverHandle>,

    p: PhantomData<T>,
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<S: Service> {
    e_tx: ChannelSyncTx<ExecutionRequest<State<S>, Request<S>>>,
}

impl<S: Service> ExecutorHandle<S>
    where
        S: Service + Send + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
{
    /// Sets the current state of the execution layer to the given value.
    pub fn install_state(&self, state: State<S>, after: Vec<Request<S>>) -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::InstallState(state, after))
            .simple(ErrorKind::Executable)
    }

    /// Queues a batch of requests `batch` for execution.
    pub fn queue_update(&self, batch: UpdateBatch<Request<S>>)
                        -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::Update(batch))
            .simple(ErrorKind::Executable)
    }

    /// Queues a batch of unordered requests for execution
    pub fn queue_update_unordered(&self, requests: UnorderedBatch<Request<S>>)
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
        batch: UpdateBatch<Request<S>>,
    ) -> Result<()> {
        self.e_tx
            .send(ExecutionRequest::UpdateAndGetAppstate(batch))
            .simple(ErrorKind::Executable)
    }
}

impl<S: Service> Clone for ExecutorHandle<S> {
    fn clone(&self) -> Self {
        let e_tx = self.e_tx.clone();
        Self { e_tx }
    }
}

impl<S, T> Executor<S, T>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + 'static,
        Reply<S>: Send + 'static,
        T: ExecutorReplier + 'static,
{
    pub fn init_handle() -> (ExecutorHandle<S>, ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>) {
        let (tx, rx) = channel::new_bounded_sync(EXECUTING_BUFFER);

        (ExecutorHandle { e_tx: tx }, rx)
    }
    /// Spawns a new service executor into the async runtime.
    pub fn new(
        reply_worker: ReplyHandle<S>,
        handle: ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>,
        mut service: S,
        current_state: Option<(State<S>, Vec<Request<S>>)>,
        send_node: SendNode<S::Data>,
        observer: Option<ObserverHandle>,
    ) -> Result<()> {
        let (state, requests) = if let Some((state, requests)) = current_state {
            (state, Some(requests))
        } else { (service.initial_state()?, None) };

        let id = send_node.id();

        let mut exec: Executor<S, T> = Executor {
            e_rx: handle,
            service,
            state,
            reply_worker,
            send_node,
            observer_handle: observer,
            p: Default::default(),
        };

        if let Some(requests) = requests {
            for request in requests {
                exec.service.update(&mut exec.state, request);
            }
        }

        // this thread is responsible for actually executing
        // requests, avoiding blocking the async runtime
        //
        // FIXME: maybe use threadpool to execute instead
        // FIXME: serialize data on exit

        std::thread::Builder::new()
            .name(format!("{:?} // Executor thread", id))
            .spawn(move || {
                while let Ok(exec_req) = exec.e_rx.recv() {
                    match exec_req {
                        ExecutionRequest::InstallState(checkpoint, after) => {
                            exec.state = checkpoint;
                            for req in after {
                                exec.service.update(&mut exec.state, req);
                            }
                        }
                        ExecutionRequest::Update(batch) => {
                            let seq_no = batch.seq_no.clone();

                            let reply_batch =
                                exec.service.update_batch(&mut exec.state, batch);

                            // deliver replies
                            exec.execution_finished(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::UpdateAndGetAppstate(batch) => {
                            let seq_no = batch.seq_no.clone();

                            let reply_batch =
                                exec.service.update_batch(&mut exec.state, batch);

                            // deliver checkpoint state to the replica
                            exec.deliver_checkpoint_state(seq_no);

                            // deliver replies
                            exec.execution_finished(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::Read(_peer_id) => {
                            todo!()
                        }
                        ExecutionRequest::ExecuteUnordered(batch) => {
                            let reply_batch =
                                exec.service.unordered_batched_execution(&exec.state, batch);

                            exec.execution_finished(None, reply_batch);
                        }
                    }
                }
            })
            .expect("Failed to start executor thread");

        Ok(())
    }

    ///Clones the current state and delivers it to the application
    /// Takes a sequence number, which corresponds to the last executed consensus instance before we performed the checkpoint
    fn deliver_checkpoint_state(&self, seq: SeqNo) {
        let cloned_state = self.state.clone();

        let system_tx = self.send_node.loopback_channel().clone();

        let m = Message::ExecutionFinishedWithAppstate((seq, cloned_state));

        if let Err(_err) = system_tx.push_request(m) {

            error!(
                "{:?} // FAILED TO DELIVER CHECKPOINT STATE",
                self.send_node.id()
            );
        };
    }

    fn execution_finished(&self, seq: Option<SeqNo>, batch: BatchReplies<Reply<S>>) {
        let send_node = self.send_node.clone();

        {
            if let Some(seq) = seq {
                if let Some(observer_handle) = &self.observer_handle {
                    //Do not notify of unordered events
                    let observe_event = MessageType::Event(ObserveEventKind::Executed(seq));

                    if let Err(err) = observer_handle.tx().send(observe_event) {
                        error!("{:?}", err);
                    }
                }
            }
        }

        T::execution_finished::<S>(send_node, seq, batch);
    }
}

impl<O> UpdateBatch<O> {
    /// Returns a new, empty batch of requests.
    pub fn new(seq_no: SeqNo) -> Self {
        Self {
            seq_no,
            inner: Vec::new(),
            meta: None
        }
    }

    pub fn new_with_cap(seq_no: SeqNo, capacity: usize) -> Self {
        Self {
            seq_no,
            inner: Vec::with_capacity(capacity),
            meta: None
        }
    }

    /// Adds a new update request to the batch.
    pub fn add(&mut self, from: NodeId, session_id: SeqNo, operation_id: SeqNo, operation: O) {
        self.inner.push(Update {
            from,
            session_id,
            operation_id,
            operation,
        });
    }

    /// Returns the inner storage.
    pub fn into_inner(self) -> Vec<Update<O>> {
        self.inner
    }

    /// Returns the length of the batch.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn append_batch_meta(&mut self, batch_meta: BatchMeta) {
        let _ = self.meta.insert(batch_meta);
    }

    pub fn take_meta(&mut self) -> Option<BatchMeta> {
        self.meta.take()
    }
}

impl<O> Orderable for UpdateBatch<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl<O> UnorderedBatch<O> {
    /// Returns a new, empty batch of requests.
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    pub fn new_with_cap(capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
        }
    }

    /// Adds a new update request to the batch.
    pub fn add(&mut self, from: NodeId, session_id: SeqNo, operation_id: SeqNo, operation: O) {
        self.inner.push(Update {
            from,
            session_id,
            operation_id,
            operation,
        });
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

impl<O> AsRef<[Update<O>]> for UpdateBatch<O> {
    fn as_ref(&self) -> &[Update<O>] {
        &self.inner[..]
    }
}

impl<O> Update<O> {
    /// Returns the inner types stored in this `Update`.
    pub fn into_inner(self) -> (NodeId, SeqNo, SeqNo, O) {
        (
            self.from,
            self.session_id,
            self.operation_id,
            self.operation,
        )
    }

    /// Returns a reference to this operation in this `Update`.
    pub fn operation(&self) -> &O {
        &self.operation
    }
}

impl<P> BatchReplies<P> {
    /*
        /// Returns a new, empty batch of replies.
        pub fn new() -> Self {
            Self { inner: Vec::new() }
        }
    */

    /// Returns a new, empty batch of replies, with the given capacity.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            inner: Vec::with_capacity(n),
        }
    }

    /// Adds a new update reply to the batch.
    pub fn add(&mut self, to: NodeId, session_id: SeqNo, operation_id: SeqNo, payload: P) {
        self.inner.push(UpdateReply {
            to,
            session_id,
            operation_id,
            payload,
        });
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
    pub fn to(&self) -> NodeId {
        self.to
    }

    /// Returns the inner types stored in this `UpdateReply`.
    pub fn into_inner(self) -> (NodeId, SeqNo, SeqNo, P) {
        (self.to, self.session_id, self.operation_id, self.payload)
    }
}
