use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_metrics::benchmarks::BatchMeta;
use crate::serialize::SharedData;


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
    fn initial_state() -> Result<State<Self>>;

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