//! User application execution business logic.

use log::error;
use std::marker::PhantomData;
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};

use febft_common::error::*;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::{NodeId, SendNode};
use febft_communication::benchmarks::BatchMeta;
use febft_communication::message::{NetworkMessageKind, System};
use febft_execution::app::{BatchReplies, Reply, Request, Service, State, UnorderedBatch, UpdateBatch};
use febft_messages::messages::{ReplyMessage, SystemMessage};

use crate::bft::core::server::client_replier::ReplyHandle;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::message::serialize::{PBFTConsensus};
use crate::bft::message::{Message, ObserveEventKind};
use crate::bft::PBFT;

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

const EXECUTING_BUFFER: usize = 8096;

pub trait ExecutorReplier: Send {
    fn execution_finished<S: Service>(
        node: SendNode<PBFT<S::Data>>,
        seq: Option<SeqNo>,
        batch: BatchReplies<Reply<S>>,
    );
}

pub struct FollowerReplier;

impl ExecutorReplier for FollowerReplier {
    fn execution_finished<S: Service>(
        node: SendNode<PBFT<S::Data>>,
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
        mut send_node: SendNode<PBFT<S::Data>>,
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
                    send_node.send(NetworkMessageKind::from(message), last_peer_id, flush);
                }

                // store previous reply message and peer id,
                // for the next iteration
                //TODO: Choose ordered or unordered reply
                let message =
                    SystemMessage::OrderedReply(ReplyMessage::new(session_id, operation_id, payload));

                curr_send = Some((message, peer_id));
            }

            // deliver last reply
            if let Some((message, last_peer_id)) = curr_send {
                send_node.send(NetworkMessageKind::from(message), last_peer_id, true);
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
    send_node: SendNode<PBFT<S::Data>>,
    loopback_channel: ChannelSyncTx<Message<S::Data>>,
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
        send_node: SendNode<PBFT<S::Data>>,
        loopback_channel: ChannelSyncTx<Message<S::Data>>,
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
            loopback_channel,
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
                            let seq_no = batch.sequence_number();

                            let reply_batch =
                                exec.service.update_batch(&mut exec.state, batch);

                            // deliver replies
                            exec.execution_finished(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::UpdateAndGetAppstate(batch) => {
                            let seq_no = batch.sequence_number();

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

        let m = Message::ExecutionFinishedWithAppstate((seq, cloned_state));

        if let Err(_err) = self.loopback_channel.send(m) {

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