//! User application execution business logic.

use log::error;
use std::marker::PhantomData;
use std::sync::Arc;
use febft_common::{channel, threadpool};
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};

use febft_common::error::*;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::{Node};
use febft_communication::message::{NetworkMessageKind};
use febft_execution::app::{BatchReplies, Reply, Request, Service, State, UnorderedBatch, UpdateBatch};
use febft_execution::{ExecutionRequest, ExecutorHandle};
use febft_execution::serialize::SharedData;
use febft_messages::messages::{Message, ReplyMessage, SystemMessage};
use febft_messages::ordering_protocol::OrderingProtocol;
use febft_messages::serialize::{OrderingProtocolMessage, ServiceMsg, StateTransferMessage};
use crate::server::client_replier::ReplyHandle;

const EXECUTING_BUFFER: usize = 8096;

pub trait ExecutorReplier: Send {
    fn execution_finished<D, OP, ST, NT>(
        node: Arc<NT>,
        seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: SharedData + 'static,
            OP: OrderingProtocolMessage + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> + 'static;
}

pub struct FollowerReplier;

impl ExecutorReplier for FollowerReplier {
    fn execution_finished<D, OP, ST, NT>(
        node: Arc<NT>,
        seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: SharedData + 'static,
            OP: OrderingProtocolMessage + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> + 'static {
        if let None = seq {
            //Followers only deliver replies to the unordered requests, since it's not part of the quorum
            // And the requests it executes are only forwarded to it

            ReplicaReplier::execution_finished::<D, OP, ST, NT>(node, seq, batch);
        }
    }
}

pub struct ReplicaReplier;

impl ExecutorReplier for ReplicaReplier {
    fn execution_finished<D, OP, ST, NT>(
        mut send_node: Arc<NT>,
        _seq: Option<SeqNo>,
        batch: BatchReplies<D::Reply>,
    ) where D: SharedData + 'static,
            OP: OrderingProtocolMessage + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> + 'static {
        if batch.len() == 0 {
            //Ignore empty batches.
            return;
        }

        threadpool::execute(move || {
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
pub struct Executor<S, NT> where
    S: Service + 'static, {
    service: S,
    state: State<S>,
    e_rx: ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>,
    reply_worker: ReplyHandle<S>,
    send_node: Arc<NT>,
    loopback_channel: ChannelSyncTx<Message<S::Data>>,

}


impl<S, NT> Executor<S, NT>
    where
        S: Service + 'static,
        NT: 'static
{
    pub fn init_handle() -> (ExecutorHandle<S::Data>, ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>) {
        let (tx, rx) = channel::new_bounded_sync(EXECUTING_BUFFER);

        (ExecutorHandle::new(tx), rx)
    }
    /// Spawns a new service executor into the async runtime.
    pub fn new<OP, ST, T>(
        reply_worker: ReplyHandle<S>,
        handle: ChannelSyncRx<ExecutionRequest<State<S>, Request<S>>>,
        mut service: S,
        current_state: Option<(State<S>, Vec<Request<S>>)>,
        send_node: Arc<NT>,
        loopback_channel: ChannelSyncTx<Message<S::Data>>,
    ) -> Result<()>
        where OP: OrderingProtocolMessage + 'static,
              ST: StateTransferMessage + 'static,
              T: ExecutorReplier + 'static,
              NT: Node<ServiceMsg<S::Data, OP, ST>> {
        let (state, requests) = if let Some((state, requests)) = current_state {
            (state, Some(requests))
        } else { (service.initial_state()?, None) };

        let mut exec: Executor<S, NT> = Executor {
            e_rx: handle,
            service,
            state,
            reply_worker,
            send_node,
            loopback_channel
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
            .name(format!("Executor thread"))
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
                            exec.execution_finished::<OP, ST, T>(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::UpdateAndGetAppstate(batch) => {
                            let seq_no = batch.sequence_number();

                            let reply_batch =
                                exec.service.update_batch(&mut exec.state, batch);

                            // deliver checkpoint state to the replica
                            exec.deliver_checkpoint_state(seq_no);

                            // deliver replies
                            exec.execution_finished::<OP, ST, T>(Some(seq_no), reply_batch);
                        }
                        ExecutionRequest::Read(_peer_id) => {
                            todo!()
                        }
                        ExecutionRequest::ExecuteUnordered(batch) => {
                            let reply_batch =
                                exec.service.unordered_batched_execution(&exec.state, batch);

                            exec.execution_finished::<OP, ST, T>(None, reply_batch);
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
                "FAILED TO DELIVER CHECKPOINT STATE",
            );
        };
    }

    fn execution_finished<OP, ST, T>(&self, seq: Option<SeqNo>, batch: BatchReplies<Reply<S>>)
        where OP: OrderingProtocolMessage + 'static,
              ST: StateTransferMessage + 'static,
              NT: Node<ServiceMsg<S::Data, OP, ST>>,
              T: ExecutorReplier + 'static {
        let send_node = self.send_node.clone();

        /*{
            if let Some(seq) = seq {
                if let Some(observer_handle) = &self.observer_handle {
                    //Do not notify of unordered events
                    let observe_event = MessageType::Event(ObserveEventKind::Executed(seq));

                    if let Err(err) = observer_handle.tx().send(observe_event) {
                        error!("{:?}", err);
                    }
                }
            }
        }*/

        T::execution_finished::<S::Data, OP, ST, NT>(send_node, seq, batch);
    }
}