use std::collections::BTreeMap;

use log::{error, warn};
use log4rs::filter::Response;

use crate::bft::{
    benchmarks::BatchMeta,
    communication::{
        channel::{self, ChannelSyncRx, ChannelSyncTx},
    },
    crypto::hash::Digest,
    executable::{ExecutorHandle, Request, Service, UpdateBatch},
    ordering::{Orderable, SeqNo},
};

use crate::bft::error::*;
use crate::bft::msg_log::decided_log::BatchExecutionInfo;
use crate::bft::msg_log::Info;

use super::{ResponseMessage, ResponseMsg};

///This is made to handle the backlog when the consensus is working faster than the persistent storage layer.
/// It holds update batches that are yet to be executed since they are still waiting for the confirmation of the persistent log
/// This is only needed (and only instantiated) when the persistency mode is strict
pub struct ConsensusBacklog<S: Service> {
    rx: ChannelSyncRx<BacklogMessage<S>>,

    //Receives messages from the persistent log
    logger_rx: ChannelSyncRx<ResponseMsg>,

    //The handle to the executor
    executor_handle: ExecutorHandle<S>,

    //This is the batch that is currently waiting for it's messages to be persisted
    //Even if we already persisted the consensus instance that came after it (for some reason)
    // We can only deliver it when all the previous ones have been delivered,
    // As it must be ordered
    currently_waiting_for: Option<AwaitingPersistence<S>>,

    //Message confirmations that we have already received but pertain to a further ahead consensus instance
    messages_received_ahead: BTreeMap<SeqNo, Vec<ResponseMessage>>,
}

type BacklogMessage<S> = BacklogMsg<S>;

enum BacklogMsg<S: Service> {
    Batch(BatchExecutionInfo<S>),
    Proof(BatchExecutionInfo<S>),
}

struct AwaitingPersistence<S: Service> {
    info: BatchExecutionInfo<S>,
    pending_rq: PendingRq,
}

/// Information about the current pending request
enum PendingRq {
    Proof(Option<SeqNo>),
    /// What is still left to persist in order to execute this batch
    /// The vector of messages still left to persist and the record of
    /// whether the metadata has been persisted or not
    Batch(Vec<Digest>, Option<SeqNo>),
}

///A detachable handle so we deliver work to the
/// consensus back log thread
pub struct ConsensusBackLogHandle<S: Service> {
    rq_tx: ChannelSyncTx<BacklogMessage<S>>,
    logger_tx: ChannelSyncTx<ResponseMsg>,
}

impl<S: Service> ConsensusBackLogHandle<S> {
    pub fn logger_tx(&self) -> ChannelSyncTx<ResponseMsg> {
        self.logger_tx.clone()
    }

    /// Queue a normal processed batch, where we received all messages individually and persisted
    /// them individually
    pub fn queue_batch(&self, batch: BatchExecutionInfo<S>) -> Result<()> {
        if let Err(err) = self.rq_tx.send(BacklogMsg::Batch(batch)) {
            Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent, format!("{:?}", err).as_str()))
        } else {
            Ok(())
        }
    }

    /// Queue a batch that we received via a proof and therefore only need to wait for the persistence
    /// of the entire proof, instead of the individual messages
    pub fn queue_batch_proof(&self, batch: BatchExecutionInfo<S>) -> Result<()> {
        if let Err(err) = self.rq_tx.send(BacklogMsg::Proof(batch)) {
            Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent, format!("{:?}", err).as_str()))
        } else {
            Ok(())
        }
    }
}

impl<S: Service> Clone for ConsensusBackLogHandle<S> {
    fn clone(&self) -> Self {
        Self {
            rq_tx: self.rq_tx.clone(),
            logger_tx: self.logger_tx.clone(),
        }
    }
}

///This channel size serves as the "buffer" for the amount of consensus instances
///That can be waiting for messages
const CHANNEL_SIZE: usize = 1024;

impl<S: Service + 'static> ConsensusBacklog<S> {
    ///Initialize the consensus backlog
    pub fn init_backlog(executor: ExecutorHandle<S>) -> ConsensusBackLogHandle<S> {
        let (logger_tx, logger_rx) = channel::new_bounded_sync(CHANNEL_SIZE);

        let (batch_tx, batch_rx) = channel::new_bounded_sync(CHANNEL_SIZE);

        let backlog_thread = ConsensusBacklog {
            rx: batch_rx,
            logger_rx,
            executor_handle: executor,
            currently_waiting_for: None,
            messages_received_ahead: BTreeMap::new(),
        };

        backlog_thread.start_thread();

        let handle = ConsensusBackLogHandle {
            rq_tx: batch_tx,
            logger_tx,
        };

        handle
    }

    fn start_thread(self) {
        std::thread::Builder::new()
            .name(format!("Consensus Backlog thread"))
            .spawn(move || {
                self.run();
            })
            .expect("Failed to start consensus backlog thread.");
    }

    fn run(mut self) {
        loop {
            if self.currently_waiting_for.is_some() {
                let notification = match self.logger_rx.recv() {
                    Ok(notification) => notification,
                    Err(_) => break,
                };

                self.handle_received_message(notification);

                if self.currently_waiting_for.as_ref().unwrap().is_ready_for_execution() {
                    let finished_batch = self.currently_waiting_for.take().unwrap();

                    self.dispatch_batch(finished_batch.into());
                }
            } else {
                let batch_info = match self.rx.recv() {
                    Ok(rcved) => rcved,
                    Err(err) => {
                        error!("{:?}", err);

                        break;
                    }
                };

                let mut awaiting = AwaitingPersistence::from(batch_info);

                self.process_pending_messages_for_current(&mut awaiting);

                if awaiting.is_ready_for_execution() {

                    //If we have already received everything, dispatch the batch immediately
                    self.dispatch_batch(awaiting.into());

                    continue;
                }

                self.currently_waiting_for = Some(awaiting);
            }
        }
    }

    fn handle_received_message(&mut self, notification: ResponseMessage) {
        let info = self.currently_waiting_for.as_mut().unwrap();

        let curr_seq = info.info().update_batch().sequence_number();

        match &notification {
            ResponseMessage::WroteMessage(seq, _) |
            ResponseMessage::Proof(seq) |
            ResponseMessage::WroteMetadata(seq) => {
                if curr_seq == *seq {
                    Self::process_incoming_message(info, notification);
                } else {
                    self.process_ahead_message(seq.clone(), notification);
                }
            }
            _ => {}
        }
    }

    fn process_pending_messages_for_current(&mut self, awaiting: &mut AwaitingPersistence<S>) {
        let seq_num = awaiting.info().update_batch().sequence_number();

        //Remove the messages that we have already received
        let messages_ahead = self.messages_received_ahead.remove(&seq_num);

        if let Some(messages_ahead) = messages_ahead {
            for persisted_message in messages_ahead {
                Self::process_incoming_message(awaiting, persisted_message);
            }
        }
    }

    fn dispatch_batch(&self, batch: BatchExecutionInfo<S>) {
        let (info, requests, batch) = batch.into();

        let checkpoint = match info {
            Info::Nil => self.executor_handle.queue_update(requests),
            Info::BeginCheckpoint => self
                .executor_handle
                .queue_update_and_get_appstate(requests),
        };

        if let Err(err) = checkpoint {
            error!("Failed to enqueue consensus {:?}", err);
        }
    }

    fn process_ahead_message(&mut self, seq: SeqNo, notification: ResponseMessage) {
        if let Some(received_msg) = self.messages_received_ahead.get_mut(&seq) {
            received_msg.push(notification);
        } else {
            self.messages_received_ahead.insert(seq, vec![notification]);
        }
    }

    fn process_incoming_message(awaiting: &mut AwaitingPersistence<S>, msg: ResponseMessage) {
        let result = awaiting.handle_incoming_message(msg);

        match result {
            Ok(result) => {
                if !result {
                    warn!("Received message for consensus instance {:?} but was not expecting it?", awaiting.info.update_batch().sequence_number());
                }
            }
            Err(err) => {
                error!("Received message that does not match up with what we were expecting {:?}", err);
            }
        }
    }
}

impl<S> From<BacklogMessage<S>> for AwaitingPersistence<S> where S: Service
{
    fn from(value: BacklogMessage<S>) -> Self {
        let pending_rq = match &value {
            BacklogMsg::Batch(info) => {
                // We can unwrap the completed batch as this was received here
                let completed_batch_info = info.completed_batch().as_ref().unwrap();

                PendingRq::Batch(completed_batch_info.messages_to_persist().clone(),
                                 Some(info.update_batch().sequence_number()))
            }
            BacklogMsg::Proof(info) => {
                PendingRq::Proof(Some(info.update_batch().sequence_number()))
            }
        };

        AwaitingPersistence {
            info: value.into(),
            pending_rq,
        }
    }
}

impl<S> Into<BatchExecutionInfo<S>> for AwaitingPersistence<S> where S: Service {
    fn into(self) -> BatchExecutionInfo<S> {
        self.info
    }
}

impl<S> Into<BatchExecutionInfo<S>> for BacklogMsg<S> where S: Service {
    fn into(self) -> BatchExecutionInfo<S> {
        match self {
            BacklogMsg::Batch(info) => {
                info
            }
            BacklogMsg::Proof(info) => {
                info
            }
        }
    }
}

impl<S> AwaitingPersistence<S> where S: Service {
    pub fn info(&self) -> &BatchExecutionInfo<S> {
        &self.info
    }

    pub fn is_ready_for_execution(&self) -> bool {
        match &self.pending_rq {
            PendingRq::Proof(opt) => {
                opt.is_none()
            }
            PendingRq::Batch(persistent, metadata) => {
                persistent.is_empty() && metadata.is_none()
            }
        }
    }

    pub fn handle_incoming_message(&mut self, msg: ResponseMessage) -> Result<bool> {
        if self.is_ready_for_execution() {
            return Ok(false);
        }

        match &mut self.pending_rq {
            PendingRq::Proof(sq_no) => {
                if let ResponseMessage::Proof(seq) = msg {
                    if seq == sq_no.unwrap() {
                        sq_no.take();

                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    Err(Error::simple_with_msg(ErrorKind::MsgLogPersistentConsensusBacklog, "Message received does not match up with the batch that we have received."))
                }
            }
            PendingRq::Batch(rqs, metadata) => {
                match msg {
                    ResponseMessage::WroteMetadata(_) => {
                        //We don't check the seq no because that is already checked before getting to this point
                        metadata.take();

                        Ok(true)
                    }
                    ResponseMessage::WroteMessage(_, persisted_message) => {
                        match rqs.iter().position(|p| *p == persisted_message) {
                            Some(msg_index) => {
                                rqs.swap_remove(msg_index);

                                Ok(true)
                            }
                            None => Ok(false),
                        }
                    }
                    _ => {
                        Err(Error::simple_with_msg(ErrorKind::MsgLogPersistentConsensusBacklog, "Message received does not match up with the batch that we have received."))
                    }
                }
            }
        }
    }
}