use std::collections::BTreeMap;

use log::{error, warn};
use atlas_common::channel;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::crypto::hash::Digest;

use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_execution::app::Service;
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::SharedData;
use crate::bft::msg_log::decided_log::BatchExecutionInfo;
use crate::bft::msg_log::Info;
use crate::bft::msg_log::persistent::{ResponseMessage, ResponseMsg};

///This is made to handle the backlog when the consensus is working faster than the persistent storage layer.
/// It holds update batches that are yet to be executed since they are still waiting for the confirmation of the persistent log
/// This is only needed (and only instantiated) when the persistency mode is strict
pub struct ConsensusBacklog<D: SharedData> {
    rx: ChannelSyncRx<BacklogMessage<D::Request>>,

    //Receives messages from the persistent log
    logger_rx: ChannelSyncRx<ResponseMsg>,

    //The handle to the executor
    executor_handle: ExecutorHandle<D>,

    //This is the batch that is currently waiting for it's messages to be persisted
    //Even if we already persisted the consensus instance that came after it (for some reason)
    // We can only deliver it when all the previous ones have been delivered,
    // As it must be ordered
    currently_waiting_for: Option<AwaitingPersistence<D::Request>>,

    //Message confirmations that we have already received but pertain to a further ahead consensus instance
    messages_received_ahead: BTreeMap<SeqNo, Vec<ResponseMessage>>,
}

type BacklogMessage<O> = BacklogMsg<O>;

enum BacklogMsg<O> {
    Batch(BatchExecutionInfo<O>),
    Proof(BatchExecutionInfo<O>),
}

struct AwaitingPersistence<O> {
    info: BatchExecutionInfo<O>,
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
pub struct ConsensusBackLogHandle<O> {
    rq_tx: ChannelSyncTx<BacklogMessage<O>>,
    logger_tx: ChannelSyncTx<ResponseMsg>,
}

impl<O> ConsensusBackLogHandle<O> {
    pub fn logger_tx(&self) -> ChannelSyncTx<ResponseMsg> {
        self.logger_tx.clone()
    }

    /// Queue a normal processed batch, where we received all messages individually and persisted
    /// them individually
    pub fn queue_batch(&self, batch: BatchExecutionInfo<O>) -> Result<()> {
        if let Err(err) = self.rq_tx.send(BacklogMsg::Batch(batch)) {
            Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent, format!("{:?}", err).as_str()))
        } else {
            Ok(())
        }
    }

    /// Queue a batch that we received via a proof and therefore only need to wait for the persistence
    /// of the entire proof, instead of the individual messages
    pub fn queue_batch_proof(&self, batch: BatchExecutionInfo<O>) -> Result<()> {
        if let Err(err) = self.rq_tx.send(BacklogMsg::Proof(batch)) {
            Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent, format!("{:?}", err).as_str()))
        } else {
            Ok(())
        }
    }
}

impl<O> Clone for ConsensusBackLogHandle<O> {
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

impl<D: SharedData + 'static> ConsensusBacklog<D> {
    ///Initialize the consensus backlog
    pub fn init_backlog(executor: ExecutorHandle<D>) -> ConsensusBackLogHandle<D::Request> {
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

    fn process_pending_messages_for_current(&mut self, awaiting: &mut AwaitingPersistence<D::Request>) {
        let seq_num = awaiting.info().update_batch().sequence_number();

        //Remove the messages that we have already received
        let messages_ahead = self.messages_received_ahead.remove(&seq_num);

        if let Some(messages_ahead) = messages_ahead {
            for persisted_message in messages_ahead {
                Self::process_incoming_message(awaiting, persisted_message);
            }
        }
    }

    fn dispatch_batch(&self, batch: BatchExecutionInfo<D::Request>) {
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

    fn process_incoming_message(awaiting: &mut AwaitingPersistence<D::Request>, msg: ResponseMessage) {
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

impl<O> From<BacklogMessage<O>> for AwaitingPersistence<O>
{
    fn from(value: BacklogMessage<O>) -> Self {
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

impl<O> Into<BatchExecutionInfo<O>> for AwaitingPersistence<O> {
    fn into(self) -> BatchExecutionInfo<O> {
        self.info
    }
}

impl<O> Into<BatchExecutionInfo<O>> for BacklogMsg<O> {
    fn into(self) -> BatchExecutionInfo<O> {
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

impl<O> AwaitingPersistence<O> {
    pub fn info(&self) -> &BatchExecutionInfo<O> {
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