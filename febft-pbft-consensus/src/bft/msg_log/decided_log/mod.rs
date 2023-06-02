use std::mem::size_of;
use std::sync::Arc;
use log::error;
use atlas_common::crypto::hash::{Context, Digest};

use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_execution::app::{Request, Service, State, UpdateBatch};
use atlas_execution::serialize::SharedData;
use atlas_core::state_transfer::Checkpoint;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::msg_log::{Info, operation_key, CHECKPOINT_PERIOD};
use crate::bft::msg_log::decisions::{CollectData, DecisionLog, Proof, ProofMetadata};
use crate::bft::msg_log::deciding_log::{CompletedBatch, DecidingLog};
use crate::bft::msg_log::persistent::{InstallState, PersistentLog, WriteMode};
use crate::bft::sync::view::ViewInfo;

pub(crate) enum CheckpointState<D> {
    // no checkpoint has been performed yet
    None,
    // we are calling this a partial checkpoint because we are
    // waiting for the application state from the execution layer
    Partial {
        // sequence number of the last executed request
        seq: SeqNo,
    },
    PartialWithEarlier {
        // sequence number of the last executed request
        seq: SeqNo,
        // save the earlier checkpoint, in case corruption takes place
        earlier: Arc<ReadOnly<Checkpoint<D>>>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Arc<ReadOnly<Checkpoint<D>>>),
}

/// The log of decisions that have already been processed by the consensus
/// algorithm
pub struct Log<D> where D: SharedData + 'static {
    //This item will only be accessed by the replica request thread
    //The current stored SeqNo in the checkpoint state.
    //NOTE: THIS IS NOT THE CURR_SEQ NUMBER IN THE CONSENSUS
    curr_seq: SeqNo,

    // The log for all of the already decided consensus instances
    dec_log: DecisionLog<D::Request>,

    //The most recent checkpoint that we have.
    //Contains the app state and the last executed seq no on
    //That app state
    checkpoint: CheckpointState<D::State>,

    // A handle to the persistent log
    persistent_log: PersistentLog<D>,
}

/// Execution data for the given batch
/// Info: Whether we need to ask the executor for a checkpoint in order to reset the current message log
/// Update Batch: All of the requests that should be executed, in the correct order
/// Completed Batch: The information collected by the [DecidingLog], if applicable. (We can receive a batch
/// via a complete proof which means this will be [None] or we can process a batch normally, which means
/// this will be [Some(CompletedBatch<D>)])
pub struct BatchExecutionInfo<O> {
    info: Info,
    update_batch: UpdateBatch<O>,
    completed_batch: Option<CompletedBatch<O>>,
}

impl<D> Log<D> where D: SharedData + 'static {
    pub(crate) fn init_decided_log(node_id: NodeId, persistent_log: PersistentLog<D>, state: Option<Arc<ReadOnly<Checkpoint<D::State>>>>) -> Self {

        //TODO: Maybe read state from local storage?
        let checkpoint = if let Some(state) = state {
            CheckpointState::Complete(state)
        } else {
            CheckpointState::None
        };

        let dec_log = DecisionLog::new();

        Self {
            curr_seq: SeqNo::ZERO,
            dec_log,
            checkpoint,

            persistent_log,
        }
    }

    /// Returns a reference to a subset of this log, containing only
    /// consensus messages.
    pub fn decision_log(&self) -> &DecisionLog<D::Request> {
        &self.dec_log
    }

    pub fn mut_decision_log(&mut self) -> &mut DecisionLog<D::Request> {
        &mut self.dec_log
    }

    /// Read the current state, if existent, from the persistent storage
    ///
    /// FIXME: The view initialization might have to be changed if we want to introduce reconfiguration
    pub fn read_current_state(&self, n: usize, f: usize) -> Result<Option<(Arc<ReadOnly<Checkpoint<D::State>>>, ViewInfo, DecisionLog<D::Request>)>> {
        let option = self.persistent_log.read_state()?;

        if let Some(state) = option {
            let (view_seq, checkpoint, dec_log) = state;

            let view_seq = ViewInfo::new(view_seq, n, f)?;

            Ok(Some((checkpoint.clone(), view_seq, dec_log)))
        } else {
            Ok(None)
        }
    }

    /// Take a snapshot of the log, used to recover a replica.
    ///
    /// This method may fail if we are waiting for the latest application
    /// state to be returned by the execution layer.
    ///
    pub fn snapshot(&self, view: ViewInfo) -> Result<(Arc<ReadOnly<Checkpoint<D::State>>>, ViewInfo, DecisionLog<D::Request>)> {
        match &self.checkpoint {
            CheckpointState::Complete(checkpoint) =>
                Ok((checkpoint.clone(), view, self.dec_log.clone())),
            _ => Err("Checkpoint to be finalized").wrapped(ErrorKind::MsgLogPersistent),
        }
    }

    /// Insert a consensus message into the log.
    /// We can use this method when we want to prevent a clone, as this takes
    /// just a reference.
    /// This is mostly used for pre prepares as they contain all the requests and are therefore very expensive to send
    pub fn insert_consensus(
        &mut self,
        consensus_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
    ) {
        if let Err(err) = self
            .persistent_log
            .write_message(WriteMode::NonBlockingSync(None), consensus_msg)
        {
            error!("Failed to persist message {:?}", err);
        }
    }

    /// Install a proof of a consensus instance into the log.
    /// This is done when we receive the final SYNC message from the leader
    /// which contains all of the collects
    /// If we are missing the request determined by the
    pub fn install_proof(&mut self, seq: SeqNo, proof: Proof<D::Request>) -> Result<Option<BatchExecutionInfo<D::Request>>> {
        let batch_execution_info = BatchExecutionInfo::from(&proof);

        if let Some(decision) = self.decision_log().last_decision() {
            if decision.seq_no() == seq {
                // Well well well, if it isn't what I'm trying to add?
                //This should not be possible

                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidedLog,
                                                  "Already have decision at that seq no"));
            } else {
                self.mut_decision_log().append_proof(proof.clone());
            }
        }

        if let Err(err) = self.persistent_log
            .write_proof(WriteMode::NonBlockingSync(None), proof) {
            error!("Failed to persist proof {:?}", err);
        }

        // Communicate with the persistent log about persisting this batch and then executing it
        self.persistent_log.wait_for_proof_persistency_and_execute(batch_execution_info)
    }

    /// Clear the occurrences of a seq no from the decision log
    pub fn clear_last_occurrence(&mut self, seq: SeqNo) {
        if let Err(err) = self.persistent_log.write_invalidate(WriteMode::NonBlockingSync(None), seq) {
            error!("Failed to invalidate last occurrence {:?}", err);
        }
    }

    /// Update the log state, received from the CST protocol.
    pub fn install_state(&mut self, checkpoint: Arc<ReadOnly<Checkpoint<D::State>>>, dec_log: DecisionLog<D::Request>) {

        //Replace the log
        self.dec_log = dec_log.clone();

        let last_seq = self.dec_log.last_execution().unwrap_or(SeqNo::ZERO);

        // self.decided = rs.requests;
        self.checkpoint = CheckpointState::Complete(checkpoint.clone());
        self.curr_seq = last_seq.clone();

        if let Err(err) = self.persistent_log
            .write_install_state(WriteMode::NonBlockingSync(None),
                                 (last_seq, checkpoint, dec_log)) {
            error!("Failed to persist message {:?}", err);
        }
    }

    fn begin_checkpoint(&mut self, seq: SeqNo) -> Result<Info> {
        let earlier = std::mem::replace(&mut self.checkpoint, CheckpointState::None);

        self.checkpoint = match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => {
                CheckpointState::PartialWithEarlier { seq, earlier }
            }
            // FIXME: this may not be an invalid state after all; we may just be generating
            // checkpoints too fast for the execution layer to keep up, delivering the
            // hash digests of the appstate
            _ => {
                error!("Invalid checkpoint state detected");

                self.checkpoint = earlier;
                
                return Ok(Info::Nil);
            }
        };

        Ok(Info::BeginCheckpoint)
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, checkpoint: Arc<ReadOnly<Checkpoint<D::State>>>) -> Result<()> {
        match &self.checkpoint {
            CheckpointState::None => {
                Err("No checkpoint has been initiated yet").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Complete(_) => {
                Err("Checkpoint already finalized").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Partial { seq: _ } | CheckpointState::PartialWithEarlier { seq: _, .. } => {
                let final_seq = checkpoint.sequence_number();

                let checkpoint_state = CheckpointState::Complete(checkpoint.clone());

                self.checkpoint = checkpoint_state;

                let mut decided_request_count;

                //Clear the log of messages up to final_seq.
                //Messages ahead of final_seq will not be removed as they are not included in the
                //Checkpoint and therefore must be logged.
                {
                    let mut guard = &mut self.dec_log;

                    decided_request_count = guard.clear_until_seq(final_seq);

                    if let Some(last_sq) = guard.last_decision() {
                        // store the id of the last received pre-prepare,
                        // which corresponds to the request currently being
                        // processed
                        self.curr_seq = last_sq.sequence_number();
                    } else {
                        self.curr_seq = final_seq;
                    }
                }

                self.persistent_log.write_checkpoint(WriteMode::NonBlockingSync(None), checkpoint)?;

                Ok(())
            }
        }
    }

    /// Register that all the batches for a given decision have already been received
    /// Basically persists the metadata for a given consensus num
    pub fn all_batches_received(&mut self, metadata: ProofMetadata) {
        self.persistent_log.write_proof_metadata(WriteMode::NonBlockingSync(None),
                                                 metadata).unwrap();
    }

    /// Finalize a batch of client requests decided on the consensus instance
    /// with sequence number `seq`, retrieving the payload associated with their
    /// given digests `digests`.
    ///
    /// The decided log may be cleared resulting from this operation. Check the enum variant of
    /// `Info`, to perform a local checkpoint when appropriate.
    ///
    /// Returns a [`Option::None`] when we are running in Strict mode, indicating the
    /// batch request has been put in the execution queue, waiting for all of the messages
    /// to be persisted
    pub fn finalize_batch(
        &mut self,
        seq: SeqNo,
        completed_batch: CompletedBatch<D::Request>,
    ) -> Result<Option<BatchExecutionInfo<D::Request>>> {
        //println!("Finalized batch of OPS seq {:?} on Node {:?}", seq, self.node_id);

        let batch = {
            let mut batch = UpdateBatch::new_with_cap(seq, completed_batch.request_count());

            for message in completed_batch.pre_prepare_messages() {
                let reqs = {
                    if let ConsensusMessageKind::PrePrepare(reqs) = (*message.message().kind()).clone() {
                        reqs
                    } else { unreachable!() }
                };

                for (header, message) in reqs.into_iter()
                    .map(|x| x.into_inner()) {
                    let _key = operation_key::<D::Request>(&header, &message);

                    //TODO: Maybe make this run on separate thread?
                    // let seq_no = latest_op_guard
                    //     .get(key)
                    //     .unwrap_or(&SeqNo::ZERO);
                    //
                    // if message.sequence_number() > *seq_no {
                    //     latest_op_guard.insert(key, message.sequence_number());
                    // }

                    batch.add(
                        header.from(),
                        message.session_id(),
                        message.sequence_number(),
                        message.into_inner_operation(),
                    );
                }
            }

            batch.append_batch_meta(completed_batch.batch_meta().clone());

            batch
        };

        let last_seq_no_u32 = u32::from(seq);

        let info = if last_seq_no_u32 > 0 && last_seq_no_u32 % CHECKPOINT_PERIOD == 0 {
            //We check that % == 0 so we don't start multiple checkpoints
            self.begin_checkpoint(seq)?
        } else {
            Info::Nil
        };

        // the last executed sequence number
        let f = 1;

        //
        // Finalize the execution and store the proof in the log as a proof
        // instead of an ongoing decision
        self.dec_log.finished_quorum_execution(&completed_batch, seq, f)?;

        // Queue the batch for the execution
        let result = self.persistent_log.wait_for_batch_persistency_and_execute(BatchExecutionInfo {
            info,
            update_batch: batch,
            completed_batch: Some(completed_batch),
        });

        result
    }

    /// Collects the most up to date data we have in store.
    /// Accepts the f for the view that it is looking for
    /// It must accept this f as the reconfiguration of the network
    /// can alter the f from one seq no to the next
    pub fn last_proof(&self, f: usize) -> Option<Proof<D::Request>> {
        self.dec_log.last_decision()
    }
}

impl<O> BatchExecutionInfo<O> {
    pub fn info(&self) -> &Info {
        &self.info
    }
    pub fn update_batch(&self) -> &UpdateBatch<O> {
        &self.update_batch
    }
    pub fn completed_batch(&self) -> &Option<CompletedBatch<O>> {
        &self.completed_batch
    }
}

impl<O> Into<(Info, UpdateBatch<O>, Option<CompletedBatch<O>>)> for BatchExecutionInfo<O> {
    fn into(self) -> (Info, UpdateBatch<O>, Option<CompletedBatch<O>>) {
        (self.info, self.update_batch, self.completed_batch)
    }
}

impl<O> From<&Proof<O>> for BatchExecutionInfo<O> where O: Clone {
    fn from(value: &Proof<O>) -> Self {
        let mut update_batch = UpdateBatch::new(value.seq_no());

        if !value.are_pre_prepares_ordered().unwrap() {
            //The batch should be provided to this already ordered.
            todo!()
        }

        for pre_prepare in value.pre_prepares() {
            let consensus_msg = (*pre_prepare.message()).clone();

            let reqs = match consensus_msg.into_kind() {
                ConsensusMessageKind::PrePrepare(reqs) => { reqs }
                _ => {
                    unreachable!()
                }
            };

            for request in reqs {
                let (header, message) = request.into_inner();

                update_batch.add(header.from(),
                                 message.session_id(),
                                 message.sequence_number(),
                                 message.into_inner_operation());
            }
        }

        Self {
            info: Info::Nil,
            update_batch,
            completed_batch: None,
        }
    }
}