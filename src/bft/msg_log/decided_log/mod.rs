use std::sync::Arc;
use log::error;

use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, StoredMessage};
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{Request, Service, State, UpdateBatch};
use crate::bft::msg_log::{Info, operation_key, PERIOD};
use crate::bft::msg_log::decisions::{Checkpoint, DecisionLog, Proof};
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::error::*;
use crate::bft::globals::ReadOnly;
use crate::bft::msg_log::deciding_log::CompletedBatch;
use crate::bft::msg_log::persistent::{PersistentLog, WriteMode};
use crate::bft::sync::view::ViewInfo;

pub(crate) enum CheckpointState<S> {
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
        earlier: Arc<ReadOnly<Checkpoint<S>>>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Arc<ReadOnly<Checkpoint<S>>>),
}

/// The log of decisions that have already been processed by the consensus
/// algorithm
pub struct DecidedLog<S> where S: Service + 'static {
    //This item will only be accessed by the replica request thread
    //The current stored SeqNo in the checkpoint state.
    //NOTE: THIS IS NOT THE CURR_SEQ NUMBER IN THE CONSENSUS
    curr_seq: SeqNo,

    //This will only be accessed by the replica processing thread since requests will only be
    //Decided by the consensus protocol, which operates completely in the replica thread
    dec_log: DecisionLog<Request<S>>,

    //The most recent checkpoint that we have.
    //Contains the app state and the last executed seq no on
    //That app state
    checkpoint: CheckpointState<State<S>>,

    // A handle to the persistent log
    persistent_log: PersistentLog<S>,
}

/// Execution data for the given batch
/// Info: Whether we need to ask the executor for a checkpoint in order to reset the current message log
/// Update Batch: All of the requests that should be executed, in the correct order
/// Completed Batch: The information collected by the [DecidingLog], if applicable. (We can receive a batch
/// via a complete proof which means this will be [None] or we can process a batch normally, which means
/// this will be [Some(CompletedBatch<S>)])
pub struct BatchExecutionInfo<S> where S: Service {
    info: Info,
    update_batch: UpdateBatch<Request<S>>,
    completed_batch: Option<CompletedBatch<S>>,
}

impl<S> DecidedLog<S> where S: Service + 'static {
    pub(crate) fn init_decided_log(persistent_log: PersistentLog<S>) -> Self {

        //TODO: Maybe read state from local storage?

        Self {
            curr_seq: SeqNo::ZERO,
            dec_log: DecisionLog::new(),
            checkpoint: CheckpointState::None,

            persistent_log,
        }
    }

    /// Returns a reference to a subset of this log, containing only
    /// consensus messages.
    pub fn decision_log(&self) -> &DecisionLog<Request<S>> {
        &self.dec_log
    }

    pub fn mut_decision_log(&mut self) -> &mut DecisionLog<Request<S>> {
        &mut self.dec_log
    }

    /// Read the current state, if existent, from the persistent storage
    ///
    /// FIXME: The view initialization might have to be changed if we want to introduce reconfiguration
    pub fn read_current_state(&self, n: usize, f: usize) -> Result<Option<RecoveryState<State<S>, Request<S>>>> {
        let option = self.persistent_log.read_state()?;

        if let Some(state) = option {
            let view_seq = ViewInfo::new(state.0, n, f)?;

            let mut requests = Vec::new();

            for proof in state.2.proofs() {
                if !proof.are_pre_prepares_ordered()? {
                    todo!()
                }

                for request in proof.pre_prepares() {
                    let pre_prepare_rqs = match request.message().kind() {
                        ConsensusMessageKind::PrePrepare(requests) => {
                            requests.clone()
                        }
                        _ => { unreachable!() }
                    };

                    for request in pre_prepare_rqs {
                        requests.push(request.into_inner().1.into_inner_operation());
                    }
                }
            }

            Ok(Some(RecoveryState {
                view: view_seq,
                checkpoint: state.1,
                requests,
                declog: state.2,
            }))
        } else {
            Ok(None)
        }
    }

    /// Take a snapshot of the log, used to recover a replica.
    ///
    /// This method may fail if we are waiting for the latest application
    /// state to be returned by the execution layer.
    ///
    pub fn snapshot(&self, view: ViewInfo) -> Result<RecoveryState<State<S>, Request<S>>> {
        match &self.checkpoint {
            CheckpointState::Complete(checkpoint) => Ok(RecoveryState::new(
                view,
                checkpoint.clone(),
                //TODO:
                vec![],
                self.dec_log.clone(),
            )),
            _ => Err("Checkpoint to be finalized").wrapped(ErrorKind::MsgLogPersistent),
        }
    }

    /// Insert a consensus message into the log.
    /// We can use this method when we want to prevent a clone, as this takes
    /// just a reference.
    /// This is mostly used for pre prepares as they contain all the requests and are therefore very expensive to send
    pub fn insert_consensus(
        &mut self,
        consensus_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        //These messages can only be sent by replicas, so the dec_log
        //Is only accessed by one thread.

        //Wrap the message in a read only reference so we can then pass it around without having to clone it everywhere,
        //Saving a lot of copies especially when sending things to the asynchronous logging
        let mut dec_log = &mut self.dec_log;

        match consensus_msg.message().kind() {
            ConsensusMessageKind::PrePrepare(_) => dec_log.append_pre_prepare(consensus_msg.clone()),
            ConsensusMessageKind::Prepare(_) => dec_log.append_prepare(consensus_msg.clone()),
            ConsensusMessageKind::Commit(_) => dec_log.append_commit(consensus_msg.clone()),
        }

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
    pub fn install_proof(&mut self, seq: SeqNo, proof: Proof<Request<S>>) -> Result<Option<BatchExecutionInfo<S>>> {
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
        self.mut_decision_log().clear_last_occurrences(seq, None);

        if let Err(err) = self.persistent_log.write_invalidate(WriteMode::NonBlockingSync(None), seq) {
            error!("Failed to invalidate last occurrence {:?}", err);
        }
    }

    /// Update the log state, received from the CST protocol.
    pub fn install_state(&mut self, last_seq: SeqNo, rs: RecoveryState<State<S>, Request<S>>) {

        //Replace the log
        self.dec_log = rs.declog.clone();

        // self.decided = rs.requests;
        self.checkpoint = CheckpointState::Complete(rs.checkpoint.clone());
        self.curr_seq = last_seq.clone();

        if let Err(err) = self.persistent_log
            .write_install_state(WriteMode::NonBlockingSync(None),
                                 (last_seq, rs.checkpoint, rs.declog)) {
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
            _ => return Err("Invalid checkpoint state detected").wrapped(ErrorKind::MsgLog),
        };

        Ok(Info::BeginCheckpoint)
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, final_seq: SeqNo, appstate: State<S>) -> Result<()> {
        match &self.checkpoint {
            CheckpointState::None => {
                Err("No checkpoint has been initiated yet").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Complete(_) => {
                Err("Checkpoint already finalized").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Partial { seq: _ } | CheckpointState::PartialWithEarlier { seq: _, .. } => {
                let checkpoint_state = CheckpointState::Complete(
                    Checkpoint::new(final_seq, appstate),
                );

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

                // This will always execute, I just wanted to unpack the checkpoint
                // Persist the newly received state
                if let CheckpointState::Complete(checkpoint) = &self.checkpoint {
                    self.persistent_log.write_checkpoint(WriteMode::NonBlockingSync(None), Arc::clone(checkpoint))?;
                }

                //Clear the decided requests log

                /*{
                    let mut decided = &mut self.de;

                    if decided_request_count < decided.len() {

                        let mut new_decided = Vec::with_capacity(decided.len() - decided_request_count);

                        let to_keep = decided.len() - decided_request_count;

                        for _ in 0..to_keep {
                            let rq_to_keep = decided.pop().unwrap();

                            new_decided.push(rq_to_keep);
                        }

                        //Get the requests in the correct order as we have inverted the order with the previous operation
                        new_decided.reverse();

                        drop(decided);

                        self.decided.replace(new_decided);
                    } else if decided_request_count == decided.len() {
                        decided.clear();
                    } else {
                        //We can't have more decided requests than decided requests LOL
                        unreachable!()
                    }
                }*/

                // {@
                // Observer code
                // @}

                /*
                if let Some(observer) = &self.observer {
                    observer
                        .tx()
                        .send(MessageType::Event(ObserveEventKind::CheckpointEnd(
                            self.curr_seq.get(),
                        )))
                        .unwrap();
                }
                */

                //
                // @}
                //

                Ok(())
            }
        }
    }

    /// Register that all the batches for a given decision have already been received
    pub fn all_batches_received(&mut self, digest: Digest, pre_prepare_ordering: Vec<Digest>) {
        let metadata = self.dec_log.all_batches_received(digest, pre_prepare_ordering.clone());

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
        completed_batch: CompletedBatch<S>,
    ) -> Result<Option<BatchExecutionInfo<S>>> {
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
                    let _key = operation_key::<Request<S>>(&header, &message);

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

        let info = if last_seq_no_u32 > 0 && last_seq_no_u32 % PERIOD == 0 {
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
        self.dec_log.finished_quorum_execution(seq, f).expect("Failed to create proof for the current instance");

        // Queue the batch for the execution
        let result = self.persistent_log.wait_for_batch_persistency_and_execute(BatchExecutionInfo {
            info,
            update_batch: batch,
            completed_batch: Some(completed_batch),
        });
        
        result
    }
}

impl<S> BatchExecutionInfo<S> where S: Service {
    pub fn info(&self) -> &Info {
        &self.info
    }
    pub fn update_batch(&self) -> &UpdateBatch<Request<S>> {
        &self.update_batch
    }
    pub fn completed_batch(&self) -> &Option<CompletedBatch<S>> {
        &self.completed_batch
    }
}

impl<S> Into<(Info, UpdateBatch<Request<S>>, Option<CompletedBatch<S>>)> for BatchExecutionInfo<S> where S: Service {
    fn into(self) -> (Info, UpdateBatch<Request<S>>, Option<CompletedBatch<S>>) {
        (self.info, self.update_batch, self.completed_batch)
    }
}

impl<S> From<&Proof<Request<S>>> for BatchExecutionInfo<S> where S: Service {
    fn from(value: &Proof<Request<S>>) -> Self {
        let mut update_batch = UpdateBatch::new(value.seq_no());

        if !value.are_pre_prepares_ordered().unwrap() {
            //The batch should be provided to this already ordered.
            todo!()
        }

        for pre_prepare in value.pre_prepares() {
            let reqs = match pre_prepare.message().kind().clone() {
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