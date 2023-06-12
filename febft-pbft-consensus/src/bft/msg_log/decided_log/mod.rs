use std::mem::size_of;
use std::sync::Arc;
use log::error;
use atlas_common::crypto::hash::{Context, Digest};

use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_communication::Node;
use atlas_core::ordering_protocol::{DecisionInformation, ExecutionResult, ProtocolConsensusDecision};
use atlas_core::persistent_log::{OrderingProtocolLog, StatefulOrderingProtocolLog, WriteMode};
use atlas_core::serialize::StateTransferMessage;
use atlas_execution::app::{Request, Service, State, UpdateBatch};
use atlas_execution::serialize::SharedData;
use atlas_core::state_transfer::Checkpoint;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::msg_log::{operation_key};
use crate::bft::msg_log::decisions::{CollectData, DecisionLog, Proof, ProofMetadata};
use crate::bft::msg_log::deciding_log::{CompletedBatch, DecidingLog};
use crate::bft::{PBFT, PBFTOrderProtocol};
use crate::bft::sync::view::ViewInfo;

/// The log of decisions that have already been processed by the consensus
/// algorithm
pub struct Log<D, PL> where D: SharedData + 'static {
    // The log for all of the already decided consensus instances
    dec_log: DecisionLog<D::Request>,

    // A handle to the persistent log
    persistent_log: PL,
}

impl<D, PL> Log<D, PL> where D: SharedData + 'static {
    pub(crate) fn init_decided_log(node_id: NodeId, persistent_log: PL,
                                   dec_log: Option<DecisionLog<D::Request>>) -> Self {
        Self {
            dec_log: dec_log.unwrap_or(DecisionLog::new()),
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
    pub fn read_current_state(&self, n: usize, f: usize) -> Result<Option<(ViewInfo, DecisionLog<D::Request>)>>
        where PL: StatefulOrderingProtocolLog<PBFTConsensus<D>, PBFTConsensus<D>> {
        let option = self.persistent_log.read_state(WriteMode::BlockingSync)?;

        if let Some((view, dec_log)) = option {
            Ok(Some((view, dec_log)))
        } else {
            Ok(None)
        }
    }

    /// Take a snapshot of the log, used to recover a replica.
    ///
    /// This method may fail if we are waiting for the latest application
    /// state to be returned by the execution layer.
    ///
    pub fn snapshot(&self, view: ViewInfo) -> Result<(ViewInfo, DecisionLog<D::Request>)> {
        Ok((view, self.dec_log.clone()))
    }

    /// Insert a consensus message into the log.
    /// We can use this method when we want to prevent a clone, as this takes
    /// just a reference.
    /// This is mostly used for pre prepares as they contain all the requests and are therefore very expensive to send
    pub fn insert_consensus(
        &mut self,
        consensus_msg: Arc<ReadOnly<StoredMessage<PBFTMessage<D::Request>>>>,
    ) where PL: OrderingProtocolLog<PBFTConsensus<D>>,
    {
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
    pub fn install_proof(&mut self, seq: SeqNo, proof: Proof<D::Request>) -> Result<ProtocolConsensusDecision<D::Request>>
        where PL: OrderingProtocolLog<PBFTConsensus<D>> {
        let batch_execution_info = ProtocolConsensusDecision::from(&proof);

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

        Ok(batch_execution_info)
    }

    /// Clear the occurrences of a seq no from the decision log
    pub fn clear_last_occurrence(&mut self, seq: SeqNo)
        where
            PL: OrderingProtocolLog<PBFTConsensus<D>> {
        if let Err(err) = self.persistent_log.write_invalidate(WriteMode::NonBlockingSync(None), seq) {
            error!("Failed to invalidate last occurrence {:?}", err);
        }
    }

    /// Update the log state, received from the CST protocol.
    pub fn install_state(&mut self, view: ViewInfo, dec_log: DecisionLog<D::Request>)
        where PL: StatefulOrderingProtocolLog<PBFTConsensus<D>, PBFTConsensus<D>> {

        //Replace the log
        self.dec_log = dec_log.clone();

        let last_seq = self.dec_log.last_execution().unwrap_or(SeqNo::ZERO);

        if let Err(err) = self.persistent_log
            .write_install_state(WriteMode::NonBlockingSync(None), view, dec_log) {
            error!("Failed to persist message {:?}", err);
        }
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, final_seq: SeqNo) -> Result<()> {
        let mut decided_request_count;

        //Clear the log of messages up to final_seq.
        //Messages ahead of final_seq will not be removed as they are not included in the
        //Checkpoint and therefore must be logged.
        {
            let mut guard = &mut self.dec_log;

            decided_request_count = guard.clear_until_seq(final_seq);
        }

        Ok(())
    }

    /// Register that all the batches for a given decision have already been received
    /// Basically persists the metadata for a given consensus num
    pub fn all_batches_received(&mut self, metadata: ProofMetadata) where
        PL: OrderingProtocolLog<PBFTConsensus<D>> {
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
    ) -> Result<ProtocolConsensusDecision<D::Request>> {
        //println!("Finalized batch of OPS seq {:?} on Node {:?}", seq, self.node_id);

        let batch = {
            let mut batch = UpdateBatch::new_with_cap(seq, completed_batch.request_count());

            for message in completed_batch.pre_prepare_messages() {
                let reqs = {
                    if let ConsensusMessageKind::PrePrepare(reqs) = (*message.message().consensus().kind()).clone() {
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

        // the last executed sequence number
        let f = 1;

        // Finalize the execution and store the proof in the log as a proof
        // instead of an ongoing decision
        self.dec_log.finished_quorum_execution(&completed_batch, seq, f)?;

        let decision = ProtocolConsensusDecision::new(seq, batch,
                                                      Some(DecisionInformation::from(completed_batch)));

        Ok(decision)
    }

    /// Collects the most up to date data we have in store.
    /// Accepts the f for the view that it is looking for
    /// It must accept this f as the reconfiguration of the network
    /// can alter the f from one seq no to the next
    pub fn last_proof(&self, f: usize) -> Option<Proof<D::Request>> {
        self.dec_log.last_decision()
    }
}

impl<O> From<&Proof<O>> for ProtocolConsensusDecision<O> where O: Clone {
    fn from(value: &Proof<O>) -> Self {
        let mut update_batch = UpdateBatch::new(value.seq_no());

        if !value.are_pre_prepares_ordered().unwrap() {
            //The batch should be provided to this already ordered.
            todo!()
        }

        for pre_prepare in value.pre_prepares() {
            let consensus_msg = (*pre_prepare.message()).clone();

            let reqs = match consensus_msg.into_consensus().into_kind() {
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

        ProtocolConsensusDecision::new(value.seq_no(),
                                       update_batch,
                                       None)
    }
}