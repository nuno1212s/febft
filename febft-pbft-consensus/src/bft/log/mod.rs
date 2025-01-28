use atlas_common::Err;
use either::Either;
use thiserror::Error;

use atlas_common::error::*;
use atlas_common::maybe_vec::MaybeVec;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::serialization_helper::SerMsg;
use atlas_communication::message::Header;
use atlas_core::messages::{ClientRqInfo, SessionBased};
use atlas_core::ordering_protocol::{BatchedDecision, Decision, ProtocolConsensusDecision};

use crate::bft::log::decided::DecisionLog;
use crate::bft::log::deciding::{CompletedBatch, FinishedMessageLog};
use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::message::ConsensusMessageKind;
use crate::bft::FeDecision;

pub mod decided;
pub mod deciding;
pub mod decisions;

pub struct Log<RQ>
where
    RQ: SerMsg,
{
    decided: DecisionLog<RQ>,
}

impl<RQ> Log<RQ>
where
    RQ: SerMsg + SessionBased,
{
    pub fn decision_log(&self) -> &DecisionLog<RQ> {
        &self.decided
    }

    pub fn last_proof(&self) -> Option<Proof<RQ>> {
        self.decided.last_decision()
    }

    pub fn install_proof(&mut self, proof: Proof<RQ>) -> Result<FeDecision<RQ>> {
        if let Some(decision) = self.decision_log().last_execution() {
            match proof.seq_no().index(decision) {
                Either::Left(_) | Either::Right(0) => {
                    return Err!(LogError::CannotInstallDecisionAlreadyAhead {
                        already_installed: decision,
                        install_attempt: proof.sequence_number()
                    });
                }
                Either::Right(1) => {
                    self.decided.append_proof(proof.clone());
                }
                Either::Right(_) => {
                    return Err!(LogError::CannotInstallWouldSkip {
                        install_attempt: proof.sequence_number(),
                        currently_installed: decision
                    });
                }
            }
        }

        let batch_info = ProtocolConsensusDecision::from(&proof);
        let sequence = proof.sequence_number();

        let (metadata, messages) = proof.into_parts();

        Ok(Decision::full_decision_info(
            sequence, metadata, MaybeVec::None, MaybeVec::from_many(messages), batch_info,
        ))
    }

    pub fn finalize_batch(
        &mut self,
        completed: CompletedBatch<RQ>,
    ) -> Result<ProtocolConsensusDecision<RQ>> {
        let CompletedBatch {
            seq,
            digest,
            pre_prepare_ordering,
            contained_messages,
            client_request_info,
            client_requests,
            batch_meta: _,
        } = completed;

        let metadata = ProofMetadata::new(seq, digest, pre_prepare_ordering, client_requests.len());

        let FinishedMessageLog {
            pre_prepares,
            prepares,
            commits,
        } = contained_messages;

        let proof = Proof::new(metadata, pre_prepares, prepares, commits);

        self.decided.append_proof(proof);

        let mut batch = BatchedDecision::new_with_cap(seq, client_requests.len());

        for cli_rq in client_requests {
            batch.add_message(cli_rq);
        }

        Ok(ProtocolConsensusDecision::new(
            seq,
            batch,
            client_request_info,
            digest,
        ))
    }
}

pub fn initialize_decided_log<RQ>(_node_id: NodeId) -> Log<RQ>
where
    RQ: SerMsg,
{
    Log {
        decided: DecisionLog::init(None),
    }
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &O) -> u64
where
    O: SessionBased,
{
    operation_key_raw(header.from(), message.session_number())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}

impl<O> From<&Proof<O>> for ProtocolConsensusDecision<O>
where
    O: Clone + SessionBased,
{
    fn from(value: &Proof<O>) -> Self {
        let mut decided_batch =
            BatchedDecision::new_with_cap(value.seq_no(), value.metadata().contained_client_rqs());
        let mut client_rqs = Vec::with_capacity(value.metadata().contained_client_rqs());

        if !value.are_pre_prepares_ordered().unwrap() {
            //The batch should be provided to this already ordered.
            todo!()
        }

        for pre_prepare in value.pre_prepares() {
            let consensus_msg = (*pre_prepare.message()).clone();

            let reqs = match consensus_msg.into_consensus().into_kind() {
                ConsensusMessageKind::PrePrepare(reqs) => reqs,
                _ => {
                    unreachable!()
                }
            };

            for request in reqs {
                client_rqs.push(ClientRqInfo::from(&request));

                decided_batch.add_message(request);
            }
        }

        ProtocolConsensusDecision::new(
            value.seq_no(),
            decided_batch,
            client_rqs,
            value.metadata().batch_digest(),
        )
    }
}

#[derive(Error, Debug)]
pub enum LogError {
    #[error("Failed to install decision {install_attempt:?} as we already have decision {already_installed:?}")]
    CannotInstallDecisionAlreadyAhead {
        install_attempt: SeqNo,
        already_installed: SeqNo,
    },
    #[error("Failed to install decision {install_attempt:?} as we are only on decision {currently_installed:?}")]
    CannotInstallWouldSkip {
        install_attempt: SeqNo,
        currently_installed: SeqNo,
    },
}
