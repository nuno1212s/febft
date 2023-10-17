use either::Either;

use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::Header;
use atlas_core::messages::{ClientRqInfo, RequestMessage};
use atlas_core::ordering_protocol::{Decision, ProtocolConsensusDecision};
use atlas_smr_application::app::UpdateBatch;
use atlas_smr_application::serialize::ApplicationData;

use crate::bft::log::decided::DecisionLog;
use crate::bft::log::deciding::{CompletedBatch, FinishedMessageLog};
use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::message::ConsensusMessageKind;
use crate::bft::OPDecision;

pub mod decided;
pub mod deciding;
pub mod decisions;

pub struct Log<D> where D: ApplicationData {
    decided: DecisionLog<D::Request>,
}

impl<D> Log<D> where D: ApplicationData {
    pub fn decision_log(&self) -> &DecisionLog<D::Request> {
        &self.decided
    }

    pub fn last_proof(&self) -> Option<Proof<D::Request>> {
        self.decided.last_decision()
    }

    pub fn install_proof(&mut self, proof: Proof<D::Request>) -> Result<OPDecision<D::Request>> {
        if let Some(decision) = self.decision_log().last_execution() {
            match proof.seq_no().index(decision) {
                Either::Left(_) | Either::Right(0) => {
                    return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidedLog,
                                                      "Cannot install proof as we already have a decision that is >= to the one that was provided"));
                }
                Either::Right(1) => {
                    self.decided.append_proof(proof.clone());
                }
                Either::Right(_) => {
                    return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidedLog,
                                                      "Cannot install proof as it would involve skipping a decision that we are not aware of"));
                }
            }
        }

        let batch_info = ProtocolConsensusDecision::from(&proof);
        let sequence = proof.sequence_number();

        let (metadata, messages) = proof.into_parts();

        Ok(Decision::full_decision_info(sequence, metadata, messages, batch_info))
    }

    pub fn finalize_batch(&mut self, completed: CompletedBatch<D::Request>) -> Result<ProtocolConsensusDecision<D::Request>> {
        let CompletedBatch {
            seq, digest,
            pre_prepare_ordering,
            contained_messages,
            client_request_info,
            client_requests,
            batch_meta
        } = completed;

        let metadata = ProofMetadata::new(seq, digest.clone(), pre_prepare_ordering,
                                          client_requests.len());

        let FinishedMessageLog {
            pre_prepares,
            prepares,
            commits
        } = contained_messages;

        let proof = Proof::new(
            metadata,
            pre_prepares,
            prepares,
            commits,
        );

        self.decided.append_proof(proof);

        let mut batch = UpdateBatch::new_with_cap(seq, client_requests.len());

        for cli_rq in client_requests {
            let (header, rq) = cli_rq.into_inner();

            batch.add(header.from(), rq.session_id(), rq.sequence_number(), rq.into_inner_operation());
        }

        Ok(ProtocolConsensusDecision::new(seq, batch, client_request_info, digest))
    }
}

pub fn initialize_decided_log<D>(node_id: NodeId) -> Log<D> where D: ApplicationData {
    Log {
        decided: DecisionLog::init(None),
    }
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    operation_key_raw(header.from(), message.session_id())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}

impl<O> From<&Proof<O>> for ProtocolConsensusDecision<O> where O: Clone {
    fn from(value: &Proof<O>) -> Self {
        let mut update_batch = UpdateBatch::new_with_cap(value.seq_no(), value.metadata().contained_client_rqs());
        let mut client_rqs = Vec::with_capacity(value.metadata().contained_client_rqs());

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
                client_rqs.push(ClientRqInfo::from(&request));

                let (header, message) = request.into_inner();

                update_batch.add(header.from(),
                                 message.session_id(),
                                 message.sequence_number(),
                                 message.into_inner_operation());
            }
        }

        ProtocolConsensusDecision::new(value.seq_no(),
                                       update_batch,
                                       client_rqs,
                                       value.metadata().batch_digest())
    }
}
