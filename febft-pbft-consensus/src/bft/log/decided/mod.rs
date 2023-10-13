use atlas_common::ordering::{Orderable, SeqNo};
use crate::bft::log::decisions::Proof;

/// A necessary decision log for the ability to perform view changes.
/// Only stores the latest performed decision
pub struct DecisionLog<O> {
    /// The last decision that was performed by the ordering protocol
    last_decision: Option<Proof<O>>
}

impl<O> DecisionLog<O> {

    fn init(last_proof: Option<Proof<O>>) -> Self {
        DecisionLog {
            last_decision: last_proof,
        }
    }

    /// Install a given proof
    fn install_proof(&mut self, proof: Proof<O>) {
        self.last_decision = Some(proof)
    }

    /// Get the last decision
    fn last_decision(&self) -> Option<Proof<O>>  {
        self.last_decision.clone()
    }
    
}

impl<O> DecisionLog<O> {
    pub fn new() -> Self {
        Self {
            last_decision: None,
        }
    }

    pub fn from_decided(proof: Proof<O>) -> Self {
        Self {
            last_decision: Some(proof),
        }
    }

    pub fn from_proofs(mut proofs: Vec<Proof<O>>) -> Self {

        proofs.sort_by(|a, b| a.sequence_number().cmp(&b.sequence_number()).reverse());

        let last_decided = proofs.first().map(|proof| proof.sequence_number());

        Self {
            last_exec: last_decided,
            decided: proofs,
        }
    }

    /// Returns the sequence number of the last executed batch of client
    /// requests, assigned by the conesensus layer.
    pub fn last_execution(&self) -> Option<SeqNo> {
        self.last_decision.map(|proof| proof.sequence_number())
    }

    /// Append a proof to the end of the log. Assumes all prior checks have been done
    pub(crate) fn append_proof(&mut self, proof: Proof<O>) {
        self.last_decision = Some(proof);
    }

    //TODO: Maybe make these data structures a BTreeSet so that the messages are always ordered
    //By their seq no? That way we cannot go wrong in the ordering of messages.
    pub(crate) fn finished_quorum_execution(&mut self, completed_batch: &CompletedBatch<O>, seq_no: SeqNo, f: usize) -> Result<()> {
        self.last_exec.replace(seq_no);

        let proof = completed_batch.proof(Some(f))?;

        self.decided.push(proof);

        Ok(())
    }
    /// Returns the proof of the last executed consensus
    /// instance registered in this `DecisionLog`.
    pub fn last_decision(&self) -> Option<Proof<O>> {
        self.decided.last().map(|p| (*p).clone())
    }

    /// Clear the decision log until the given sequence number
    pub(crate) fn clear_until_seq(&mut self, seq_no: SeqNo) -> usize {
        let mut net_decided = Vec::with_capacity(self.decided.len());

        let mut decided_request_count = 0;

        let prev_decided = std::mem::replace(&mut self.decided, net_decided);

        for proof in prev_decided.into_iter().rev() {
            if proof.seq_no <= seq_no {
                for pre_prepare in &proof.pre_prepares {
                    //Mark the requests contained in this message for removal
                    decided_request_count += match pre_prepare.message().kind() {
                        ConsensusMessageKind::PrePrepare(messages) => messages.len(),
                        _ => 0,
                    };
                }
            } else {
                self.decided.push(proof);
            }
        }

        self.decided.reverse();

        decided_request_count
    }
}

impl<O> Orderable for DecisionLog<O> {
    fn sequence_number(&self) -> SeqNo {
        self.last_exec.unwrap_or(SeqNo::ZERO)
    }
}
