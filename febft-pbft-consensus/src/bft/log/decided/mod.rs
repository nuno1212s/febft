use atlas_common::ordering::{Orderable, SeqNo};

use crate::bft::log::decisions::Proof;

/// A necessary decision log for the ability to perform view changes.
/// Only stores the latest performed decision
pub struct DecisionLog<O> {
    /// The last decision that was performed by the ordering protocol
    last_decision: Option<Proof<O>>,
}

impl<O> DecisionLog<O> {
    pub(crate) fn init(last_proof: Option<Proof<O>>) -> Self {
        DecisionLog {
            last_decision: last_proof,
        }
    }

    /// Install a given proof
    pub fn install_proof(&mut self, proof: Proof<O>) {
        self.last_decision = Some(proof)
    }

    /// Get the last decision
    pub fn last_decision(&self) -> Option<Proof<O>> {
        self.last_decision.clone()
    }

    pub fn last_execution(&self) -> Option<SeqNo> {
        self.last_decision
            .as_ref()
            .map(|decision| decision.sequence_number())
    }

    pub fn append_proof(&mut self, proof: Proof<O>) {
        self.last_decision = Some(proof)
    }
}

impl<O> Orderable for DecisionLog<O> {
    fn sequence_number(&self) -> SeqNo {
        self.last_decision
            .as_ref()
            .map(|f| f.sequence_number())
            .unwrap_or(SeqNo::ZERO)
    }
}
