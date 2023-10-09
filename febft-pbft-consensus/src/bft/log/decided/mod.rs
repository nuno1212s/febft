use crate::bft::log::decisions::Proof;

/// A necessary decision log for the ability to perform view changes.
/// Only stores the latest performed decision
pub struct OngoingDecisionLog<O> {
    /// The last decision that was performed by the ordering protocol
    last_decision: Option<Proof<O>>
}

impl<O> OngoingDecisionLog<O> {

    fn init(last_proof: Option<Proof<O>>) -> Self {
        OngoingDecisionLog {
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