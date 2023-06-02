use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use atlas_common::error::*;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use atlas_common::crypto::hash::Digest;
use atlas_common::globals::ReadOnly;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_core::serialize::{OrderProtocolLog, OrderProtocolProof};
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::msg_log::deciding_log::CompletedBatch;

pub type StoredConsensusMessage<O> = Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Decision<O> {
    seq_no: SeqNo,
    pre_prepare_ordering: Vec<Digest>,
    pre_prepares: Vec<StoredConsensusMessage<O>>,
    prepares: Vec<StoredConsensusMessage<O>>,
    commits: Vec<StoredConsensusMessage<O>>,
}

/// Contains all the decisions the consensus has decided since the last checkpoint.
/// The currently deciding variable contains the decision that is currently ongoing.
///
/// Cloning this decision log is actually pretty cheap (compared to the alternative of cloning
/// all requests executed since the last checkpoint) since it only has to clone the arcs (which is one atomic operation)
/// We can't wrap the entire vector since the decision log is actually constantly modified by the consensus
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct DecisionLog<O> {
    last_exec: Option<SeqNo>,
    decided: Vec<Proof<O>>,
}

/// Metadata about a proof
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct ProofMetadata {
    seq_no: SeqNo,
    batch_digest: Digest,
    pre_prepare_ordering: Vec<Digest>,
}

/// Represents a single decision from the `DecisionLog`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Proof<O> {
    metadata: ProofMetadata,
    pre_prepares: Vec<StoredConsensusMessage<O>>,
    prepares: Vec<StoredConsensusMessage<O>>,
    commits: Vec<StoredConsensusMessage<O>>,
}

/// Contains a collection of `ViewDecisionPair` values,
/// pertaining to a particular consensus instance.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct PrepareSet(pub Vec<ViewDecisionPair>);

/// Contains a sequence number pertaining to a particular view,
/// as well as a hash digest of a value decided in a consensus
/// instance of that view.
///
/// Corresponds to the `TimestampValuePair` class in `BFT-SMaRt`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct ViewDecisionPair(pub SeqNo, pub Digest);

/// Represents an incomplete decision from the `DecisionLog`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct IncompleteProof {
    in_exec: SeqNo,
    write_set: PrepareSet,
    quorum_prepares: Option<ViewDecisionPair>,
}

impl PrepareSet {
    /// Iterate over this `WriteSet`.
    ///
    /// Convenience method for calling `iter()` on the inner `Vec`.
    pub fn iter(&self) -> impl Iterator<Item=&ViewDecisionPair> {
        self.0.iter()
    }
}

impl ProofMetadata {
    /// Create a new proof metadata
    pub(crate) fn new(seq_no: SeqNo, digest: Digest, pre_prepare_ordering: Vec<Digest>) -> Self {
        Self {
            seq_no,
            batch_digest: digest,
            pre_prepare_ordering,
        }
    }

    pub fn seq_no(&self) -> SeqNo {
        self.seq_no
    }

    pub fn batch_digest(&self) -> Digest {
        self.batch_digest
    }

    pub fn pre_prepare_ordering(&self) -> &Vec<Digest> {
        &self.pre_prepare_ordering
    }
}

impl<O> Proof<O> {
    pub fn new(metadata: ProofMetadata,
               pre_prepares: Vec<StoredConsensusMessage<O>>,
               prepares: Vec<StoredConsensusMessage<O>>,
               commits: Vec<StoredConsensusMessage<O>>) -> Self {
        Self {
            metadata,
            pre_prepares,
            prepares,
            commits,
        }
    }

    /// Returns the `PRE-PREPARE` message of this `Proof`.
    pub fn pre_prepares(&self) -> &[StoredConsensusMessage<O>] {
        &self.pre_prepares[..]
    }

    /// Returns the `PREPARE` message of this `Proof`.
    pub fn prepares(&self) -> &[StoredConsensusMessage<O>] {
        &self.prepares[..]
    }

    /// Returns the `COMMIT` message of this `Proof`.
    pub fn commits(&self) -> &[StoredConsensusMessage<O>] {
        &self.commits[..]
    }

    /// Check if the amount of pre prepares line up with the expected amount
    fn check_pre_prepare_sizes(&self) -> Result<()> {
        if self.metadata.pre_prepare_ordering().len() != self.pre_prepares.len() {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                              "Wrong number of pre prepares."));
        }

        Ok(())
    }

    /// Check if the pre prepares stored here are ordered correctly according
    /// to the [pre_prepare_ordering] in this same proof.
    pub fn are_pre_prepares_ordered(&self) -> Result<bool> {
        self.check_pre_prepare_sizes()?;

        for index in 0..self.metadata.pre_prepare_ordering().len() {
            if self.metadata.pre_prepare_ordering()[index] != *self.pre_prepares[index].header().digest() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Order the pre prepares on this proof
    pub fn order_pre_prepares(&mut self) -> Result<()> {
        self.check_pre_prepare_sizes()?;

        let pre_prepare_ordering = self.metadata.pre_prepare_ordering();

        let mut ordered_pre_prepares = Vec::with_capacity(pre_prepare_ordering.len());

        for index in 0..pre_prepare_ordering.len() {
            let digest = pre_prepare_ordering[index];

            let pre_prepare = self.pre_prepares.iter().position(|msg| *msg.header().digest() == digest);

            match pre_prepare {
                Some(index) => {
                    let message = self.pre_prepares.swap_remove(index);

                    ordered_pre_prepares.push(message);
                }
                None => {
                    return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                      "Proof's batches do not match with the digests provided."));
                }
            }
        }

        self.pre_prepares = ordered_pre_prepares;

        Ok(())
    }

}

impl<O> Orderable for Proof<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl<O> Deref for Proof<O> {
    type Target = ProofMetadata;

    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl IncompleteProof {
    pub fn new(in_exec: SeqNo, write_set: PrepareSet, quorum_prepares: Option<ViewDecisionPair>) -> Self {
        Self {
            in_exec,
            write_set,
            quorum_prepares,
        }
    }

    /// Returns the sequence number of the consensus instance currently
    /// being executed.
    pub fn executing(&self) -> SeqNo {
        self.in_exec
    }

    /// Returns a reference to the `WriteSet` included in this `IncompleteProof`.
    pub fn write_set(&self) -> &PrepareSet {
        &self.write_set
    }

    /// Returns a reference to the quorum prepares included in this `IncompleteProof`,
    /// if any value was prepared in the previous view.
    pub fn quorum_prepares(&self) -> Option<&ViewDecisionPair> {
        self.quorum_prepares.as_ref()
    }
}

/// Contains data about the running consensus instance,
/// as well as the last stable proof acquired from the previous
/// consensus instance.
///
/// Corresponds to the class of the same name in `BFT-SMaRt`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct CollectData<O> {
    pub(crate) incomplete_proof: IncompleteProof,
    pub(crate) last_proof: Option<Proof<O>>,
}

impl<O> CollectData<O> {

    pub fn new(incomplete_proof: IncompleteProof, last_proof: Option<Proof<O>>) -> Self {
        Self { incomplete_proof, last_proof }
    }

    pub fn incomplete_proof(&self) -> &IncompleteProof {
        &self.incomplete_proof
    }

    pub fn last_proof(&self) -> Option<&Proof<O>> {
        self.last_proof.as_ref()
    }
}

impl<O> DecisionLog<O> {
    pub fn new() -> Self {
        Self {
            last_exec: None,
            decided: vec![],
        }
    }

    pub fn from_decided(last_exec: SeqNo, proofs: Vec<Proof<O>>) -> Self {
        Self {
            last_exec: Some(last_exec),
            decided: proofs,
        }
    }

    /// Returns the sequence number of the last executed batch of client
    /// requests, assigned by the conesensus layer.
    pub fn last_execution(&self) -> Option<SeqNo> {
        self.last_exec
    }

    /// Get all of the decided proofs in this decisionn log
    pub fn proofs(&self) -> &[Proof<O>] {
        &self.decided[..]
    }

    /// Append a proof to the end of the log. Assumes all prior checks have been done
    pub(crate) fn append_proof(&mut self, proof: Proof<O>) {
        self.last_exec = Some(proof.seq_no());

        self.decided.push(proof);
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

impl<O> OrderProtocolLog for DecisionLog<O> {}

impl<O> OrderProtocolProof for Proof<O> {}

impl<O> Clone for Proof<O> {
    fn clone(&self) -> Self {
        let mut new_pre_prepares = Vec::with_capacity(self.pre_prepares.len());

        let mut new_prepares = Vec::with_capacity(self.prepares.len());

        let mut new_commits = Vec::with_capacity(self.commits.len());

        for pre_prepare in &self.pre_prepares {
            new_pre_prepares.push(pre_prepare.clone());
        }

        for prepare in &self.prepares {
            new_prepares.push(prepare.clone());
        }

        for commit in &self.commits {
            new_commits.push(commit.clone());
        }

        Self {
            metadata: self.metadata.clone(),
            pre_prepares: new_pre_prepares,
            prepares: new_prepares,
            commits: new_commits,
        }
    }
}

impl<O> Debug for Proof<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Proof {{ Metadata: {:?}, pre_prepares: {}, prepares: {}, commits: {} }}", self.metadata, self.pre_prepares.len(), self.prepares.len(), self.commits.len())
    }
}

impl<O> Debug for CollectData<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CollectData {{ incomplete_proof: {:?}, last_proof: {:?} }}", self.incomplete_proof, self.last_proof)
    }
}