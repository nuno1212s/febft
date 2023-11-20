use std::fmt::{Debug, Formatter};
use std::iter;
use std::ops::Deref;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::ordering_protocol::networking::serialize::OrderProtocolProof;
use atlas_core::smr::smr_decision_log::ShareableMessage;

use crate::bft::message::{ConsensusMessageKind, PBFTMessage};

pub type StoredConsensusMessage<O> = ShareableMessage<PBFTMessage<O>>;

#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Decision<O> {
    seq_no: SeqNo,
    pre_prepare_ordering: Vec<Digest>,
    pre_prepares: Vec<StoredConsensusMessage<O>>,
    prepares: Vec<StoredConsensusMessage<O>>,
    commits: Vec<StoredConsensusMessage<O>>,
}

/// Metadata about a proof
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug)]
pub struct ProofMetadata {
    seq_no: SeqNo,
    batch_digest: Digest,
    pre_prepare_ordering: Vec<Digest>,
    contained_client_rqs: usize,
}

impl Orderable for ProofMetadata {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
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
    pub(crate) fn new(seq_no: SeqNo, digest: Digest, pre_prepare_ordering: Vec<Digest>, contained_rqs: usize) -> Self {
        Self {
            seq_no,
            batch_digest: digest,
            pre_prepare_ordering,
            contained_client_rqs: contained_rqs,
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

    pub fn contained_client_rqs(&self) -> usize {
        self.contained_client_rqs
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

    pub fn init_from_messages(metadata: ProofMetadata, messages: Vec<StoredConsensusMessage<O>>) -> Result<Self> {
        let mut pre_prepares: Vec<Option<StoredConsensusMessage<O>>> = iter::repeat(None).take(metadata.pre_prepare_ordering().len()).collect();
        let mut prepares = Vec::new();
        let mut commits = Vec::new();

        for x in messages {
            match x.message().consensus().kind() {
                ConsensusMessageKind::PrePrepare(_) => {
                    let option = metadata.pre_prepare_ordering().iter().position(|digest| *x.header().digest() == *digest);

                    let index = option.ok_or(Error::simple_with_msg(ErrorKind::OrderProtocolProof, "Failed to create proof as pre prepare is not contained in metadata ordering"))?;

                    pre_prepares[index] = Some(x);
                }
                ConsensusMessageKind::Prepare(_) => {
                    prepares.push(x);
                }
                ConsensusMessageKind::Commit(_) => {
                    commits.push(x);
                }
            }
        }

        let mut pre_prepares_f = Vec::with_capacity(metadata.pre_prepare_ordering().len());

        for message in pre_prepares.into_iter() {
            pre_prepares_f.push(message.ok_or(Error::simple_with_msg(ErrorKind::OrderProtocolProof, "Failed to create proof as pre prepare list is not complete"))?);
        }

        Ok(Self {
            metadata,
            pre_prepares: pre_prepares_f,
            prepares,
            commits,
        })
    }

    pub(crate) fn metadata(&self) -> &ProofMetadata {
        &self.metadata
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

    pub fn into_parts(self) -> (ProofMetadata, Vec<ShareableMessage<PBFTMessage<O>>>) {
        let mut vec = Vec::with_capacity(self.pre_prepares.len() + self.prepares.len() + self.commits.len());

        for pre_prepares in self.pre_prepares {
            vec.push(pre_prepares);
        }

        for prepare in self.prepares {
            vec.push(prepare);
        }

        for commit in self.commits {
            vec.push(commit);
        }

        (self.metadata, vec)
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

impl<O> OrderProtocolProof for Proof<O> {
    fn contained_messages(&self) -> usize {
        self.metadata().contained_client_rqs()
    }
}

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