use std::cmp::Ordering;
use std::ops::Deref;
use std::sync::Arc;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, StoredMessage};
use crate::bft::crypto::hash::Digest;
use crate::bft::globals::ReadOnly;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::error::*;

/// Represents a local checkpoint.
///
/// Contains the last application state, as well as the sequence number
/// which decided the last batch of requests executed before the checkpoint.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Checkpoint<S> {
    seq: SeqNo,
    app_state: S,
}

impl<S> Orderable for Checkpoint<S> {
    /// Returns the sequence number of the batch of client requests
    /// decided before the local checkpoint.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S> Checkpoint<S> {
    pub(crate) fn new(seq: SeqNo, app_state: S) -> Arc<ReadOnly<Self>> {
        Arc::new(ReadOnly::new(Self {
            seq,
            app_state,
        }))
    }

    /// The last sequence no represented in this checkpoint
    pub fn last_seq(&self) -> &SeqNo {
        &self.seq
    }

    /// Returns a reference to the state of the application before
    /// the local checkpoint.
    pub fn state(&self) -> &S {
        &self.app_state
    }

    /// Returns the inner values within this local checkpoint.
    pub fn into_inner(self) -> (SeqNo, S) {
        (self.seq, self.app_state)
    }
}

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
    currently_deciding: OnGoingDecision<O>,
    decided: Vec<Proof<O>>,
}

/// Represents the decision that is currently ongoing in the consensus instance
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct OnGoingDecision<O> {
    seq_no: SeqNo,
    batch_digest: Option<Digest>,
    pre_prepare_ordering: Option<Vec<Digest>>,
    pre_prepares: Vec<StoredConsensusMessage<O>>,
    prepares: Vec<StoredConsensusMessage<O>>,
    commits: Vec<StoredConsensusMessage<O>>,
}

/// Metadata about a proof
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
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
#[derive(Clone)]
pub struct WriteSet(pub Vec<ViewDecisionPair>);

/// Contains a sequence number pertaining to a particular view,
/// as well as a hash digest of a value decided in a consensus
/// instance of that view.
///
/// Corresponds to the `TimestampValuePair` class in `BFT-SMaRt`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ViewDecisionPair(pub SeqNo, pub Digest);

/// Represents an incomplete decision from the `DecisionLog`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct IncompleteProof {
    in_exec: SeqNo,
    write_set: WriteSet,
    quorum_prepares: Option<ViewDecisionPair>,
}

impl WriteSet {
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
    /// Returns the sequence number of the consensus instance currently
    /// being executed.
    pub fn executing(&self) -> SeqNo {
        self.in_exec
    }

    /// Returns a reference to the `WriteSet` included in this `IncompleteProof`.
    pub fn write_set(&self) -> &WriteSet {
        &self.write_set
    }

    /// Returns a reference to the quorum writes included in this `IncompleteProof`,
    /// if any value was prepared in the previous view.
    pub fn quorum_writes(&self) -> Option<&ViewDecisionPair> {
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
    incomplete_proof: IncompleteProof,
    last_proof: Option<Proof<O>>,
}

impl<O> CollectData<O> {
    pub fn incomplete_proof(&self) -> &IncompleteProof {
        &self.incomplete_proof
    }

    pub fn last_proof(&self) -> Option<&Proof<O>> {
        self.last_proof.as_ref()
    }
}

impl<O> OnGoingDecision<O> {
    pub(crate) fn init_blank(seq_no: SeqNo) -> Self {
        Self {
            seq_no,
            batch_digest: None,
            pre_prepare_ordering: None,
            pre_prepares: vec![],
            prepares: vec![],
            commits: vec![],
        }
    }

    pub(crate) fn init_from_info(seq_no: SeqNo, batch_digest: Digest, pre_prepare_ordering: Vec<Digest>) -> Self {
        Self {
            seq_no,
            batch_digest: Some(batch_digest),
            pre_prepare_ordering: Some(pre_prepare_ordering),
            pre_prepares: vec![],
            prepares: vec![],
            commits: vec![],
        }
    }

    pub(crate) fn proof(mut self, quorum: Option<usize>) -> Result<Proof<O>> {

        //TODO: Order the pre prepares to match the ordering of the vector

        let (leader_count, order) = {
            if let Some(ordering) = self.pre_prepare_ordering {
                (ordering.len(), ordering)
            } else {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                  "Failed to create proof, no ordering available"));
            }
        };

        if self.pre_prepares.len() != leader_count {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                              "Failed to create a proof, pre_prepares do not match up to the leader count"));
        }

        let mut ordered_pre_prepares = Vec::with_capacity(leader_count);

        for digest in &order {
            for i in 0..self.pre_prepares.len() {
                let message = self.pre_prepares.get(i).unwrap();

                if *message.header().digest() == *digest {
                    ordered_pre_prepares.push(self.pre_prepares.swap_remove(i));

                    break;
                }
            }
        }

        if let Some(quorum) = quorum {
            if self.prepares.len() < quorum {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                  "Failed to create a proof, prepares do not match up to the 2*f+1"));
            }

            if self.commits.len() < quorum {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                  "Failed to create a proof, commits do not match up to the 2*f+1"));
            }
        }

        Ok(Proof {
            metadata: ProofMetadata::new(self.seq_no,
                                         self.batch_digest.unwrap(),
                                         order),
            pre_prepares: ordered_pre_prepares,
            prepares: self.prepares,
            commits: self.commits,
        })
    }

    pub(crate) fn append_pre_prepare(&mut self, pre_prepare: StoredConsensusMessage<O>) {
        self.pre_prepares.push(pre_prepare);
    }

    pub(crate) fn append_prepare(&mut self, prepare: StoredConsensusMessage<O>) {
        self.prepares.push(prepare);
    }

    pub(crate) fn append_commit(&mut self, commit: StoredConsensusMessage<O>) {
        self.commits.push(commit);
    }

    /// Register that all the batches have been received
    pub(crate) fn all_batches_received(&mut self, digest: Digest, pre_prepare_ordering: Vec<Digest>) -> ProofMetadata {
        self.batch_digest = Some(digest);
        self.pre_prepare_ordering = Some(pre_prepare_ordering);

        ProofMetadata::new(self.seq_no,
                           self.batch_digest.unwrap().clone(),
                           self.pre_prepare_ordering.as_ref().unwrap().clone())
    }

    pub fn seq_no(&self) -> SeqNo {
        self.seq_no
    }

    pub fn batch_digest(&self) -> Option<Digest> {
        self.batch_digest
    }
    pub fn pre_prepare_ordering(&self) -> &Option<Vec<Digest>> {
        &self.pre_prepare_ordering
    }
    pub fn pre_prepares(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.pre_prepares
    }
    pub fn prepares(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.prepares
    }
    pub fn commits(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.commits
    }
}

impl<O> DecisionLog<O> {
    pub fn new() -> Self {
        Self {
            last_exec: None,
            currently_deciding: OnGoingDecision::init_blank(SeqNo::ZERO),
            decided: vec![],
        }
    }

    pub fn from_decided(last_exec: SeqNo, proofs: Vec<Proof<O>>) -> Self {
        let current_exec = last_exec.next();

        Self {
            last_exec: Some(last_exec),
            currently_deciding: OnGoingDecision::init_blank(current_exec),
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

    pub(crate) fn append_pre_prepare(&mut self, pre_prepare: StoredConsensusMessage<O>) {
        self.currently_deciding.append_pre_prepare(pre_prepare);
    }

    pub(crate) fn append_prepare(&mut self, prepare: StoredConsensusMessage<O>) {
        self.currently_deciding.append_prepare(prepare);
    }

    pub(crate) fn append_commit(&mut self, commit: StoredConsensusMessage<O>) {
        self.currently_deciding.append_commit(commit);
    }

    /// Append a proof to the end of the log. Assumes all prior checks have been done
    pub(crate) fn append_proof(&mut self, proof: Proof<O>) {
        self.last_exec = Some(proof.seq_no());

        self.decided.push(proof);
    }

    /// Register that all the batches have been received
    pub(crate) fn all_batches_received(&mut self, digest: Digest, pre_prepare_ordering: Vec<Digest>) -> ProofMetadata {
        self.currently_deciding.all_batches_received(digest, pre_prepare_ordering)
    }

    //TODO: Maybe make these data structures a BTreeSet so that the messages are always ordered
    //By their seq no? That way we cannot go wrong in the ordering of messages.
    pub(crate) fn finished_quorum_execution(&mut self, seq_no: SeqNo, f: usize) -> Result<()> {
        self.last_exec.replace(seq_no);

        let decided = std::mem::replace(&mut self.currently_deciding, OnGoingDecision::init_blank(seq_no.next()));
        let proof = decided.proof(Some(f))?;

        self.decided.push(proof);

        Ok(())
    }

    /// Collects the most up to date data we have in store.
    /// Accepts the f for the view that it is looking for
    /// It must accept this f as the reconfiguration of the network
    /// can alter the f from one seq no to the next
    pub fn collect_data(&self, f: usize) -> CollectData<O> {
        CollectData {
            incomplete_proof: self.to_be_decided(f),
            last_proof: self.last_decision(),
        }
    }
    /// Returns the sequence number of the consensus instance
    /// currently being executed
    pub fn executing(&self) -> SeqNo {
        // we haven't called `finalize_batch` yet, so the in execution
        // seq no will be the last + 1 or 0
        self.currently_deciding.seq_no
    }

    /// Get a reference to the current on going decision
    pub fn current_execution(&self) -> &OnGoingDecision<O> {
        &self.currently_deciding
    }

    /// Returns the proof of the last executed consensus
    /// instance registered in this `DecisionLog`.
    pub fn last_decision(&self) -> Option<Proof<O>> {
        self.decided.get(self.decided.len() - 1).map(|p| (*p).clone())
    }

    /// Returns an incomplete proof of the consensus
    /// instance currently being decided in this `DecisionLog`.
    /// Accepts the f of the system at the time of the request
    pub fn to_be_decided(&self, f: usize) -> IncompleteProof {
        let in_exec = self.executing();

        // fetch write set
        let write_set = WriteSet({
            let mut buf = Vec::new();

            for stored in self.currently_deciding.prepares.iter().rev() {
                match stored.message().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
                        buf.push(ViewDecisionPair(
                            stored.message().view(),
                            stored.header().digest().clone(),
                        ));
                    }
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            buf
        });

        // fetch quorum prepares
        let quorum_writes = 'outer: loop {
            // NOTE: check `last_decision` comment on quorum
            let quorum = f << 1;
            let mut last_view = None;
            let mut count = 0;

            for stored in self.currently_deciding.prepares.iter().rev() {
                match stored.message().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
                        match last_view {
                            None => (),
                            Some(v) if stored.message().view() == v => (),
                            _ => count = 0,
                        }
                        last_view = Some(stored.message().view());
                        count += 1;
                        if count == quorum {
                            let digest = match stored.message().kind() {
                                ConsensusMessageKind::Prepare(d) => d.clone(),
                                _ => unreachable!(),
                            };
                            break 'outer Some(ViewDecisionPair(stored.message().view(), digest));
                        }
                    }
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            break 'outer None;
        };

        IncompleteProof {
            in_exec,
            write_set,
            quorum_prepares: quorum_writes,
        }
    }


    /// Clear incomplete proofs from the log, which match the consensus
    /// with sequence number `in_exec`.
    ///
    /// If `value` is `Some(v)`, then a `PRE-PREPARE` message will be
    /// returned matching the digest `v`.
    pub fn clear_last_occurrences(
        &mut self,
        in_exec: SeqNo,
        value: Option<&Digest>,
    ) -> Option<StoredMessage<ConsensusMessage<O>>> {
        // let mut scratch = Vec::with_capacity(8);

        if self.currently_deciding.seq_no == in_exec {
            let ongoing_decision = std::mem::replace(&mut self.currently_deciding, OnGoingDecision::init_blank(in_exec));

            todo!()
        } else {
            unreachable!()
        }
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