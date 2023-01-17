use std::cmp::Ordering;
use std::sync::Arc;
use crate::bft::communication::message::{ConsensusMessage, ConsensusMessageKind, StoredMessage};
use crate::bft::crypto::hash::Digest;
use crate::bft::globals::ReadOnly;
use crate::bft::ordering::{Orderable, SeqNo};

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

/// Subset of a `Log`, containing only consensus messages.
///
/// Cloning this decision log is actually pretty cheap (compared to the alternative of cloning
/// all requests executed since the last checkpoint) since it only has to clone the arcs (which is one atomic operation)
/// We can't wrap the entire vector since the decision log is actually constantly modified by the consensus
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct DecisionLog<O> {
    last_exec: Option<SeqNo>,
    pre_prepares: Vec<Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>>,
    prepares: Vec<Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>>,
    commits: Vec<Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>>,
}

/// Represents a single decision from the `DecisionLog`.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Proof<O> {
    pre_prepare: StoredMessage<ConsensusMessage<O>>,
    prepares: Vec<StoredMessage<ConsensusMessage<O>>>,
    commits: Vec<StoredMessage<ConsensusMessage<O>>>,
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
    quorum_writes: Option<ViewDecisionPair>,
}

impl WriteSet {
    /// Iterate over this `WriteSet`.
    ///
    /// Convenience method for calling `iter()` on the inner `Vec`.
    pub fn iter(&self) -> impl Iterator<Item=&ViewDecisionPair> {
        self.0.iter()
    }
}

impl<O> Proof<O> {
    /// Returns the `PRE-PREPARE` message of this `Proof`.
    pub fn pre_prepare(&self) -> &StoredMessage<ConsensusMessage<O>> {
        &self.pre_prepare
    }

    /// Returns the `PREPARE` message of this `Proof`.
    pub fn prepares(&self) -> &[StoredMessage<ConsensusMessage<O>>] {
        &self.prepares[..]
    }

    /// Returns the `COMMIT` message of this `Proof`.
    pub fn commits(&self) -> &[StoredMessage<ConsensusMessage<O>>] {
        &self.commits[..]
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
        self.quorum_writes.as_ref()
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

impl<O: Clone> DecisionLog<O> {
    /// Returns a brand new `DecisionLog`.
    pub fn new() -> Self {
        Self {
            // TODO: when recovering a replica from persistent
            // storage, set this value to `Some(...)`
            last_exec: None,
            pre_prepares: Vec::new(),
            prepares: Vec::new(),
            commits: Vec::new(),
        }
    }

    /// Returns the sequence number of the last executed batch of client
    /// requests, assigned by the conesensus layer.
    pub fn last_execution(&self) -> Option<SeqNo> {
        self.last_exec
    }

    //TODO: Maybe make these data structures a BTreeSet so that the messages are always ordered
    //By their seq no? That way we cannot go wrong in the ordering of messages.
    pub(crate) fn finished_quorum_execution(&mut self, seq_no: SeqNo) {
        self.last_exec.replace(seq_no);
    }

    pub(crate) fn append_pre_prepare(&mut self, pre_prepare: Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>) {
        self.pre_prepares.push(pre_prepare)
    }

    pub(crate) fn append_prepare(&mut self, prepare: Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>) {
        self.prepares.push(prepare);
    }

    pub(crate) fn append_commit(&mut self, commit: Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>) {
        self.commits.push(commit);
    }

    /// Returns the list of `PRE-PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn pre_prepares(&self) -> &[Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>] {
        &self.pre_prepares[..]
    }

    /// Returns the list of `PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn prepares(&self) -> &[Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>] {
        &self.prepares[..]
    }

    /// Returns the list of `COMMIT` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn commits(&self) -> &[Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>] {
        &self.commits[..]
    }

    /// Collects the most up to date data we have in store.
    /// Accepts the f for the view that it is looking for
    /// It must accept this f as the reconfiguration of the network
    /// can alter the f from one seq no to the next
    pub fn collect_data(&self, f: usize) -> CollectData<O> {
        CollectData {
            incomplete_proof: self.to_be_decided(f),
            last_proof: self.last_decision(f),
        }
    }

    /// Returns the sequence number of the consensus instance
    /// currently being executed
    pub fn executing(&self) -> SeqNo {
        // we haven't called `finalize_batch` yet, so the in execution
        // seq no will be the last + 1 or 0
        self.last_exec
            .map(|last| SeqNo::from(u32::from(last) + 1))
            .unwrap_or(SeqNo::ZERO)
    }

    /// Returns an incomplete proof of the consensus
    /// instance currently being decided in this `DecisionLog`.
    /// Accepts the f of the system at the time of the request
    pub fn to_be_decided(&self, f: usize) -> IncompleteProof {
        let in_exec = self.executing();

        // fetch write set
        let write_set = WriteSet({
            let mut buf = Vec::new();
            for stored in self.pre_prepares.iter().rev() {
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

        // fetch quorum writes
        let quorum_writes = 'outer: loop {
            // NOTE: check `last_decision` comment on quorum
            let quorum = f << 1;
            let mut last_view = None;
            let mut count = 0;

            for stored in self.prepares.iter().rev() {
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
            quorum_writes,
        }
    }

    /// Returns the proof of the last executed consensus
    /// instance registered in this `DecisionLog`.
    pub fn last_decision(&self, f: usize) -> Option<Proof<O>> {
        //Get the last executed decision in this log.
        //This only gets updated when a batch is finished being decided
        let last_exec = self.last_exec?;

        let pre_prepare = 'outer: loop {
            for stored in self.pre_prepares.iter().rev() {
                if stored.message().sequence_number() == last_exec {
                    break 'outer (**stored).clone();
                }
            }
            // if nothing went wrong, this code should be unreachable,
            // since we registered the last executed sequence number
            unreachable!()
        };

        let quorum = 2 * f + 1;

        let view = pre_prepare.message().view();

        let prepares = Self::get_quorum_from_messages(
            &self.prepares,
            last_exec,
            view,
            //The pre prepare already serves as a prepare sent from the leader,
            //Since he obviously agrees with his own ordering
            quorum - 1,
        )?;

        let commits = Self::get_quorum_from_messages(
            &self.commits,
            last_exec,
            view,
            quorum,
        )?;

        Some(Proof {
            pre_prepare,
            prepares,
            commits,
        })
    }

    /// Collect quorum messages from a given list of messages.
    /// Only collects messages
    fn get_quorum_from_messages(messages: &Vec<Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>>,
                                seq_no: SeqNo,
                                view_no: SeqNo,
                                quorum: usize) ->
                                Option<Vec<StoredMessage<ConsensusMessage<O>>>> {
        let mut collected_messages = Vec::with_capacity(quorum);

        //Start at the last added message
        for message in messages.iter().rev() {
            if !found {
                if message.message().sequence_number() != seq_no ||
                    message.message().view() != view_no {
                    continue;
                } else {
                    found = true;

                    buf.push((**message).clone());
                    continue;
                }
            }

            if message.message().sequence_number() != seq_no
                || message.message().view() != view_no {
                break;
            } else {
                //Since the ordering is maintained,
                //We will keep adding to the message vec until we
                //Reach the next seq no
                buf.push((**message).clone());
            }
        }

        if collected_messages.len() < quorum {
            //Only return if we have successfully obtained a quorum
            None
        } else {
            Some(buf)
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
        let mut scratch = Vec::with_capacity(8);

        fn clear_log<M>(
            in_exec: SeqNo,
            scratch: &mut Vec<usize>,
            log: &mut Vec<Arc<ReadOnly<StoredMessage<M>>>>,
        ) where
            M: Orderable,
        {
            for (i, stored) in log.iter().enumerate().rev() {
                if stored.message().sequence_number() != in_exec {
                    break;
                }
                scratch.push(i);
            }
            for i in scratch.drain(..) {
                log.remove(i);
            }
        }

        let pre_prepare = {
            let mut pre_prepare_i = None;
            let mut pre_prepare = None;

            // find which indices to remove, and try to locate PRE-PREPARE
            for (i, stored) in self.pre_prepares.iter().enumerate().rev() {
                if stored.message().sequence_number() != in_exec {
                    break;
                }
                scratch.push(i);
                if let Some(v) = value {
                    if pre_prepare_i.is_none() && stored.header().digest() == v {
                        pre_prepare_i = Some(i);
                    }
                }
            }

            // remove indices from scratch space, and retrieve
            // PRE-PREPARE if available
            for i in scratch.drain(..) {
                match pre_prepare_i {
                    Some(j) if i == j => {
                        pre_prepare = Some((**self.pre_prepares.swap_remove(i)).clone());
                        pre_prepare_i = None;
                    }
                    _ => {
                        self.pre_prepares.swap_remove(i);
                    }
                }
            }

            pre_prepare
        };

        clear_log(in_exec, &mut scratch, &mut self.prepares);
        clear_log(in_exec, &mut scratch, &mut self.commits);

        pre_prepare
    }

    pub(crate) fn clear_until_seq(&mut self, seq_no: SeqNo) -> usize{
        let mut new_pre_prepares = Vec::new();
        let mut new_prepares = Vec::new();
        let mut new_commits = Vec::new();
        let mut decided_request_count = 0;

        for pre_prepare in self.pre_prepares.iter() {

            if pre_prepare.message().sequence_number() <= seq_no {

                //Mark the requests contained in this message for removal
                decided_request_count += match ele.message().kind() {
                    ConsensusMessageKind::PrePrepare(messages) => messages.len(),
                    _ => 0,
                };

                continue
            }

            new_pre_prepares.push(pre_prepare.clone());
        }

        for ele in self.prepares().iter().rev() {
            if ele.message().sequence_number() <= seq_no {
                // Since the messages are inserted in ascending order,
                // When we reach the first message that is <= to seq_no, then
                // we know that all other messages are also going to be <= seq_no
                break
            }

            new_prepares.push(ele.clone());
        }

        for ele in self.commits().iter().rev() {
            if ele.message().sequence_number() <= seq_no {
                // Since the messages are inserted in ascending order,
                // When we reach the first message that is <= to seq_no, then
                // we know that all other messages are also going to be <= seq_no
                break
            }

            new_commits.inser(ele.clone());
        }

        new_pre_prepares.reverse();
        new_prepares.reverse();
        new_commits.reverse();

        self.pre_prepares = new_pre_prepares;
        self.prepares = new_prepares;
        self.commits = new_commits;

        decided_request_count
    }
}
