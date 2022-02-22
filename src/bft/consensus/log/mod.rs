//! A module to manage the `febft` message log.

use std::borrow::Borrow;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use dashmap::DashMap;
use intmap::IntMap;
use parking_lot::{Mutex, RwLock};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use crate::bft::benchmarks::BatchMeta;
use crate::bft::collections;
use crate::bft::collections::{ConcurrentHashMap, HashMap};
use crate::bft::communication::message::{
    ConsensusMessage,
    ConsensusMessageKind,
    Header,
    RequestMessage,
    StoredMessage,
    SystemMessage,
};
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;
use crate::bft::executable::UpdateBatch;
use crate::bft::ordering::{
    Orderable,
    SeqNo,
};

/// Checkpoint period.
///
/// Every `PERIOD` messages, the message log is cleared,
/// and a new log checkpoint is initiated.
pub const PERIOD: u32 = 120_000_000;

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log became full. We are waiting for the execution layer
    /// to provide the current serialized application state, so we can
    /// complete the log's garbage collection and eventually its
    /// checkpoint.
    BeginCheckpoint,
}

enum CheckpointState<S> {
    // no checkpoint has been performed yet
    None,
    // we are calling this a partial checkpoint because we are
    // waiting for the application state from the execution layer
    Partial {
        // sequence number of the last executed request
        seq: SeqNo,
    },
    PartialWithEarlier {
        // sequence number of the last executed request
        seq: SeqNo,
        // save the earlier checkpoint, in case corruption takes place
        earlier: Checkpoint<S>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Checkpoint<S>),
}

/// Represents a local checkpoint.
///
/// Contains the last application state, as well as the sequence number
/// which decided the last batch of requests executed before the checkpoint.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct Checkpoint<S> {
    seq: SeqNo,
    appstate: S,
}

impl<S> Orderable for Checkpoint<S> {
    /// Returns the sequence number of the batch of client requests
    /// decided before the local checkpoint.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<S> Checkpoint<S> {
    /// Returns a reference to the state of the application before
    /// the local checkpoint.
    pub fn state(&self) -> &S {
        &self.appstate
    }

    /// Returns the inner values within this local checkpoint.
    pub fn into_inner(self) -> (SeqNo, S) {
        (self.seq, self.appstate)
    }
}

/// Subset of a `Log`, containing only consensus messages.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct DecisionLog<O> {
    last_exec: Option<SeqNo>,
    pre_prepares: Vec<StoredMessage<ConsensusMessage<O>>>,
    prepares: Vec<StoredMessage<ConsensusMessage<O>>>,
    commits: Vec<StoredMessage<ConsensusMessage<O>>>,
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

    /// Returns the list of `PRE-PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn pre_prepares(&self) -> &[StoredMessage<ConsensusMessage<O>>] {
        &self.pre_prepares[..]
    }

    /// Returns the list of `PREPARE` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn prepares(&self) -> &[StoredMessage<ConsensusMessage<O>>] {
        &self.prepares[..]
    }

    /// Returns the list of `COMMIT` messages after the last checkpoint
    /// at the moment of the creation of this `DecisionLog`.
    pub fn commits(&self) -> &[StoredMessage<ConsensusMessage<O>>] {
        &self.commits[..]
    }

    // TODO: quorum sizes may differ when we implement reconfiguration
    pub fn collect_data(&self, view: ViewInfo) -> CollectData<O> {
        CollectData {
            incomplete_proof: self.to_be_decided(view),
            last_proof: self.last_decision(view),
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
    pub fn to_be_decided(&self, view: ViewInfo) -> IncompleteProof {
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
            let quorum = view.params().f() << 1;
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
                            break 'outer Some(ViewDecisionPair(
                                stored.message().view(),
                                digest,
                            ));
                        }
                    }
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            break 'outer None;
        };

        IncompleteProof { in_exec, write_set, quorum_writes }
    }

    /// Returns the proof of the last executed consensus
    /// instance registered in this `DecisionLog`.
    pub fn last_decision(&self, view: ViewInfo) -> Option<Proof<O>> {
        let last_exec = self.last_exec?;

        let pre_prepare = 'outer: loop {
            for stored in self.pre_prepares.iter().rev() {
                if stored.message().sequence_number() == last_exec {
                    break 'outer stored.clone();
                }
            }
            // if nothing went wrong, this code should be unreachable,
            // since we registered the last executed sequence number
            unreachable!()
        };
        let prepares = {
            // TODO: this code could be improved when `ControlFlow` is stabilized in
            // the Rust standard library
            let mut buf = Vec::new();
            let mut last_view = SeqNo::ZERO;
            let mut found = false;
            for stored in self.prepares.iter().rev() {
                if !found {
                    if stored.message().sequence_number() != last_exec {
                        // skip messages added to log after the last execution
                        continue;
                    } else {
                        found = true;
                        last_view = stored.message().view();
                        buf.push(stored.clone());
                        continue;
                    }
                }
                let will_exit = stored.message().sequence_number() != last_exec
                    || stored.message().view() != last_view;
                if will_exit {
                    break;
                }
                buf.push(stored.clone());
            }
            // quorum size minus one, because leader doesn't vote in the PREPARE
            // phase, since it already voted in the PRE-PREPARE phase;
            // = (N - F) - 1 = (2F + 1) - 1 = 2F
            let quorum = view.params().f() << 1;
            if buf.len() < quorum { None } else { Some(buf) }
        }?;
        let commits = {
            let mut buf = Vec::new();
            let mut last_view = SeqNo::ZERO;
            let mut found = false;
            for stored in self.commits.iter().rev() {
                if !found {
                    if stored.message().sequence_number() != last_exec {
                        continue;
                    } else {
                        found = true;
                        last_view = stored.message().view();
                        buf.push(stored.clone());
                        continue;
                    }
                }
                let will_exit = stored.message().sequence_number() != last_exec
                    || stored.message().view() != last_view;
                if will_exit {
                    break;
                }
                buf.push(stored.clone());
            }
            let quorum = view.params().quorum();
            if buf.len() < quorum { None } else { Some(buf) }
        }?;

        Some(Proof { pre_prepare, prepares, commits })
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

        fn clear_log<M>(in_exec: SeqNo, scratch: &mut Vec<usize>, log: &mut Vec<StoredMessage<M>>)
            where
                M: Orderable,
        {
            for (i, stored) in log.iter().enumerate().rev() {
                if stored.message().sequence_number() != in_exec {
                    break;
                }
                scratch.push(i);
            }
            for i in scratch.drain(..) {
                log.swap_remove(i);
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
                        pre_prepare = Some(self.pre_prepares.swap_remove(i));
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
}

/// Represents a log of messages received by the BFT system.
pub struct Log<S, O, P> {
    //This item will only be accessed by the replica request thread
    curr_seq: Cell<SeqNo>,
    batch_size: usize,
    //This will only be access by the
    declog: RefCell<DecisionLog<O>>,
    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    latest_op: RwLock<IntMap<SeqNo>>,
    ///TODO: Implement a concurrent IntMap and replace this one with it
    //This item will be accessed from both the client request thread and the
    //Replica request thread
    requests: ConcurrentHashMap<Digest, StoredMessage<RequestMessage<O>>>,
    //This will only be accessed from the replica request thread so we can wrap it
    //In a simple cell
    decided: Cell<Vec<O>>,
    checkpoint: Cell<CheckpointState<S>>,
    //Some stuff for statistics.
    meta: Mutex<BatchMeta>,
    _marker: PhantomData<P>,
}

//TODO: Might have to unsafe impl Sync

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<S, O: Clone, P> Log<S, O, P> {
    /// Creates a new message log.
    ///
    /// The value `batch_size` represents the maximum number of
    /// client requests to queue before executing a consensus instance.
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            curr_seq: Cell::new(SeqNo::ZERO),
            latest_op: RwLock::new(IntMap::new()),
            declog: RefCell::new(DecisionLog::new()),
            // TODO: use config value instead of const
            decided: Cell::new(Vec::with_capacity(PERIOD as usize)),
            requests: collections::concurrent_hash_map_with_capacity(PERIOD as usize),
            checkpoint: Cell::new(CheckpointState::None),
            meta: Mutex::new(BatchMeta::new()),
            _marker: PhantomData,
        }
    }

    pub fn batch_meta(&self) -> &Mutex<BatchMeta> {
        &self.meta
    }

    /// Returns a reference to a subset of this log, containing only
    /// consensus messages.
    pub fn decision_log(&self) -> &RefCell<DecisionLog<O>> {
        &self.declog
    }

    /// Update the log state, received from the CST protocol.
    pub fn install_state(&self, last_seq: SeqNo, rs: RecoveryState<S, O>) {
        // FIXME: what to do with `self.deciding`..?

        //Replace the log
        self.declog.replace(rs.declog);

        self.decided.replace(rs.requests);
        self.checkpoint.replace(CheckpointState::Complete(rs.checkpoint));
        self.curr_seq.replace(last_seq);
    }

    /// Take a snapshot of the log, used to recover a replica.
    ///
    /// This method may fail if we are waiting for the latest application
    /// state to be returned by the execution layer.
    //
    // TODO: return reference to the log state, so we don't have to clone()
    // it, which can be quite expensive
    //
    pub fn snapshot(&self, view: ViewInfo) -> Result<RecoveryState<S, O>>
        where
            S: Clone,
            O: Clone,
    {
        match self.checkpoint {
            CheckpointState::Complete(ref checkpoint) => {
                Ok(RecoveryState::new(
                    view,
                    (*checkpoint).clone(),
                    (*self.decided).clone(),
                    (*self.declog).clone(),
                ))
            }
            _ => Err("Checkpoint to be finalized").wrapped(ErrorKind::ConsensusLog),
        }
    }

    /*
        /// Replaces the current `Log` with an empty one, and returns
        /// the replaced instance.
        pub fn take(&mut self) -> Self {
            std::mem::replace(self, Log::new())
        }
    */

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&self, header: Header, message: SystemMessage<S, O, P>) {
        match message {
            SystemMessage::Request(message) => {
                let key = operation_key::<O>(&header, &message);

                let latest_op_guard = self.latest_op.read();

                let seq_no = latest_op_guard
                    .get(key)
                    .copied()
                    .unwrap_or(SeqNo::ZERO);

                drop(latest_op_guard);

                // avoid executing earlier requests twice
                if message.sequence_number() < seq_no {
                    return;
                }

                let digest = header.unique_digest();
                let stored = StoredMessage::new(header, message);

                self.requests.insert(digest, stored);
            }
            SystemMessage::Consensus(message) => {
                //These messages can only be sent by replicas, so the dec_log
                //Is only accessed by one thread.
                let stored = StoredMessage::new(header, message);

                let dec_log = self.declog.borrow_mut();

                match stored.message().kind() {
                    ConsensusMessageKind::PrePrepare(_) => dec_log.pre_prepares.push(stored),
                    ConsensusMessageKind::Prepare(_) => dec_log.prepares.push(stored),
                    ConsensusMessageKind::Commit(_) => dec_log.commits.push(stored),
                }
            }
            // rest are not handled by the log
            _ => (),
        }
    }

    /// Retrieves the next batch of requests available for proposing, if any.
    pub fn next_batch(&self) -> Option<Vec<StoredMessage<RequestMessage<O>>>> {
        // TODO:
        // - we may include another condition here to decide on a
        // smaller batch size, so that client request latency is lower
        // - prevent non leader replicas from collecting a batch of digests,
        // as only the leader will actually propose!
        if self.requests.len() > 0 {
            Some(self.requests
                .values()
                .cloned()
                .take(self.batch_size)
                .collect())
        } else {
            None
        }
    }

    /// Retrieves a batch of requests to be proposed during a view change.
    pub fn view_change_propose(&self) -> Vec<StoredMessage<RequestMessage<O>>> {
        self.requests
            .values()
            .take(self.batch_size)
            .cloned()
            .collect()
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    pub fn has_request(&self, digest: &Digest) -> bool {
        self.requests.contains_key(digest)
    }

    /// Clone the requests corresponding to the provided list of hash digests.
    pub fn clone_requests(&self, digests: &[Digest]) -> Vec<StoredMessage<RequestMessage<O>>>
        where
            O: Clone,
    {
        digests
            .iter()
            .flat_map(|d| self.requests.get(d))
            .cloned()
            .collect()
    }

    /// Finalize a batch of client requests decided on the consensus instance
    /// with sequence number `seq`, retrieving the payload associated with their
    /// given digests `digests`.
    ///
    /// The log may be cleared resulting from this operation. Check the enum variant of
    /// `Info`, to perform a local checkpoint when appropriate.
    pub fn finalize_batch(&self, seq: SeqNo, digests: &[Digest]) -> Result<(Info, UpdateBatch<O>)>
        where
            O: Clone,
    {
        let mut batch = UpdateBatch::new();

        for digest in digests {
            let (header, message) = self.requests
                .remove(digest)
                .map(StoredMessage::into_inner)
                .ok_or(Error::simple(ErrorKind::ConsensusLog))?;

            let key = operation_key::<O>(&header, &message);

            let mut latest_op_guard = self.latest_op.write();

            let seq_no = latest_op_guard
                .get(key)
                .copied()
                .unwrap_or(SeqNo::ZERO);

            if message.sequence_number() > seq_no {
                latest_op_guard.insert(key, message.sequence_number());
            }

            drop(latest_op_guard);

            batch.add(
                header.from(),
                message.session_id(),
                message.sequence_number(),
                message.into_inner_operation(),
            );
        }

        // TODO: optimize batch cloning, as this can take
        // several ms if the batch size is large, and each
        // request also large
        for update in batch.as_ref() {
            self.decided.push(update.operation().clone());
        }

        // retrive the sequence number stored within the PRE-PREPARE message
        // pertaining to the current request being executed
        let mut dec_log_guard = self.declog.lock();

        let last_seq_no = if dec_log_guard.pre_prepares.len() > 0 {
            let stored_pre_prepare =
                &dec_log_guard.pre_prepares[dec_log_guard.pre_prepares.len() - 1].message();
            stored_pre_prepare.sequence_number()
        } else {
            // the log was cleared concurrently, retrieve
            // the seq number stored before the log was cleared
            (*self.curr_seq).clone()
        };

        let last_seq_no_u32 = u32::from(last_seq_no);

        let info = if last_seq_no_u32 > 0 && last_seq_no_u32 % PERIOD == 0 {
            self.begin_checkpoint(last_seq_no)?
        } else {
            Info::Nil
        };

        // the last executed sequence number
        dec_log_guard.last_exec = Some(seq);

        Ok((info, batch))
    }

    fn begin_checkpoint(&self, seq: SeqNo) -> Result<Info> {
        let earlier = self.checkpoint.replace(CheckpointState::None);

        self.checkpoint.replace(match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => CheckpointState::PartialWithEarlier { seq, earlier },
            // FIXME: this may not be an invalid state after all; we may just be generating
            // checkpoints too fast for the execution layer to keep up, delivering the
            // hash digests of the appstate
            _ => return Err("Invalid checkpoint state detected").wrapped(ErrorKind::ConsensusLog),
        });
        Ok(Info::BeginCheckpoint)
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&self, appstate: S) -> Result<()> {
        match *self.checkpoint {
            CheckpointState::None => Err("No checkpoint has been initiated yet").wrapped(ErrorKind::ConsensusLog),
            CheckpointState::Complete(_) => Err("Checkpoint already finalized").wrapped(ErrorKind::ConsensusLog),
            CheckpointState::Partial { ref seq } | CheckpointState::PartialWithEarlier { ref seq, .. } => {
                let seq = *seq;

                self.checkpoint.replace(CheckpointState::Complete(Checkpoint {
                    seq,
                    appstate,
                }));

                self.decided.clear();
                //
                // NOTE: workaround bug where when we clear the log,
                // we remove the PRE-PREPARE of an on-going request
                //
                // FIXME: the log should not be cleared until a request is over
                //
                let guard = self.declog.lock();

                match self.declog.pre_prepares.pop() {
                    Some(last_pre_prepare) => {
                        self.declog.pre_prepares.clear();

                        // store the id of the last received pre-prepare,
                        // which corresponds to the request currently being
                        // processed
                        self.curr_seq.replace(last_pre_prepare.message().sequence_number());
                    }
                    None => {
                        // no stored PRE-PREPARE messages, NOOP
                    }
                }
                self.declog.prepares.clear();
                self.declog.commits.clear();
                Ok(())
            }
        }
    }
}

#[inline]
fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = header.from().into();
    let session_id: u64 = message.session_id().into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
