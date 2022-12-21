//! A module to manage the `febft` message log.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;

use std::path::Path;
use std::sync::Arc;

use intmap::IntMap;
use log::{debug, error};
use parking_lot::Mutex;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use crate::bft::benchmarks::BatchMeta;
use crate::bft::collections;
use crate::bft::collections::ConcurrentHashMap;
use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, ObserveEventKind, RequestMessage,
    StoredMessage, SystemMessage,
};
use crate::bft::communication::NodeId;

use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;
use crate::bft::executable::{ExecutorHandle, Reply, Request, Service, State, UpdateBatch};
use crate::bft::globals::ReadOnly;
use crate::bft::ordering::{Orderable, SeqNo};

use self::persistent::{PersistentLog, WriteMode};
use self::persistent::{PersistentLogModeTrait};

pub mod persistent;

/// Checkpoint period.
///
/// Every `PERIOD` messages, the message log is cleared,
/// and a new log checkpoint is initiated.
/// TODO: Move this to an env variable as it can be highly dependent on the service implemented on top of it
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
        earlier: Arc<ReadOnly<Checkpoint<S>>>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Arc<ReadOnly<Checkpoint<S>>>),
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

    // TODO: quorum sizes may differ when we implement reconfiguration
    pub fn collect_data(&self, view: ViewInfo) -> CollectData<O> {
        CollectData {
            incomplete_proof: self.to_be_decided(view.clone()),
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
    pub fn last_decision(&self, view: ViewInfo) -> Option<Proof<O>> {
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
                        buf.push((**stored).clone());
                        continue;
                    }
                }
                let will_exit = stored.message().sequence_number() != last_exec
                    || stored.message().view() != last_view;
                if will_exit {
                    break;
                }
                buf.push((**stored).clone());
            }
            // quorum size minus one, because leader doesn't vote in the PREPARE
            // phase, since it already voted in the PRE-PREPARE phase;
            // = (N - F) - 1 = (2F + 1) - 1 = 2F
            let quorum = view.params().f() << 1;
            if buf.len() < quorum {
                None
            } else {
                Some(buf)
            }
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
                        buf.push((**stored).clone());
                        continue;
                    }
                }
                let will_exit = stored.message().sequence_number() != last_exec
                    || stored.message().view() != last_view;
                if will_exit {
                    break;
                }
                buf.push((**stored).clone());
            }
            let quorum = view.params().quorum();
            if buf.len() < quorum {
                None
            } else {
                Some(buf)
            }
        }?;

        Some(Proof {
            pre_prepare,
            prepares,
            commits,
        })
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
}

/// Represents a log of messages received by the BFT system.
pub struct Log<S: Service, T: PersistentLogModeTrait> {
    node_id: NodeId,

    //This item will only be accessed by the replica request thread
    //The current stored SeqNo in the checkpoint state.
    //THIS IS NOT THE CURR_SEQ NUMBER IN THE CONSENSUS
    curr_seq: Cell<SeqNo>,
    batch_size: usize,
    //This will only be accessed by the replica processing thread since requests will only be
    //Decided by the consensus protocol, which operates completly in the replica thread
    declog: RefCell<DecisionLog<Request<S>>>,
    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    latest_op: Mutex<IntMap<SeqNo>>,

    ///TODO: Implement a concurrent IntMap and replace this one with it
    //This item will be accessed from both the client request thread and the
    //Replica request thread
    //This stores client requests that have not yet been put into a batch
    requests: ConcurrentHashMap<Digest, StoredMessage<RequestMessage<Request<S>>>>,

    //Stores just request batches. Much faster than individually storing all the requests and
    //Then having to always access them one by one
    request_batches:
    ConcurrentHashMap<Digest, Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>>,
    //This will only be accessed from the replica request thread so we can wrap it
    //In a simple cell

    //Stores all of the performed requests concatenated in a vec
    decided: RefCell<Vec<Request<S>>>,
    checkpoint: RefCell<CheckpointState<State<S>>>,
    //Some stuff for statistics.
    meta: Arc<Mutex<BatchMeta>>,

    persistent_log: PersistentLog<S, T>,

    //Observer
    observer: Option<ObserverHandle>,
}

///Justification/Sketch of proof:
/// The current sequence number, decision log, decided and checkpoint
/// Will only be accessed by the replica request thread, since they require communication
/// from the other threads and actual consensus to be reached.
/// The requests, latest_op and batch meta can be accessed and altered by both the replica request thread
/// and the client request thread, so we protected only those fields
unsafe impl<S: Service, T: PersistentLogModeTrait> Sync for Log<S, T> {}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<S, T> Log<S, T>
    where
        S: Service + 'static,
        T: PersistentLogModeTrait,
{
    /// Creates a new message log.
    ///
    /// The value `batch_size` represents the maximum number of
    /// client requests to queue before executing a consensus instance.
    pub fn new<K>(
        node: NodeId,
        batch_size: usize,
        observer: Option<ObserverHandle>,
        executor: ExecutorHandle<S>,
        db_path: K,
    ) -> Arc<Self>
        where
            K: AsRef<Path>,
    {
        let persistent_log =
            PersistentLog::init_log(executor, db_path).expect("Failed to init persistent log");

        Arc::new(Self {
            node_id: node,
            batch_size,
            curr_seq: Cell::new(SeqNo::ZERO),
            latest_op: Mutex::new(IntMap::new()),
            declog: RefCell::new(DecisionLog::new()),
            // TODO: use config value instead of const
            decided: RefCell::new(Vec::with_capacity(PERIOD as usize)),
            requests: collections::concurrent_hash_map_with_capacity(PERIOD as usize),
            request_batches: collections::concurrent_hash_map(),
            checkpoint: RefCell::new(CheckpointState::None),
            meta: Arc::new(Mutex::new(BatchMeta::new())),
            persistent_log,
            observer,
        })
    }

    pub fn latest_op(&self) -> &Mutex<IntMap<SeqNo>> {
        &self.latest_op
    }

    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.meta
    }

    /// Returns a reference to a subset of this log, containing only
    /// consensus messages.
    pub fn decision_log(&self) -> &RefCell<DecisionLog<Request<S>>> {
        &self.declog
    }

    /// Update the log state, received from the CST protocol.
    pub fn install_state(&self, last_seq: SeqNo, rs: RecoveryState<State<S>, Request<S>>) {
        // FIXME: what to do with `self.deciding`..?

        //Replace the log
        self.declog.replace(rs.declog);

        self.decided.replace(rs.requests);
        self.checkpoint
            .replace(CheckpointState::Complete(rs.checkpoint));
        self.curr_seq.replace(last_seq);
    }

    /// Read the current state, if existent, from the persistent storage
    ///
    /// FIXME: The view initialization might have to be changed if we want to introduce reconfiguration
    pub fn read_current_state(&self, n: usize, f: usize) -> Result<Option<RecoveryState<State<S>, Request<S>>>> {
        let option = self.persistent_log.read_state()?;

        if let Some(state) = option {
            let view_seq = ViewInfo::new(state.0, n, f)?;

            let mut requests = Vec::new();

            for request in state.2.pre_prepares() {
                let pre_prepare_rqs = match request.message().kind() {
                    ConsensusMessageKind::PrePrepare(requests) => {
                        requests.clone()
                    }
                    ConsensusMessageKind::Prepare(_) => { unreachable!() }
                    ConsensusMessageKind::Commit(_) => { unreachable!()}
                };

                for request in pre_prepare_rqs {
                    requests.push(request.into_inner().1.into_inner_operation());
                }
            }

            Ok(Some(RecoveryState {
                view: view_seq,
                checkpoint: state.1,
                requests,
                declog: state.2,
            }))
        } else {
            Ok(None)
        }
    }

    /// Take a snapshot of the log, used to recover a replica.
    ///
    /// This method may fail if we are waiting for the latest application
    /// state to be returned by the execution layer.
    ///
    pub fn snapshot(&self, view: ViewInfo) -> Result<RecoveryState<State<S>, Request<S>>> {
        match &*self.checkpoint.borrow() {
            CheckpointState::Complete(checkpoint) => Ok(RecoveryState::new(
                view,
                checkpoint.clone(),
                self.decided.borrow().clone(),
                self.declog.borrow().clone(),
            )),
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

    ///Insert a consensus message into the log.
    /// We can use this method when we want to prevent a clone, as this takes
    /// just a reference.
    /// This is mostly used for pre prepares as they contain all the requests and are therefore very expensive to send
    pub fn insert_consensus(
        &self,
        consensus_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        //These messages can only be sent by replicas, so the dec_log
        //Is only accessed by one thread.

        //Wrap the message in a read only reference so we can then pass it around without having to clone it everywhere,
        //Saving a lot of copies especially when sending things to the asynchronous logging
        let mut dec_log = self.declog.borrow_mut();

        match consensus_msg.message().kind() {
            ConsensusMessageKind::PrePrepare(_) => dec_log.pre_prepares.push(consensus_msg.clone()),
            ConsensusMessageKind::Prepare(_) => dec_log.prepares.push(consensus_msg.clone()),
            ConsensusMessageKind::Commit(_) => dec_log.commits.push(consensus_msg.clone()),
        }

        if let Err(err) = self
            .persistent_log
            .queue_message(WriteMode::Async(None), consensus_msg)
        {
            error!("Failed to persist message {:?}", err);
        }
    }

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&self, header: Header, message: SystemMessage<State<S>, Request<S>, Reply<S>>) {
        match message {
            SystemMessage::Request(message) => {
                let key = operation_key::<Request<S>>(&header, &message);

                let seq_no = {
                    let latest_op_guard = self.latest_op.lock();

                    latest_op_guard.get(key).copied().unwrap_or(SeqNo::ZERO)
                };

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

                //Wrap the message in a read only reference so we can then pass it around without having to clone it everywhere,
                //Saving a lot of copies especially when sending things to the asynchronous logging
                let stored = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                let stored_2 = stored.clone();

                let mut dec_log = self.declog.borrow_mut();

                match stored.message().kind() {
                    ConsensusMessageKind::PrePrepare(_) => dec_log.pre_prepares.push(stored),
                    ConsensusMessageKind::Prepare(_) => dec_log.prepares.push(stored),
                    ConsensusMessageKind::Commit(_) => dec_log.commits.push(stored),
                }

                if let Err(err) = self
                    .persistent_log
                    .queue_message(WriteMode::Async(None), stored_2)
                {
                    error!("Failed to persist message {:?}", err);
                }
            }
            // rest are not handled by the log
            _ => (),
        }
    }

    pub fn insert_batched(
        &self,
        message: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        self.request_batches
            .insert(message.header().digest().clone(), message.clone());

        if let Err(err) = self
            .persistent_log
            .queue_message(WriteMode::Async(None), message)
        {
            error!("Failed to persist message {:?}", err);
        }
    }

    pub fn take_batched_requests(
        &self,
        batch_digest: &Digest,
    ) -> Option<Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>> {
        match self.request_batches.remove(batch_digest) {
            None => None,
            Some((_digest, batch)) => Some(batch),
        }
    }

    /// Retrieves a batch of requests to be proposed during a view change.
    pub fn view_change_propose(&self) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        todo!()

        //COLLECT ALL REQUESTS FROM THE MAP
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    ///
    /// If this request is only contained within a batch, this will not return correctly
    pub fn has_request(&self, digest: &Digest) -> bool {
        self.requests.contains_key(digest)
    }

    /// Clone the requests corresponding to the provided list of hash digests.
    pub fn clone_requests(
        &self,
        digests: &[Digest],
    ) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        digests
            .iter()
            .flat_map(|d| self.requests.get(d))
            .map(|rq_ref| rq_ref.value().clone())
            .collect()
    }

    /// Finalize a batch of client requests decided on the consensus instance
    /// with sequence number `seq`, retrieving the payload associated with their
    /// given digests `digests`.
    ///
    /// The log may be cleared resulting from this operation. Check the enum variant of
    /// `Info`, to perform a local checkpoint when appropriate.
    ///
    /// Returns a [`Option::None`] when we are running in Strict mode, indicating the
    /// batch request has been put in the execution queue, waiting for all of the messages
    /// to be persisted  
    pub fn finalize_batch(
        &self,
        seq: SeqNo,
        batch_digest: Digest,
        digests: &[Digest],
        needed_messages: Vec<Digest>,
        meta: BatchMeta,
    ) -> Result<Option<(Info, UpdateBatch<Request<S>>, BatchMeta)>> {
        //println!("Finalized batch of OPS seq {:?} on Node {:?}", seq, self.node_id);

        let mut rqs;

        match self.take_batched_requests(&batch_digest) {
            None => {
                debug!("Could not find batched requests, having to go 1 by 1");

                rqs = Vec::with_capacity(digests.len());

                for digest in digests {
                    let message =
                        self.requests
                            .remove(digest)
                            .map(|f| f.1)
                            .ok_or(Error::simple_with_msg(
                                ErrorKind::ConsensusLog,
                                "Request not present in log when finalizing",
                            ))?;

                    rqs.push(message);
                }
            }
            Some(requests) => {
                rqs = match requests.message().kind() {
                    ConsensusMessageKind::PrePrepare(req) => req.clone(),
                    _ => {
                        unreachable!()
                    }
                };
            }
        }

        {
            //TODO: Check how much we really need this
            //Encase in a scope to limit the action of the borrow
            let mut guard = self.decided.borrow_mut();

            for request in &rqs {
                guard.push(request.message().operation().clone());
            }
        }

        // let mut latest_op_guard = self.latest_op().lock();

        let mut batch = UpdateBatch::new_with_cap(seq, rqs.len());

        for x in rqs {
            let (header, message) = x.into_inner();

            let _key = operation_key::<Request<S>>(&header, &message);

            // let seq_no = latest_op_guard
            //     .get(key)
            //     .unwrap_or(&SeqNo::ZERO);
            //
            // if message.sequence_number() > *seq_no {
            //     latest_op_guard.insert(key, message.sequence_number());
            // }

            batch.add(
                header.from(),
                message.session_id(),
                message.sequence_number(),
                message.into_inner_operation(),
            );
        }

        // retrieve the sequence number stored within the PRE-PREPARE message
        // pertaining to the current request being executed
        let mut dec_log_guard = self.declog.borrow_mut();

        let last_seq_no = if dec_log_guard.pre_prepares.len() > 0 {
            let stored_pre_prepare =
                &dec_log_guard.pre_prepares[dec_log_guard.pre_prepares.len() - 1].message();
            stored_pre_prepare.sequence_number()
        } else {
            // the log was cleared concurrently, retrieve
            // the seq number stored before the log was cleared
            self.curr_seq.get()
        };

        let last_seq_no_u32 = u32::from(last_seq_no);

        let info = if last_seq_no_u32 > 0 && last_seq_no_u32 % PERIOD == 0 {
            //We check that % == 0 so we don't start multiple checkpoints
            self.begin_checkpoint(last_seq_no)?
        } else {
            Info::Nil
        };

        // the last executed sequence number
        dec_log_guard.last_exec = Some(seq);

        self.persistent_log.queue_batch(((info, batch, meta), needed_messages))
    }

    fn begin_checkpoint(&self, seq: SeqNo) -> Result<Info> {
        let earlier = self.checkpoint.replace(CheckpointState::None);

        self.checkpoint.replace(match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => {
                CheckpointState::PartialWithEarlier { seq, earlier }
            }
            // FIXME: this may not be an invalid state after all; we may just be generating
            // checkpoints too fast for the execution layer to keep up, delivering the
            // hash digests of the appstate
            _ => return Err("Invalid checkpoint state detected").wrapped(ErrorKind::ConsensusLog),
        });

        if let Some(observer) = &self.observer {
            observer
                .tx()
                .send(MessageType::Event(ObserveEventKind::CheckpointStart(seq)))
                .unwrap();
        }

        Ok(Info::BeginCheckpoint)
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&self, final_seq: SeqNo, appstate: State<S>) -> Result<()> {
        match *self.checkpoint.borrow() {
            CheckpointState::None => {
                Err("No checkpoint has been initiated yet").wrapped(ErrorKind::ConsensusLog)
            }
            CheckpointState::Complete(_) => {
                Err("Checkpoint already finalized").wrapped(ErrorKind::ConsensusLog)
            }
            CheckpointState::Partial { seq: _ }
            | CheckpointState::PartialWithEarlier { seq: _, .. } => {
                self.checkpoint
                    .replace(CheckpointState::Complete(Arc::new(ReadOnly::new(
                        Checkpoint {
                            seq: final_seq,
                            appstate,
                        },
                    ))));

                let mut decided_request_count: usize = 0;

                //Clear the log of messages up to final_seq.
                //Messages ahead of final_seq will not be removed as they are not included in the
                //Checkpoint and therefore must be logged.
                {
                    let mut guard = self.declog.borrow_mut();

                    let mut new_preprepares = Vec::new();
                    let mut new_prepares = Vec::new();
                    let mut new_commits = Vec::new();

                    for ele in &guard.pre_prepares {
                        if ele.message().sequence_number() <= final_seq {
                            //Mark the requests contained in this message for removal
                            decided_request_count += match ele.message().kind() {
                                ConsensusMessageKind::PrePrepare(messages) => messages.len(),
                                _ => 0,
                            };


                            continue;
                        }

                        new_preprepares.push(ele.clone());
                    }

                    for ele in &guard.prepares {
                        if ele.message().sequence_number() <= final_seq {
                            continue;
                        }

                        new_prepares.push(ele.clone());
                    }

                    for ele in &guard.commits {
                        if ele.message().sequence_number() <= final_seq {
                            continue;
                        }

                        new_commits.push(ele.clone());
                    }

                    guard.pre_prepares = new_preprepares;
                    guard.prepares = new_prepares;
                    guard.commits = new_commits;

                    if let Some(last_sq) = guard.pre_prepares.last() {
                        // store the id of the last received pre-prepare,
                        // which corresponds to the request currently being
                        // processed
                        self.curr_seq.replace(last_sq.message().sequence_number());
                    } else {
                        self.curr_seq.replace(final_seq);
                    }
                }

                //Clear the decided requests log
                {
                    let mut decided = self.decided.borrow_mut();

                    if decided_request_count < decided.len() {
                        let mut new_decided = Vec::with_capacity(decided.len() - decided_request_count);

                        let to_keep = decided.len() - decided_request_count;

                        for _ in 0..to_keep {
                            let rq_to_keep = decided.pop().unwrap();

                            new_decided.push(rq_to_keep);
                        }

                        //Get the requests in the correct order as we have inverted the order with the previous operation
                        new_decided.reverse();

                        drop(decided);

                        self.decided.replace(new_decided);
                    } else if decided_request_count == decided.len() {
                        decided.clear();
                    } else {
                        //We can't have more decided requests than decided requests LOL
                        unreachable!()
                    }
                }

                if let Some(observer) = &self.observer {
                    observer
                        .tx()
                        .send(MessageType::Event(ObserveEventKind::CheckpointEnd(
                            self.curr_seq.get(),
                        )))
                        .unwrap();
                }

                Ok(())
            }
        }
    }
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = header.from().into();
    let session_id: u64 = message.session_id().into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
