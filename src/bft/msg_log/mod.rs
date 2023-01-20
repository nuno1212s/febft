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
use crate::bft::consensus::Consensus;
use crate::bft::core::server::observer::{MessageType, ObserverHandle};
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;
use crate::bft::executable::{ExecutorHandle, Reply, Request, Service, State, UpdateBatch};
use crate::bft::globals::ReadOnly;
use crate::bft::msg_log::deciding_log::CompletedBatch;
use crate::bft::msg_log::decisions::{Checkpoint, DecisionLog};
use crate::bft::ordering::{Orderable, SeqNo};

use self::persistent::{PersistentLog, WriteMode};
use self::persistent::{PersistentLogModeTrait};

pub mod persistent;
pub mod decisions;
pub mod deciding_log;

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

pub type ReadableConsensusMessage<O> = Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>;

/// Represents a log of messages received by the BFT system.
pub struct Log<S: Service, T: PersistentLogModeTrait> {
    node_id: NodeId,

    //This item will only be accessed by the replica request thread
    //The current stored SeqNo in the checkpoint state.
    //NOTE: THIS IS NOT THE CURR_SEQ NUMBER IN THE CONSENSUS
    curr_seq: Cell<SeqNo>,

    //This will only be accessed by the replica processing thread since requests will only be
    //Decided by the consensus protocol, which operates completly in the replica thread
    declog: RefCell<DecisionLog<Request<S>>>,

    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    latest_op: Mutex<IntMap<SeqNo>>,

    //This item will be accessed from both the client request thread and the
    //Replica request thread
    //This stores client requests that have not yet been put into a batch
    pending_requests: ConcurrentHashMap<Digest, StoredMessage<RequestMessage<Request<S>>>>,

    //Stores just request batches. Much faster than individually storing all the requests and
    //Then having to always access them one by one
    //Given this in principle only has one batch at a time, does this even need to happen'
    request_batches:
    ConcurrentHashMap<Digest, Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>>,
    //This will only be accessed from the replica request thread so we can wrap it
    //In a simple cell

    //Stores all of the performed requests concatenated in a vec
    decided: RefCell<Vec<Request<S>>>,
    //The most recent checkpoint that we have.
    //Contains the app state and the last executed seq no on
    //That app state
    checkpoint: RefCell<CheckpointState<State<S>>>,

    //Some stuff for statistics.
    meta: Arc<Mutex<BatchMeta>>,

    //Persistent logging
    persistent_log: PersistentLog<S, T>,

    //Observer
    observer: Option<ObserverHandle>,
}

pub struct PendingRequestLog<S> where S: Service {
    //This item will be accessed from both the client request thread and the
    //Replica request thread
    //This stores client requests that have not yet been put into a batch
    pending_requests: ConcurrentHashMap<Digest, StoredMessage<RequestMessage<Request<S>>>>,

    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    latest_op: Mutex<IntMap<SeqNo>>,
}


pub struct DecidedLog<S> where S: Service {
    //This item will only be accessed by the replica request thread
    //The current stored SeqNo in the checkpoint state.
    //NOTE: THIS IS NOT THE CURR_SEQ NUMBER IN THE CONSENSUS
    curr_seq: Cell<SeqNo>,

    //This will only be accessed by the replica processing thread since requests will only be
    //Decided by the consensus protocol, which operates completly in the replica thread
    declog: RefCell<DecisionLog<Request<S>>>,

    //The most recent checkpoint that we have.
    //Contains the app state and the last executed seq no on
    //That app state
    checkpoint: RefCell<CheckpointState<State<S>>>,
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
            curr_seq: Cell::new(SeqNo::ZERO),
            latest_op: Mutex::new(IntMap::new()),
            declog: RefCell::new(DecisionLog::new()),
            // TODO: use config value instead of const
            decided: RefCell::new(Vec::with_capacity(PERIOD as usize)),
            pending_requests: collections::concurrent_hash_map_with_capacity(PERIOD as usize),
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
                    ConsensusMessageKind::Commit(_) => { unreachable!() }
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
            _ => Err("Checkpoint to be finalized").wrapped(ErrorKind::MsgLog),
        }
    }

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
            ConsensusMessageKind::PrePrepare(_) => dec_log.append_pre_prepare(consensus_msg.clone()),
            ConsensusMessageKind::Prepare(_) => dec_log.append_prepare(consensus_msg.clone()),
            ConsensusMessageKind::Commit(_) => dec_log.append_commit(consensus_msg.clone()),
        }

        if let Err(err) = self
            .persistent_log
            .queue_message(WriteMode::Async(None), consensus_msg)
        {
            error!("Failed to persist message {:?}", err);
        }
    }

    /// Adds a new `message` and its respective `header` to the log.
    /// When passed an individual request into here, it will be stored in the pending requests.
    /// When passed a consensus message, it will be stored in the appropriate location
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

                self.pending_requests.insert(digest, stored);
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
                    ConsensusMessageKind::PrePrepare(_) => dec_log.append_pre_prepare(stored),
                    ConsensusMessageKind::Prepare(_) => dec_log.append_prepare(stored),
                    ConsensusMessageKind::Commit(_) => dec_log.append_commit(stored),
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

    /// Insert a batch of requests, received from a pre prepare
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

    /// Take the stored pre prepare request with the given digest
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
        let mut pending_messages = Vec::with_capacity(self.pending_requests.len());

        self.pending_requests.iter().for_each(|multi_ref| {
            pending_messages.push(multi_ref.value().clone());
        });

        pending_messages
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    ///
    /// If this request is only contained within a batch, this will not return correctly
    pub fn has_pending_request(&self, digest: &Digest) -> bool {
        self.pending_requests.contains_key(digest)
    }

    /// Clone the requests corresponding to the provided list of hash digests.
    pub fn clone_pending_requests(
        &self,
        digests: &[Digest],
    ) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        digests
            .iter()
            .flat_map(|d| self.pending_requests.get(d))
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
        completed_batch: CompletedBatch<S>,
        meta: BatchMeta,
    ) -> Result<Option<(Info, UpdateBatch<Request<S>>, BatchMeta)>> {
        //println!("Finalized batch of OPS seq {:?} on Node {:?}", seq, self.node_id);

        let (
            pre_prepare_message,
            _batch_digest,
            _request_digests,
            needed_messages
        ) = completed_batch.into();

        let reqs = match pre_prepare_message.message().kind().clone() {
            ConsensusMessageKind::PrePrepare(reqs) => {
                reqs
            }
            _ => {
                panic!("")
            }
        };

        {
            //TODO: Check how much we really need this
            //Encase in a scope to limit the action of the borrow
            let mut guard = self.decided.borrow_mut();

            for request in &reqs {
                guard.push(request.message().operation().clone());
            }
        }

        // let mut latest_op_guard = self.latest_op().lock();

        let mut batch = UpdateBatch::new_with_cap(seq, reqs.len());

        for x in reqs {
            let (header, message) = x.into_inner();

            let _key = operation_key::<Request<S>>(&header, &message);

            //TODO: Maybe make this run on separate thread?
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

        let pre_prepares = dec_log_guard.pre_prepares();

        let last_seq_no = if pre_prepares.len() > 0 {
            let stored_pre_prepare =
                pre_prepares[pre_prepares.len() - 1].message();

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
        dec_log_guard.finished_quorum_execution(last_seq_no);

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
            _ => return Err("Invalid checkpoint state detected").wrapped(ErrorKind::MsgLog),
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
                Err("No checkpoint has been initiated yet").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Complete(_) => {
                Err("Checkpoint already finalized").wrapped(ErrorKind::MsgLog)
            }
            CheckpointState::Partial { seq: _ } | CheckpointState::PartialWithEarlier { seq: _, .. } => {
                let checkpoint_state = CheckpointState::Complete(
                    Checkpoint::new(final_seq, appstate),
                );

                self.checkpoint.replace(checkpoint_state);

                let mut decided_request_count;

                //Clear the log of messages up to final_seq.
                //Messages ahead of final_seq will not be removed as they are not included in the
                //Checkpoint and therefore must be logged.
                {
                    let mut guard = self.declog.borrow_mut();

                    decided_request_count = guard.clear_until_seq(final_seq);

                    if let Some(last_sq) = guard.pre_prepares().last() {
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

                // {@
                // Observer code
                // @}

                if let Some(observer) = &self.observer {
                    observer
                        .tx()
                        .send(MessageType::Event(ObserveEventKind::CheckpointEnd(
                            self.curr_seq.get(),
                        )))
                        .unwrap();
                }

                //
                // @}
                //

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
