pub mod consensus_backlog;
mod serialization;

use std::collections::BTreeMap;
use std::convert::TryInto;
use std::iter;

use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc};
use std::sync::atomic::{AtomicUsize, Ordering};

use log::error;

use crate::bft::communication::channel;
use crate::bft::communication::channel::{ChannelSyncRx, SendError};
use crate::bft::communication::message::ConsensusMessage;
use crate::bft::communication::message::ConsensusMessageKind;
use crate::bft::communication::message::Header;
use crate::bft::communication::NodeId;

use crate::bft::communication::serialize::{SharedData};
use crate::bft::msg_log::persistent::serialization::{make_proof_info, Persister};

use crate::bft::crypto::hash::Digest;

use crate::bft::error::*;
use crate::bft::executable::ExecutorHandle;
use crate::bft::globals::ReadOnly;
use crate::bft::ordering::Orderable;
use crate::bft::{
    communication::{channel::ChannelSyncTx, message::StoredMessage},
    executable::{Request, Service, State},
    ordering::SeqNo,
    persistentdb::KVDB,
};
use crate::bft::executable::ExecutionRequest::Read;
use crate::bft::msg_log::decided_log::BatchExecutionInfo;
use crate::bft::msg_log::decisions::{Decision, OnGoingDecision, Proof, ProofMetadata};
use crate::bft::sync::view::ViewInfo;

use self::consensus_backlog::ConsensusBackLogHandle;
use self::consensus_backlog::ConsensusBacklog;

use super::Checkpoint;
use super::DecisionLog;

///Latest checkpoint made by febft
const LATEST_STATE: &str = "latest_state";
///First sequence number (committed) since the last checkpoint
const FIRST_SEQ: &str = "first_seq";
///Last sequence number (committed) since the last checkpoint
const LATEST_SEQ: &str = "latest_seq";
///Latest known view sequence number
const LATEST_VIEW_SEQ: &str = "latest_view_seq";

const CF_PROOF_INFO: &str = "proof_info";
const CF_OTHER: &str = "others";
const CF_PRE_PREPARES: &str = "preprepares";
const CF_PREPARES: &str = "prepares";
const CF_COMMITS: &str = "commits";

///The general type for a callback.
/// Callbacks are optional and can be used when you want to
/// execute a function when the logger stops finishes the computation
pub type CallbackType = Box<dyn FnOnce(Result<ResponseMsg>) + Send>;

pub enum PersistentLogMode<S: Service> {
    ///The strict log mode is meant to indicate that the consensus can only be finalized and the
    /// requests executed when the replica has all the information persistently stored.
    ///
    /// This allows for all replicas to crash and still be able to recover from their own stored
    /// local state, meaning we can always recover without losing any piece of replied to information
    /// So we have the guarantee that once a request has been replied to, it will never be lost (given f byzantine faults).
    ///
    /// Performance will be dependent on the speed of the datastore as the consensus will only move to the
    /// executing phase once all requests have been successfully stored.
    Strict(ConsensusBackLogHandle<S>),

    ///Optimistic mode relies a lot more on the assumptions that are made by the BFT algorithm in order
    /// to maximize the performance.
    ///
    /// It works by separating the persistent data storage with the consensus algorithm. It relies on
    /// the fact that we only allow for f faults concurrently, so we assume that we can never have a situation
    /// where more than f replicas fail at the same time, so they can always rely on the existence of other
    /// replicas that it can use to rebuild it's state from where it left off.
    ///
    /// One might say this provides no security benefits comparatively to storing information just in RAM (since
    /// we don't have any guarantees on what was actually stored in persistent storage)
    /// however this does provide more performance benefits as we don't have to rebuild the entire state from the
    /// other replicas of the system, which would degrade performance. We can take our incomplete state and
    /// just fill in the blanks using the state transfer algorithm
    Optimistic,

    ///Perform no persistent logging to the database and rely only on the prospect that
    /// We are always able to rebuild our state from other replicas that may be online
    None,
}

pub trait PersistentLogModeTrait: Send {
    fn init_persistent_log<S>(executor: ExecutorHandle<S>) -> PersistentLogMode<S>
        where
            S: Service + 'static;
}

///Strict log mode initializer
pub struct StrictPersistentLog;

impl PersistentLogModeTrait for StrictPersistentLog {
    fn init_persistent_log<S>(executor: ExecutorHandle<S>) -> PersistentLogMode<S>
        where
            S: Service + 'static,
    {
        let handle = ConsensusBacklog::init_backlog(executor);

        PersistentLogMode::Strict(handle)
    }
}

///Optimistic log mode intializer
pub struct OptimisticPersistentLog;

impl PersistentLogModeTrait for OptimisticPersistentLog {
    fn init_persistent_log<S: Service + 'static>(_: ExecutorHandle<S>) -> PersistentLogMode<S> {
        PersistentLogMode::Optimistic
    }
}

pub struct NoPersistentLog;

impl PersistentLogModeTrait for NoPersistentLog {
    fn init_persistent_log<S>(_: ExecutorHandle<S>) -> PersistentLogMode<S> where S: Service + 'static {
        PersistentLogMode::None
    }
}

///How should the data be written and response delivered?
/// If Sync is chosen the function will block on the call and return the result of the operation
/// If Async is chosen the function will not block and will return the response as a message to a channel
pub enum WriteMode {
    //When writing in async mode, you have the option of having the response delivered on a function
    //Of your choice
    //Note that this function will be executed on the persistent logging thread, so keep it short and
    //Be careful with race conditions.
    NonBlockingSync(Option<CallbackType>),
    BlockingSync,
}


///TODO: Handle sequence numbers that loop the u32 range.
/// This is the main reference to the persistent log, used to push data to it
pub struct PersistentLog<S: Service>
{
    persistency_mode: PersistentLogMode<S>,

    // A handle for the persistent log workers (each with his own thread)
    worker_handle: Arc<PersistentLogWorkerHandle<S>>,

    ///The persistent KV-DB to be used
    db: KVDB,
}

/// A handle for all of the persistent workers.
/// Handles task distribution and load balancing across the
/// workers
pub struct PersistentLogWorkerHandle<S: Service> {
    round_robin_counter: AtomicUsize,
    tx: Vec<PersistentLogWriteStub<S>>,
}

///A stub that is only useful for writing to the persistent log
#[derive(Clone)]
struct PersistentLogWriteStub<S: Service> {
    tx: ChannelSyncTx<ChannelMsg<S>>,
}

impl<S: Service + 'static> PersistentLog<S>
{
    pub fn init_log<K, T>(executor: ExecutorHandle<S>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
            T: PersistentLogModeTrait
    {
        let prefixes = vec![CF_PROOF_INFO, CF_OTHER, CF_PRE_PREPARES, CF_PREPARES, CF_COMMITS];

        let log_mode = T::init_persistent_log(executor);

        let mut response_txs = vec![];

        match &log_mode {
            PersistentLogMode::Strict(handle) => response_txs.push(handle.logger_tx().clone()),
            _ => {}
        }

        let kvdb = KVDB::new(db_path, prefixes)?;

        let (tx, rx) = channel::new_bounded_sync(1024);

        let worker = PersistentLogWorker {
            request_rx: rx,
            response_txs,
            db: kvdb.clone(),
        };

        match &log_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                std::thread::Builder::new().name(format!("Persistent log Worker #1"))
                    .spawn(move || {
                        worker.work();
                    }).unwrap();
            }
            _ => {}
        }

        let persistent_log_write_stub = PersistentLogWriteStub { tx };

        let worker_handle = Arc::new(PersistentLogWorkerHandle {
            round_robin_counter: AtomicUsize::new(0),
            tx: vec![persistent_log_write_stub],
        });

        Ok(Self {
            persistency_mode: log_mode,
            worker_handle,
            db: kvdb,
        })
    }

    /// TODO: Maybe make this async? We need it to start execution anyways...
    pub fn read_state(&self) -> Result<Option<InstallState<S>>> {
        match self.kind() {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                read_latest_state::<S>(&self.db)
            }
            PersistentLogMode::None => {
                Ok(None)
            }
        }
    }

    pub fn kind(&self) -> &PersistentLogMode<S> {
        &self.persistency_mode
    }

    pub fn write_committed_seq_no(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_committed(seq, callback)
                    }
                    WriteMode::BlockingSync => write_latest_seq(&self.db, seq),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn write_view_info(&self, write_mode: WriteMode, view_seq: ViewInfo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_view_number(view_seq, callback)
                    }
                    WriteMode::BlockingSync => write_latest_view_seq(&self.db, view_seq.sequence_number()),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    /// Write the metadata of the proof
    pub fn write_proof_metadata(&self, write_mode: WriteMode,
                                metadata: ProofMetadata) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof_metadata(metadata, callback)
                    }
                    WriteMode::BlockingSync => {
                        finalize_instance(&self.db, metadata)
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn write_message(
        &self,
        write_mode: WriteMode,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_message(msg, callback)
                    }
                    WriteMode::BlockingSync => write_message::<S>(&self.db, &msg),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn write_checkpoint(
        &self,
        write_mode: WriteMode,
        checkpoint: Arc<ReadOnly<Checkpoint<State<S>>>>,
    ) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_state(checkpoint, callback)
                    }
                    WriteMode::BlockingSync => {
                        let state = checkpoint.state();

                        let last_seq = checkpoint.last_seq();

                        write_checkpoint::<S>(&self.db, state, last_seq.clone())
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn write_invalidate(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_invalidate(seq, callback)
                    }
                    WriteMode::BlockingSync => {
                        invalidate_seq(&self.db, seq)?;

                        Ok(())
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    /// Attempt to install the state into persistent storage
    pub fn write_install_state(&self, write_mode: WriteMode, state: InstallState<S>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_install_state(state, callback)
                    }
                    WriteMode::BlockingSync => {
                        write_state::<S>(&self.db, state)
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    /// Write a proof to the persistent log.
    pub fn write_proof(&self, write_mode: WriteMode, proof: Proof<Request<S>>) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::NonBlockingSync(callback) => {
                        self.worker_handle.queue_proof(proof, callback)
                    }
                    WriteMode::BlockingSync => {
                        write_proof::<S>(&self.db, proof)
                    }
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    ///Attempt to queue a batch into waiting for persistent logging
    /// If the batch does not have to wait, it's returned to it can be instantly
    /// passed to the executor
    pub fn wait_for_batch_persistency_and_execute(&self, batch: BatchExecutionInfo<S>) -> Result<Option<BatchExecutionInfo<S>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(consensus_backlog) => {
                consensus_backlog.queue_batch(batch)?;

                Ok(None)
            }
            PersistentLogMode::Optimistic | PersistentLogMode::None => {
                Ok(Some(batch))
            }
        }
    }

    ///Attempt to queue a batch that was received in the form of a completed proof
    /// into waiting for persistent logging, instead of receiving message by message (Received in
    /// a view change)
    /// If the batch does not have to wait, it's returned to it can be instantly
    /// passed to the executor
    pub fn wait_for_proof_persistency_and_execute(&self, batch: BatchExecutionInfo<S>) -> Result<Option<BatchExecutionInfo<S>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(backlog) => {
                backlog.queue_batch_proof(batch)?;

                Ok(None)
            }
            _ => {
                Ok(Some(batch))
            }
        }
    }
}

impl<S: Service> Clone for PersistentLog<S> {
    fn clone(&self) -> Self {
        Self {
            persistency_mode: self.persistency_mode.clone(),
            worker_handle: self.worker_handle.clone(),
            db: self.db.clone(),
        }
    }
}

impl<S: Service> Clone for PersistentLogMode<S> {
    fn clone(&self) -> Self {
        match self {
            PersistentLogMode::Strict(handle) => {
                PersistentLogMode::Strict(handle.clone())
            }
            PersistentLogMode::Optimistic => {
                PersistentLogMode::Optimistic
            }
            PersistentLogMode::None => {
                PersistentLogMode::None
            }
        }
    }
}

impl<S: Service> Deref for PersistentLogWriteStub<S> {
    type Target = ChannelSyncTx<ChannelMsg<S>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<S: Service> PersistentLogWorkerHandle<S> {
    /// Employ a simple round robin load distribution
    fn next_worker(&self) -> &PersistentLogWriteStub<S> {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);

        self.tx.get(counter % self.tx.len()).unwrap()
    }

    fn translate_error<V, T>(result: std::result::Result<V, SendError<T>>) -> Result<V> {
        match result {
            Ok(v) => {
                Ok(v)
            }
            Err(err) => {
                Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent, format!("{:?}", err).as_str()))
            }
        }
    }

    fn register_callback_receiver(&self, receiver: ChannelSyncTx<ResponseMsg>) -> Result<()> {
        for write_stub in &self.tx {
            Self::translate_error(write_stub.send((PWMessage::RegisterCallbackReceiver(receiver.clone()), None)))?;
        }

        Ok(())
    }

    fn queue_invalidate(&self, seq_no: SeqNo, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::Invalidate(seq_no), callback)))
    }

    fn queue_committed(&self, seq_no: SeqNo, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::Committed(seq_no), callback)))
    }

    fn queue_proof_metadata(&self, metadata: ProofMetadata, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::ProofMetadata(metadata), callback)))
    }

    fn queue_view_number(&self, view: ViewInfo, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::View(view), callback)))
    }

    fn queue_message(&self, message: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
                     callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::Message(message), callback)))
    }

    fn queue_state(&self, state: Arc<ReadOnly<Checkpoint<State<S>>>>, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::Checkpoint(state), callback)))
    }

    fn queue_install_state(&self, install_state: InstallState<S>, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::InstallState(install_state), callback)))
    }

    fn queue_proof(&self, proof: Proof<Request<S>>, callback: Option<CallbackType>) -> Result<()> {
        Self::translate_error(self.next_worker().send((PWMessage::Proof(proof), callback)))
    }
}

///A worker for the persistent logging
struct PersistentLogWorker<S: Service> {
    request_rx: ChannelSyncRx<ChannelMsg<S>>,

    response_txs: Vec<ChannelSyncTx<ResponseMsg>>,

    db: KVDB,
}

impl<S: Service> PersistentLogWorker<S> {
    fn work(mut self) {
        loop {
            let (request, callback) = match self.request_rx.recv() {
                Ok((request, callback)) => (request, callback),
                Err(err) => {
                    error!("{:?}", err);
                    break;
                }
            };

            let response = self.exec_req(request);

            if let Some(callback) = callback {
                //If we have a callback to call with the response, then call it
                (callback)(response);
            } else {
                //If not, then deliver it down the response_txs
                match response {
                    Ok(response) => {
                        for ele in &self.response_txs {
                            if let Err(err) = ele.send(response.clone()) {
                                error!("Failed to deliver response to log. {:?}", err);
                            }
                        }
                    }
                    Err(err) => {
                        error!("Failed to execute persistent log request because {:?}", err);
                    }
                }
            }
        }
    }

    fn exec_req(&mut self, message: PWMessage<S>) -> Result<ResponseMsg> {
        Ok(match message {
            PWMessage::View(view) => {
                write_latest_view_seq(&self.db, view.sequence_number())?;

                ResponseMsg::ViewPersisted(view.sequence_number())
            }
            PWMessage::Committed(seq) => {
                write_latest_seq(&self.db, seq)?;

                ResponseMsg::CommittedPersisted(seq)
            }
            PWMessage::Message(msg) => {
                write_message::<S>(&self.db, &msg)?;

                let seq = msg.message().sequence_number();

                ResponseMsg::WroteMessage(seq, msg.header().digest().clone())
            }
            PWMessage::Checkpoint(checkpoint) => {
                write_checkpoint::<S>(&self.db, checkpoint.state(), checkpoint.sequence_number())?;

                ResponseMsg::Checkpointed(checkpoint.sequence_number())
            }
            PWMessage::Invalidate(seq) => {
                invalidate_seq(&self.db, seq)?;

                ResponseMsg::InvalidationPersisted(seq)
            }
            PWMessage::InstallState(state) => {
                let seq_no = state.2.last_execution().unwrap();

                write_state::<S>(&self.db, state)?;

                ResponseMsg::InstalledState(seq_no)
            }
            PWMessage::Proof(proof) => {
                let seq_no = proof.seq_no();

                write_proof::<S>(&self.db, proof)?;

                ResponseMessage::Proof(seq_no)
            }
            PWMessage::RegisterCallbackReceiver(receiver) => {
                self.response_txs.push(receiver);

                ResponseMessage::RegisteredCallback
            }
            PWMessage::ProofMetadata(metadata) => {
                let seq = metadata.seq_no();

                finalize_instance(&self.db, metadata)?;

                ResponseMessage::WroteMetadata(seq)
            }
        })
    }
}

/// Messages that are sent to the logging thread to log specific requests
pub(crate) type ChannelMsg<S> = (PWMessage<S>, Option<CallbackType>);

/// The type of the installed state information
pub type InstallState<S> = (
    //The view sequence number
    SeqNo,
    // The state that we want to persist
    Arc<ReadOnly<Checkpoint<State<S>>>>,
    //The decision log that comes after that state
    DecisionLog<Request<S>>,
);

pub(crate) enum PWMessage<S: Service> {
    //Persist a new view into the persistent storage
    View(ViewInfo),

    //Persist a new sequence number as the consensus instance has been committed and is therefore ready to be persisted
    Committed(SeqNo),

    // Persist the metadata for a given decision
    ProofMetadata(ProofMetadata),

    //Persist a given message into storage
    Message(Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>),

    //Persist a given state into storage.
    Checkpoint(Arc<ReadOnly<Checkpoint<State<S>>>>),

    //Remove all associated stored messages for this given seq number
    Invalidate(SeqNo),

    // Register a proof of the decision log
    Proof(Proof<Request<S>>),

    //Install a recovery state received from CST or produced by us
    InstallState(InstallState<S>),

    RegisterCallbackReceiver(ChannelSyncTx<ResponseMsg>),
}

pub type ResponseMsg = ResponseMessage;

#[derive(Clone)]
pub enum ResponseMessage {
    ///Notify that we have persisted the view with the given sequence number
    ViewPersisted(SeqNo),

    ///Notifies that we have persisted the sequence number that has been persisted (Only the actual sequence number)
    /// Not related to actually persisting messages
    CommittedPersisted(SeqNo),

    // Notifies that the metadata for a given seq no has been persisted
    WroteMetadata(SeqNo),

    ///Notifies that a message with a given SeqNo and a given unique identifier for the message
    /// TODO: Decide this unique identifier
    WroteMessage(SeqNo, Digest),

    // Notifies that the state has been successfully installed and returns
    InstalledState(SeqNo),

    /// Notifies that all messages relating to the given sequence number have been destroyed
    InvalidationPersisted(SeqNo),

    /// Notifies that the given checkpoint was persisted into the database
    Checkpointed(SeqNo),

    // Stored the proof with the given sequence
    Proof(SeqNo),

    RegisteredCallback,
}


/// Basic Information about a proof
/// Used for storage by the persistent storage system
pub struct ProofInfo {
    batch_digest: Digest,

    pre_prepare_ordering: Vec<Digest>,
}

impl From<(Digest, Vec<Digest>)> for ProofInfo {
    fn from(value: (Digest, Vec<Digest>)) -> Self {
        let (digest, ordering) = value;

        ProofInfo {
            batch_digest: digest,
            pre_prepare_ordering: ordering,
        }
    }
}

///Write a state provided by the CST protocol into the persistent DB
fn write_state<S: Service>(db: &KVDB, (view, checkpoint, dec_log): InstallState<S>) -> Result<()> {
    //Update the view number to the current view number
    write_latest_view_seq(db, view)?;

    //Write the received checkpoint into persistent storage and delete all existing
    //Messages that were stored as they will be replaced by the new log
    write_checkpoint::<S>(db, checkpoint.state(), checkpoint.sequence_number())?;

    for proof in dec_log.proofs() {
        for ele in proof.pre_prepares() {
            write_message::<S>(db, ele)?;
        }

        for ele in proof.prepares() {
            write_message::<S>(db, ele)?;
        }

        for ele in proof.commits() {
            write_message::<S>(db, ele)?;
        }
    }

    Ok(())
}

///Read the latest state from the persistent DB
fn read_latest_state<S: Service>(db: &KVDB) -> Result<Option<InstallState<S>>> {
    let view = read_latest_view_seq(db)?;

    let state = read_latest_checkpoint::<S>(db)?;

    let first_seq = read_first_seq(db)?;

    let dec_log = read_all_present_proofs::<S>(db)?;

    let checkpoint = Checkpoint::new(first_seq.unwrap(), state.unwrap());

    Ok(Some((view.unwrap(), checkpoint, dec_log)))
}

///Read the latest checkpoint stored in persistent storage
fn read_latest_checkpoint<S: Service>(db: &KVDB) -> Result<Option<State<S>>> {
    let checkpoint = db.get(CF_OTHER, LATEST_STATE)?;

    if let Some(checkpoint) = checkpoint {
        let state = <S::Data as SharedData>::deserialize_state(&checkpoint[..])?;

        Ok(Some(state))
    } else {
        Ok(None)
    }
}

///Write a checkpoint to persistent storage.
/// Deletes all previous messages from the log as they no longer pertain to the current checkpoint.
/// Sets the first seq to the seq number of the last message the state contains
fn write_checkpoint<S: Service>(db: &KVDB, state: &State<S>, last_seq: SeqNo) -> Result<()> {
    let mut buf = Vec::new();

    <S::Data as SharedData>::serialize_state(&mut buf, &state)?;

    db.set(CF_OTHER, LATEST_STATE, buf)?;

    //Only remove the previous operations after persisting the checkpoint,
    //To assert no information can be lost
    let start = db.get(CF_OTHER, FIRST_SEQ)?.unwrap();

    let start = serialization::read_seq(&start[..])?;

    //Update the first seq number, officially making all of the previous messages useless
    //And ready to be deleted
    db.set(
        CF_OTHER,
        FIRST_SEQ,
        serialization::make_seq(last_seq.next())?,
    )?;

    //We want the end to be the last message contained inside the checkpoint.
    //Not the current message end, which can already be far ahead of the current checkpoint,
    //Which would mean we could lose information.
    let end = last_seq;

    // Delete all of the now obsolete proofs
    delete_proofs_between(db, start, end)?;

    Ok(())
}

///Read all messages for the given range
/// The end seq number is included in the messages
fn read_message_for_range<S: Service>(
    db: &KVDB,
    msg_seq_start: SeqNo,
    msg_seq_end: SeqNo,
) -> Result<Vec<StoredMessage<ConsensusMessage<Request<S>>>>> {
    let mut start_key = serialization::make_message_key(msg_seq_start, None)?;
    let mut end_key = serialization::make_message_key(msg_seq_end.next(), None)?;

    let mut messages = Vec::new();

    let preprepares = db.iter_range(CF_PRE_PREPARES, Some(&start_key), Some(&end_key))?;

    let prepares = db.iter_range(CF_PREPARES, Some(&start_key), Some(&end_key))?;

    let commits = db.iter_range(CF_COMMITS, Some(&start_key), Some(&end_key))?;

    for res in preprepares {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    for res in prepares {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    for res in commits {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    Ok(messages)
}

fn read_proof_infos_for_range(
    db: &KVDB,
    msg_seq_start: SeqNo,
    msg_seq_end: SeqNo,
) -> Result<BTreeMap<SeqNo, ProofInfo>> {
    let start_key = serialization::make_seq(msg_seq_start)?;
    let end_key = serialization::make_seq(msg_seq_end.next())?;

    let proof_infos = db.iter_range(CF_PROOF_INFO, Some(start_key), Some(end_key))?;

    let mut result = BTreeMap::new();

    for res in proof_infos {
        if let Ok((key, value)) = res {
            let seq = serialization::read_seq(&*key)?;

            let pi = serialization::deserialize_proof_info(&*value)?;

            result.insert(seq, pi);
        }
    }

    Ok(result)
}

fn finalize_instance(db: &KVDB, metadata: ProofMetadata) -> Result<()> {
    let pi = ProofInfo {
        batch_digest: metadata.batch_digest(),
        pre_prepare_ordering: metadata.pre_prepare_ordering().clone(),
    };

    let mut key = serialization::make_seq(metadata.seq_no())?;

    let mut proof_info = make_proof_info(&pi)?;

    db.set(CF_PROOF_INFO, key, proof_info)?;

    Ok(())
}

fn write_proof<S: Service>(db: &KVDB, proof: Proof<Request<S>>) -> Result<()> {
    let pi = ProofInfo {
        batch_digest: proof.batch_digest(),
        pre_prepare_ordering: proof.pre_prepare_ordering().clone(),
    };

    let mut key = serialization::make_seq(proof.seq_no())?;

    let mut proof_info = make_proof_info(&pi)?;

    db.set(CF_PROOF_INFO, key, proof_info)?;

    for pre_prepare in proof.pre_prepares() {
        write_message::<S>(db, pre_prepare)?;
    }

    for prepare in proof.prepares() {
        write_message::<S>(db, prepare)?;
    }

    for commits in proof.commits() {
        write_message::<S>(db, commits)?;
    }

    Ok(())
}

///Read all the messages for a given consensus instance
fn read_messages_for_seq<S: Service>(
    db: &KVDB,
    msg_seq: SeqNo,
) -> Result<Vec<StoredMessage<ConsensusMessage<Request<S>>>>> {
    let mut start_key =
        serialization::make_message_key(msg_seq, None)?;
    let mut end_key =
        serialization::make_message_key(msg_seq.next(), None)?;


    let mut messages = Vec::new();

    let pre_prepares = db.iter_range(CF_PRE_PREPARES, Some(&start_key), Some(&end_key))?;

    let prepares = db.iter_range(CF_PREPARES, Some(&start_key), Some(&end_key))?;

    let commits = db.iter_range(CF_COMMITS, Some(&start_key), Some(&end_key))?;

    for res in pre_prepares {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    for res in prepares {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    for res in commits {
        if let Ok((key, value)) = res {
            messages.push(parse_message::<S, Box<[u8]>>(key, value)?);
        }
    }

    Ok(messages)
}

fn read_proof<S: Service>(db: &KVDB, seq_no: SeqNo) -> Result<Proof<Request<S>>> {
    let mut start_key = serialization::make_message_key(seq_no, None)?;
    let mut end_key = serialization::make_message_key(seq_no.next(), None)?;


    todo!()
}

/// Read all proofs that are present in the log
fn read_all_present_proofs<S: Service>(db: &KVDB) -> Result<DecisionLog<Request<S>>> {
    // The last seq number to have been executed
    let last_seq = read_latest_seq(db)?;

    let first_seq = read_first_seq(db)?;

    let (first_seq, last_seq) = match (first_seq, last_seq) {
        (Some(first), Some(latest)) => (first, latest),
        _ => {
            todo!()
        }
    };

    let messages = read_message_for_range::<S>(db, first_seq, last_seq)?;

    let proof_infos = read_proof_infos_for_range(db, first_seq, last_seq)?;

    let mut final_decisions = BTreeMap::new();

    // Analyse all of the messages from the persistent log and place them in the correct part
    // Of the decision log
    for message in messages {
        let wrapped_msg = Arc::new(ReadOnly::new(message));

        let seq_no = wrapped_msg.message().sequence_number();

        if !final_decisions.contains_key(&seq_no) {
            // If we have not yet processed this decision, create a new data object for it
            let proof_info = proof_infos.get(&seq_no).unwrap();

            let (digest, ordering): (Digest, Vec<Digest>) = (proof_info.batch_digest.clone(), proof_info.pre_prepare_ordering.clone());

            let mut ongoing_decision = OnGoingDecision::init_from_info(seq_no,
                                                                       digest,
                                                                       ordering);

            final_decisions.insert(seq_no, ongoing_decision);
        }

        let decision = final_decisions.get_mut(&seq_no).unwrap();

        match wrapped_msg.message().kind() {
            ConsensusMessageKind::PrePrepare(_) => decision.append_pre_prepare(wrapped_msg),
            ConsensusMessageKind::Prepare(_) => decision.append_prepare(wrapped_msg),
            ConsensusMessageKind::Commit(_) => decision.append_commit(wrapped_msg)
        }
    }

    // When we are done reading all of the messages, we must create the decision log

    let mut proof_vec: Vec<Proof<Request<S>>> =
        Vec::with_capacity(final_decisions.len());

    // Take out all of the proofs one by one and add them to the final vec

    while let Some((seq, ongoing_decision)) = final_decisions.pop_first() {
        if let Some(last) = proof_vec.last() {
            // Check if we have skipped any sequence numbers, for any given reason
            if last.sequence_number().next() != seq {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogPersistent,
                                                  "Failed to load proofs from local storage as\
                                                   we are missing a decision from the log"));
            }
        }

        let result = ongoing_decision.proof(None)?;

        proof_vec.push(result);
    }

    let decision_log = DecisionLog::from_decided(last_seq, proof_vec);

    Ok(decision_log)
}

/// Parse a given message from its bytes representation
fn parse_message<S: Service, T>(
    _key: T,
    value: T,
) -> Result<StoredMessage<ConsensusMessage<Request<S>>>> where T: AsRef<[u8]> {
    let header = Header::deserialize_from(&value.as_ref()[..Header::LENGTH])?;

    let message = <S::Data>::deserialize_consensus_message(&value.as_ref()[Header::LENGTH..])?;

    Ok(StoredMessage::new(header, message))
}

///Write the given message into the keystore
fn write_message<S: Service>(
    db: &KVDB,
    message: &StoredMessage<ConsensusMessage<Request<S>>>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(Header::LENGTH + message.header().payload_length());

    message.header().serialize_into(buf.as_mut_slice()).unwrap();

    <S::Data>::serialize_consensus_message(message.message(), &mut buf[Header::LENGTH..])?;

    let msg_seq = message.message().sequence_number();

    let mut key = serialization::make_message_key(msg_seq, Some(message.header().from()))?;

    match message.message().kind() {
        ConsensusMessageKind::PrePrepare(_) => db.set(CF_PRE_PREPARES, key, buf)?,
        ConsensusMessageKind::Prepare(_) => db.set(CF_PREPARES, key, buf)?,
        ConsensusMessageKind::Commit(_) => db.set(CF_COMMITS, key, buf)?,
    }

    Ok(())
}

/// Delete all the proofs stored between the first_seq and the last_seq
/// [first_seq, last_seq]
fn delete_proofs_between(db: &KVDB, first_seq: SeqNo, last_seq: SeqNo) -> Result<()> {

    delete_proof_metadata_between(db, first_seq, last_seq)?;

    delete_proof_messages_between(db, first_seq, last_seq)?;

    Ok(())
}

fn delete_proof_messages_between(db: &KVDB, first_seq: SeqNo, last_seq: SeqNo) -> Result<()> {

    let start = serialization::make_message_key(first_seq, None)?;
    let end = serialization::make_message_key(last_seq.next(), None)?;

    db.erase_range(CF_PRE_PREPARES, &start, &end)?;
    db.erase_range(CF_PREPARES, &start, &end)?;
    db.erase_range(CF_COMMITS, &start, &end)?;

    Ok(())
}

fn delete_proof_metadata_between(db: &KVDB, first_seq: SeqNo, last_seq: SeqNo) -> Result<()> {

    let start = serialization::make_seq(first_seq)?;
    let end = serialization::make_seq(last_seq.next())?;

    db.erase_range(CF_PROOF_INFO, start, end)?;

    Ok(())
}

fn invalidate_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    delete_all_msgs_for_seq(db, seq)?;
    delete_all_proof_metadata_for_seq(db, seq)?;
    Ok(())
}

///Delete all msgs relating to a given sequence number
fn delete_all_msgs_for_seq(db: &KVDB, msg_seq: SeqNo) -> Result<()> {
    let mut start_key =
        serialization::make_message_key(msg_seq, None)?;
    let mut end_key =
        serialization::make_message_key(msg_seq.next(), None)?;

    db.erase_range(CF_PRE_PREPARES, &start_key, &end_key)?;

    db.erase_range(CF_PREPARES, &start_key, &end_key)?;

    db.erase_range(CF_COMMITS, &start_key, &end_key)?;

    Ok(())
}

fn delete_all_proof_metadata_for_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq = serialization::make_seq(seq)?;

    db.erase(CF_PROOF_INFO, &seq)?;

    Ok(())
}

fn read_first_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, FIRST_SEQ)?;

    get_seq_from_result(result)
}

fn read_latest_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, LATEST_SEQ)?;

    get_seq_from_result(result)
}

fn write_latest_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let mut f_seq_no = serialization::make_seq(seq)?;

    if !db.exists(CF_OTHER, FIRST_SEQ)? {
        db.set(CF_OTHER, FIRST_SEQ, &f_seq_no[..])?;
    }

    db.set(CF_OTHER, LATEST_SEQ, &f_seq_no[..])
}

fn read_latest_view_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, LATEST_VIEW_SEQ)?;

    get_seq_from_result(result)
}

fn write_latest_view_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let mut f_seq_no =
        serialization::make_seq(seq)?;

    db.set(CF_OTHER, LATEST_VIEW_SEQ, &f_seq_no[..])
}

fn get_seq_from_result(res: Option<Vec<u8>>) -> Result<Option<SeqNo>> {
    if let Some(res) = res {
        let res = serialization::read_seq(&res[..])?;

        Ok(Some(res))
    } else {
        Ok(None)
    }
}