pub mod consensus_backlog;

use std::convert::TryInto;

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use log::error;

use crate::bft::communication::channel;
use crate::bft::communication::channel::ChannelSyncRx;
use crate::bft::communication::message::ConsensusMessage;
use crate::bft::communication::message::ConsensusMessageKind;
use crate::bft::communication::message::Header;
use crate::bft::communication::NodeId;

use crate::bft::communication::serialize::{SharedData, Persister};

use crate::bft::core::server::ViewInfo;
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
use crate::bft::msg_log::persistent::consensus_backlog::{BatchInfo, PendingBatch};

use self::consensus_backlog::ConsensusBackLogHandle;
use self::consensus_backlog::ConsensusBacklog;

use super::Checkpoint;
use super::DecisionLog;

///Latest checkpoint made by febft
pub const LATEST_STATE: &str = "latest_state";
///First sequence number (committed) since the last checkpoint
pub const FIRST_SEQ: &str = "first_seq";
///Last sequence number (committed) since the last checkpoint
pub const LATEST_SEQ: &str = "latest_seq";
///Latest known view sequence number
pub const LATEST_VIEW_SEQ: &str = "latest_view_seq";

pub const CF_OTHER: &str = "others";
pub const CF_PREPREPARES: &str = "preprepares";
pub const CF_PREPARES: &str = "prepares";
pub const CF_COMMITS: &str = "commits";

///The general type for a callback.
/// Callbacks are optional and can be used when you want to
/// execute a function when the logger stops finishes the computation
pub type CallbackType = Box<dyn FnOnce(Result<ResponseMsg>) + Send>;

#[derive(Clone)]
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
    //TODO: Maybe change this to the threadpool?
    Async(Option<CallbackType>),
    Sync,
}

#[derive(Clone)]
///TODO: Handle sequence numbers that loop the u32 range.
/// This is the main reference to the persistent log, used to push data to it
pub struct PersistentLog<S: Service, T>
    where
        T: PersistentLogModeTrait,
{
    ///The persistency mode for this log
    persistency_mode: PersistentLogMode<S>,

    //Handle to send the work to the worker thread
    tx: ChannelSyncTx<ChannelMsg<S>>,

    ///The persistent KV-DB to be used
    db: KVDB,

    p: PhantomData<T>,
}

///We can do this thanks to two primitives
unsafe impl<S, T> Sync for PersistentLog<S, T>
    where
        S: Service,
        T: PersistentLogModeTrait,
{}

impl<S: Service + 'static, T> PersistentLog<S, T>
    where
        T: PersistentLogModeTrait,
{
    pub fn init_log<K>(executor: ExecutorHandle<S>, db_path: K) -> Result<Self>
        where
            K: AsRef<Path>,
    {
        let prefixes = vec![CF_OTHER, CF_PREPREPARES, CF_PREPARES, CF_COMMITS];

        let log_mode = T::init_persistent_log(executor);

        let mut response_txs = vec![];

        match &log_mode {
            PersistentLogMode::Strict(handle) => response_txs.push(handle.logger_tx().clone()),
            _ => {}
        }

        let kvdb = KVDB::new(db_path, prefixes)?;

        let (tx, rx) = channel::new_bounded_sync(1024);

        let _worker = PersistentLogWorker {
            request_rx: rx,
            response_txs,
            db: kvdb.clone(),
        };

        //TODO: Start worker

        Ok(Self {
            persistency_mode: log_mode,
            tx,
            db: kvdb,
            p: Default::default(),
        })
    }

    pub fn kind(&self) -> &PersistentLogMode<S> {
        &self.persistency_mode
    }

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

    pub fn queue_committed(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::Async(callback) => {
                        self.tx.send((Message::Committed(seq), callback));

                        todo!()
                    }
                    WriteMode::Sync => write_latest_seq(&self.db, seq),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn queue_view_number(&self, write_mode: WriteMode, view_seq: ViewInfo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::Async(callback) => {
                        self.tx.send((Message::View(view_seq), callback));

                        todo!()
                    }
                    WriteMode::Sync => write_latest_view_seq(&self.db, view_seq.sequence_number()),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn queue_message(
        &self,
        write_mode: WriteMode,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::Async(callback) => {
                        self.tx.send((Message::Message(msg), callback));

                        todo!()
                    }
                    WriteMode::Sync => write_message::<S>(&self.db, &msg),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn queue_state(
        &self,
        write_mode: WriteMode,
        state: Arc<ReadOnly<Checkpoint<State<S>>>>,
    ) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::Async(callback) => {
                        self.tx.send((Message::Checkpoint(state), callback));

                        todo!()
                    }
                    WriteMode::Sync => Ok(()),
                }
            }
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    pub fn queue_invalidate(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match self.persistency_mode {
            PersistentLogMode::Strict(_) | PersistentLogMode::Optimistic => {
                match write_mode {
                    WriteMode::Async(callback) => {
                        self.tx.send((Message::Invalidate(seq), callback));

                        todo!()
                    }
                    WriteMode::Sync => Ok(()),
                }}
            PersistentLogMode::None => {
                Ok(())
            }
        }
    }

    ///Attempt to queue a batch into waiting for persistent logging
    /// If the batch does not have to wait, it's returned to it can be instantly
    /// passed to the executor
    pub fn queue_batch(&self, batch: PendingBatch<S>) -> Result<Option<BatchInfo<S>>> {
        match &self.persistency_mode {
            PersistentLogMode::Strict(consensus_backlog) => {
                consensus_backlog.queue_batch(batch)?;

                Ok(None)
            }
            PersistentLogMode::Optimistic | PersistentLogMode::None => {
                Ok(Some(batch.0))
            }
        }
    }
}

///A worker for the persistent logging
pub struct PersistentLogWorker<S: Service> {
    request_rx: ChannelSyncRx<ChannelMsg<S>>,

    response_txs: Vec<ChannelSyncTx<ResponseMsg>>,

    db: KVDB,
}

impl<S: Service> PersistentLogWorker<S> {
    fn work(self) {
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
                (callback)(response);
            } else {
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

    fn exec_req(&self, message: Message<S>) -> Result<ResponseMsg> {
        Ok(match message {
            Message::View(view) => {
                write_latest_view_seq(&self.db, view.sequence_number())?;

                ResponseMsg::ViewPersisted(view.sequence_number())
            }
            Message::Committed(seq) => {
                write_latest_seq(&self.db, seq)?;

                ResponseMsg::CommittedPersisted(seq)
            }
            Message::Message(msg) => {
                write_message::<S>(&self.db, &msg)?;

                let seq = msg.message().sequence_number();

                ResponseMsg::WroteMessage(seq, msg.header().digest().clone())
            }
            Message::Checkpoint(checkpoint) => {
                write_checkpoint::<S>(&self.db, checkpoint.state(), checkpoint.sequence_number())?;

                ResponseMsg::Checkpointed(checkpoint.sequence_number())
            }
            Message::Invalidate(seq) => {
                delete_all_msgs_for_seq::<S>(&self.db, seq)?;

                ResponseMsg::InvalidationPersisted(seq)
            }
            Message::InstallState(state) => {
                write_state::<S>(&self.db, state)?;

                ResponseMsg::InstalledState(todo!())
            }
        })
    }
}

/// Messages that are sent to the logging thread to log specific requests
pub type ChannelMsg<S> = (Message<S>, Option<CallbackType>);

/// The type of the installed state information
pub type InstallState<S> = (
    //The view sequence number
    SeqNo,
    Arc<ReadOnly<Checkpoint<State<S>>>>,
    DecisionLog<Request<S>>,
);

pub enum Message<S: Service> {
    //Persist a new view into the persistent storage
    View(ViewInfo),

    //Persist a new sequence number as the consensus instance has been committed and is therefore ready to be persisted
    Committed(SeqNo),

    //Persist a given message into storage
    Message(Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>),

    //Persist a given state into storage.
    Checkpoint(Arc<ReadOnly<Checkpoint<State<S>>>>),

    //Remove all associated stored messages for this given seq number
    Invalidate(SeqNo),

    //Install a recovery state received from CST
    InstallState(InstallState<S>),
}

pub type ResponseMsg = ResponseMessage;

#[derive(Clone)]
pub enum ResponseMessage {
    ///Notify that we have persisted the view with the given sequence number
    ViewPersisted(SeqNo),

    ///Notifies that we have persisted the sequence number that has been persisted (Only the actual sequence number)
    /// Not related to actually persisting messages
    CommittedPersisted(SeqNo),

    ///Notifies that a message with a given SeqNo and a given unique identifier for the message
    /// TODO: Decide this unique identifier
    WroteMessage(SeqNo, Digest),

    //
    InstalledState(SeqNo),

    /// Notifies that all messages relating to the given sequence number have been destroyed
    InvalidationPersisted(SeqNo),

    /// Notifies that the given checkpoint was persisted into the database
    Checkpointed(SeqNo),
}

///Write a state provided by the CST protocol into the persistent DB
fn write_state<S: Service>(db: &KVDB, (view, checkpoint, dec_log): InstallState<S>) -> Result<()> {
    //Update the view number to the current view number
    write_latest_view_seq(db, view)?;

    //Write the received checkpoint into persistent storage and delete all existing
    //Messages that were stored as they will be replaced by the new log
    write_checkpoint::<S>(db, checkpoint.state(), checkpoint.sequence_number())?;

    for ele in dec_log.pre_prepares() {
        write_message::<S>(db, ele)?;
    }

    for ele in dec_log.prepares() {
        write_message::<S>(db, ele)?;
    }

    for ele in dec_log.commits() {
        write_message::<S>(db, ele)?;
    }

    Ok(())
}

///Read the latest state from the persistent DB
fn read_latest_state<S: Service>(db: &KVDB) -> Result<Option<InstallState<S>>> {
    let view = read_latest_view_seq(db)?;

    let state = read_latest_checkpoint::<S>(db)?;

    let last_seq = read_latest_seq(db)?;

    let first_seq = read_first_seq(db)?;

    let (first_seq, last_seq) = match (first_seq, last_seq) {
        (Some(first), Some(latest)) => (first, latest),
        _ => {
            todo!()
        }
    };

    let messages = read_message_for_range::<S>(db, first_seq, last_seq)?;

    let mut dec_log = DecisionLog::new();

    dec_log.finished_quorum_execution(last_seq);

    for ele in messages {
        let wrapped_msg = Arc::new(ReadOnly::new(ele));

        //TODO: Make sure this is sorted
        match wrapped_msg.message().kind() {
            ConsensusMessageKind::PrePrepare(_) => dec_log.append_pre_prepare(wrapped_msg),
            ConsensusMessageKind::Prepare(_) => dec_log.append_prepare(wrapped_msg),
            ConsensusMessageKind::Commit(_) => dec_log.append_commit(wrapped_msg),
        }
    }

    let checkpoint = Checkpoint::new(first_seq, state.unwrap());

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
    let start = db.get(CF_OTHER, FIRST_SEQ)?;

    //Update the first seq number, officially making all of the previous messages useless
    //And ready to be deleted
    db.set(
        CF_OTHER,
        FIRST_SEQ,
        u32::from(last_seq.next()).to_le_bytes(),
    )?;


    //We want the end to be the last message contained inside the checkpoint.
    //Not the current message end, which can already be far ahead of the current checkpoint,
    //Which would mean we could lose information.
    let end = u32::from(last_seq).to_le_bytes();

    match &start {
        Some(start) => {
            let start = &start[..];

            //Erase the logs from the previous executions
            db.erase_range(CF_COMMITS, start, &end)?;
            db.erase_range(CF_PREPARES, start, &end)?;
            db.erase_range(CF_PREPREPARES, start, &end)?;
        }
        _ => {
            return Err(Error::simple_with_msg(
                ErrorKind::MsgLogPersistent,
                "Failed to get the range of values to erase",
            ));
        }
    }

    Ok(())
}

///Read all messages for the given range
/// The end seq number is included in the messages
fn read_message_for_range<S: Service>(
    db: &KVDB,
    msg_seq_start: SeqNo,
    msg_seq_end: SeqNo,
) -> Result<Vec<StoredMessage<ConsensusMessage<Request<S>>>>> {
    let start_key = make_msg_seq(msg_seq_start, None);
    let end_key = make_msg_seq(msg_seq_end.next(), None);

    let mut messages = Vec::new();

    let preprepares = db.iter_range(CF_PREPREPARES, Some(&start_key), Some(&end_key))?;

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

///Read all the messages for a given consensus instance
fn read_messages_for_seq<S: Service>(
    db: &KVDB,
    msg_seq: SeqNo,
) -> Result<Vec<StoredMessage<ConsensusMessage<Request<S>>>>> {
    let start_key = make_msg_seq(msg_seq, None);

    let end_key = make_msg_seq(msg_seq.next(), None);

    let mut messages = Vec::new();

    let preprepares = db.iter_range(CF_PREPREPARES, Some(&start_key), Some(&end_key))?;

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

///Parse a given message from its bytes representation
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

    let key = make_msg_seq(msg_seq, Some(message.header().from()));

    match message.message().kind() {
        ConsensusMessageKind::PrePrepare(_) => db.set(CF_PREPREPARES, key, buf)?,
        ConsensusMessageKind::Prepare(_) => db.set(CF_PREPARES, key, buf)?,
        ConsensusMessageKind::Commit(_) => db.set(CF_COMMITS, key, buf)?,
    }

    Ok(())
}

///Delete all msgs relating to a given sequence number
fn delete_all_msgs_for_seq<S: Service>(db: &KVDB, msg_seq: SeqNo) -> Result<()> {
    let start_key = make_msg_seq(msg_seq, None);

    let end_key = make_msg_seq(msg_seq.next(), None);

    db.erase_range(CF_PREPREPARES, &start_key, &end_key)?;

    db.erase_range(CF_PREPARES, &start_key, &end_key)?;

    db.erase_range(CF_COMMITS, &start_key, &end_key)?;

    Ok(())
}

fn read_first_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, FIRST_SEQ)?;

    if let Some(res) = result {
        let seq = read_seq_from_vec(res)?;

        Ok(Some(seq))
    } else {
        Ok(None)
    }
}

fn read_latest_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, LATEST_SEQ)?;

    if let Some(res) = result {
        let seq = read_seq_from_vec(res)?;

        Ok(Some(seq))
    } else {
        Ok(None)
    }
}

fn write_latest_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq_no: u32 = seq.into();

    if !db.exists(CF_OTHER, FIRST_SEQ)? {
        db.set(CF_OTHER, FIRST_SEQ, seq_no.to_le_bytes())?;
    }

    db.set(CF_OTHER, LATEST_SEQ, seq_no.to_le_bytes())
}

fn read_latest_view_seq(db: &KVDB) -> Result<Option<SeqNo>> {
    let result = db.get(CF_OTHER, LATEST_VIEW_SEQ)?;

    if let Some(res) = result {
        let res = read_seq_from_vec(res)?;

        Ok(Some(res))
    } else {
        Ok(None)
    }
}

fn read_seq_from_vec(data: Vec<u8>) -> Result<SeqNo> {
    let seq_cast: [u8; 4] = data
        .as_slice()
        .try_into()
        .wrapped(ErrorKind::MsgLogPersistent)?;

    let seq = u32::from_le_bytes(seq_cast);

    Ok(seq.into())
}

fn write_latest_view_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq_no: u32 = seq.into();

    db.set(CF_OTHER, LATEST_VIEW_SEQ, seq_no.to_le_bytes())
}

pub fn make_msg_seq(msg_seq: SeqNo, from: Option<NodeId>) -> Vec<u8> {
    let msg_u32: u32 = msg_seq.into();

    let from_u32: u32 = if let Some(from) = from {
        from.into()
    } else {
        0
    };

    [msg_u32.to_le_bytes(), from_u32.to_le_bytes()].concat()
}
