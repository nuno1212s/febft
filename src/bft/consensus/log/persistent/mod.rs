pub mod consensus_backlog;

use std::convert::TryInto;
use std::marker::PhantomData;

use std::path::Path;
use std::sync::Arc;

use log::error;

use crate::bft::communication::channel::ChannelSyncRx;
use crate::bft::communication::message::ConsensusMessage;
use crate::bft::communication::message::ConsensusMessageKind;
use crate::bft::communication::message::Header;
use crate::bft::communication::NodeId;

use crate::bft::communication::serialize::SharedData;

use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::install_recovery_state;
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
pub type CallbackType = Box<dyn FnOnce(Result<ResponseMsg>)>;

pub enum PersistentLogMode<S: Service> {
    ///The strict log mode is meant to indicate that the consensus can only be finalized and the
    /// requests executed when the replica has all the information persistently stored.
    ///
    /// This allows for all replicas to crash and still be able to recover from their own stored
    /// local state, meaning we can always recover
    ///
    /// Performance will be dependent on the speed of the datastore as the consensus will only move to the
    /// executing phase once all requests have been successfully stored.
    Strict(ConsensusBacklog<S>),

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
}

/// Messages that are sent to the logging thread to log specific requests
pub type ChannelMsg<S> = (Message<S>, Option<CallbackType>);

pub type InstallState<S> = (
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

pub struct PersistentLog<S: Service> {
    ///The persistency mode for this log
    persistency_mode: PersistentLogMode<S>,

    tx: ChannelSyncTx<ChannelMsg<S>>,

    ///The persistent KV-DB to be used
    db: KVDB,
}

impl<S: Service> PersistentLog<S> {
    pub fn init_log<T>(log_mode: PersistentLogMode<S>, db_path: T) -> Result<()>
    where
        T: AsRef<Path>,
    {
        let prefixes = vec![CF_OTHER, CF_PREPREPARES, CF_PREPARES, CF_COMMITS];

        let kvdb = KVDB::new(db_path, prefixes)?;

        Ok(())
    }

    pub fn queue_committed(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        todo!()
    }

    pub fn queue_view_number(&self, write_mode: WriteMode, view_seq: SeqNo) -> Result<()> {
        todo!()
    }

    pub fn queue_message(
        &self,
        write_mode: WriteMode,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) -> Result<()> {
        match write_mode {
            WriteMode::Async(callback) => {
                self.tx.send((Message::Message(msg), callback));

                todo!()
            }
            Sync => write_message::<S>(&self.db, &msg),
        }
    }

    pub fn queue_state(
        &self,
        write_mode: WriteMode,
        state: Arc<ReadOnly<Checkpoint<State<S>>>>,
    ) -> Result<()> {
        match write_mode {
            WriteMode::Async(callback) => {
                self.tx.send((Message::Checkpoint(state), callback));
                todo!();
            }
            Sync => Ok(()),
        }
    }

    pub fn queue_invalidate(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match write_mode {
            WriteMode::Async(callback) => {
                self.tx.send((Message::Invalidate(seq), callback));

                todo!()
            }
            Sync => Ok(()),
        }
    }
}

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

            
        }
    }

    fn exec_req(&self, message: Message<S>) -> Result<ResponseMsg> {
        Ok(match message {
            Message::View(view) => {
                update_latest_view_seq(&self.db, view.sequence_number())?;

                ResponseMsg::ViewPersisted(view.sequence_number())
            }
            Message::Committed(seq) => {
                update_latest_seq(&self.db, seq)?;

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

fn write_state<S: Service>(db: &KVDB, (view, checkpoint, dec_log): InstallState<S>) -> Result<()> {
    //Update the view number to the current view number
    update_latest_view_seq(db, view)?;

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

    dec_log.last_exec = Some(last_seq);

    for ele in messages {
        let wrapped_msg = Arc::new(ReadOnly::new(ele));

        match wrapped_msg.message().kind() {
            ConsensusMessageKind::PrePrepare(_) => dec_log.pre_prepares.push(wrapped_msg),
            ConsensusMessageKind::Prepare(_) => dec_log.prepares.push(wrapped_msg),
            ConsensusMessageKind::Commit(_) => dec_log.commits.push(wrapped_msg),
        }
    }

    let checkpoint = Arc::new(ReadOnly::new(Checkpoint {
        seq: last_seq,
        appstate: state.unwrap(),
    }));

    Ok(Some((view.unwrap(), checkpoint, dec_log)))
}

fn read_latest_checkpoint<S: Service>(db: &KVDB) -> Result<Option<State<S>>> {
    let checkpoint = db.get(CF_OTHER, LATEST_STATE)?;

    if let Some(checkpoint) = checkpoint {
        let state = <S::Data as SharedData>::deserialize_state(&checkpoint[..])?;

        Ok(Some(state))
    } else {
        Ok(None)
    }
}

fn write_checkpoint<S: Service>(db: &KVDB, state: &State<S>, last_seq: SeqNo) -> Result<()> {
    let mut buf = Vec::new();

    <S::Data as SharedData>::serialize_state(&mut buf, &state)?;

    db.set(CF_OTHER, LATEST_STATE, buf)?;

    //Only remove the previous operations after persisting the checkpoint,
    //To assert no information can be lost
    let start = db.get(CF_OTHER, FIRST_SEQ)?;
    let end = u32::from(last_seq).to_le_bytes();

    match (&start) {
        Some(start) => {
            //Erase the logs from the previous
            db.erase_range(CF_COMMITS, start, end)?;
            db.erase_range(CF_PREPARES, start, end)?;
            db.erase_range(CF_PREPREPARES, start, end)?;
        }
        _ => {
            return Err(Error::simple_with_msg(
                ErrorKind::ConsensusLogPersistent,
                "Failed to get the range of values to erase",
            ));
        }
    }

    db.erase_keys(CF_OTHER, [LATEST_SEQ, FIRST_SEQ])?;

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

    for (key, value) in preprepares {
        messages.push(parse_message::<S>(key, value)?);
    }

    for (key, value) in prepares {
        messages.push(parse_message::<S>(key, value)?);
    }

    for (key, value) in commits {
        messages.push(parse_message::<S>(key, value)?);
    }

    Ok(messages)
}

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

    for (key, value) in preprepares {
        messages.push(parse_message::<S>(key, value)?);
    }

    for (key, value) in prepares {
        messages.push(parse_message::<S>(key, value)?);
    }

    for (key, value) in commits {
        messages.push(parse_message::<S>(key, value)?);
    }

    Ok(messages)
}

fn parse_message<S: Service>(
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<StoredMessage<ConsensusMessage<Request<S>>>> {
    let header = Header::deserialize_from(&value[..Header::LENGTH])?;

    let message = <S::Data>::deserialize_consensus_message(&value[Header::LENGTH..])?;

    Ok(StoredMessage::new(header, message))
}

fn write_message<S: Service>(
    db: &KVDB,
    message: &StoredMessage<ConsensusMessage<Request<S>>>,
) -> Result<()> {
    let mut buf = Vec::with_capacity(Header::LENGTH + message.header().payload_length());

    message.header().serialize_into(buf.as_mut_slice()).unwrap();

    <S::Data>::serialize_consensus_message(&mut buf[Header::LENGTH..], message.message())?;

    let msg_seq = message.message().sequence_number();

    let key = make_msg_seq(msg_seq, Some(message.header().from()));

    match message.message().kind() {
        ConsensusMessageKind::PrePrepare(_) => db.set(CF_PREPREPARES, key, buf)?,
        ConsensusMessageKind::Prepare(_) => db.set(CF_PREPARES, key, buf)?,
        ConsensusMessageKind::Commit(_) => db.set(CF_COMMITS, key, buf)?,
    }

    Ok(())
}

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

fn update_latest_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
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
        .wrapped(ErrorKind::ConsensusLogPersistent)?;

    let seq = u32::from_le_bytes(seq_cast);

    Ok(seq.into())
}

fn update_latest_view_seq(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq_no: u32 = seq.into();

    db.set(CF_OTHER, LATEST_VIEW_SEQ, seq_no.to_le_bytes())
}

fn make_msg_seq(msg_seq: SeqNo, from: Option<NodeId>) -> Vec<u8> {
    let msg_u32: u32 = msg_seq.into();

    let from_u32: u32 = if let Some(from) = from {
        from.into()
    } else {
        0
    };

    [msg_u32.to_le_bytes(), from_u32.to_le_bytes()].concat()
}
