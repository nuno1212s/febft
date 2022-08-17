use std::marker::PhantomData;

use crate::bft::communication::channel::ChannelSyncRx;
use crate::bft::communication::serialize::SharedData;
use crate::bft::cst::RecoveryState;
use crate::bft::error::*;
use crate::bft::executable::ExecutorHandle;
use crate::bft::ordering::Orderable;
use crate::bft::{
    communication::{
        channel::ChannelSyncTx,
        message::{RequestMessage, StoredMessage},
    },
    executable::{Request, Service, State},
    ordering::SeqNo,
    persistentdb::KVDB,
};

///Latest checkpoint made by febft
pub const LATEST_STATE: &str = "latest_state";
///First sequence number (committed) since the last checkpoint
pub const FIRST_SEQ: &str = "first_seq";
///Last sequence number (committed) since the last checkpoint
pub const LATEST_SEQ: &str = "latest_seq";
///Latest known view sequence number
pub const LATEST_VIEW_SEQ: &str = "latest_view_seq";

pub enum PersistentLogMode<S: Service> {
    ///The strict log mode is meant to indicate that the consensus can only be finalized and the
    /// requests executed when the replica has all the information persistently stored.
    ///
    /// This allows for all replicas to crash and still be able to recover from their own stored
    /// local state, meaning we can always recover
    ///
    /// Performance will be dependent on the speed of the datastore as the consensus will only move to the
    /// executing phase once all requests have been successfully stored.
    Strict(ExecutorHandle<S>),

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
pub type ChannelMsg<S> = Message<S>;

pub enum Message<S: Service> {
    //Persist a new view number
    View(SeqNo),

    //Persist a new sequence number as the consensus instance has been committed and is therefore ready to be persisted
    Committed(SeqNo),

    //Persist a given message into storage
    Message(StoredMessage<RequestMessage<Request<S>>>),

    //Persist a given state into storage.
    Checkpoint(State<S>),

    //Remove all associated stored messages for this given seq number
    Invalidate(SeqNo),

    //Install a recovery state received from CST
    InstallState(RecoveryState<State<S>, Request<S>>),
}

pub type ResponseMsg<S> = ResponseMessage<S>;

pub enum ResponseMessage<S: Service> {
    
    ViewPersisted(SeqNo),

    CommittedPersisted(SeqNo),

    //TODO: MessagePersisted(SeqNo)
    P(PhantomData<S>),

    InvalidationPersisted(SeqNo),
}

pub struct PersistentLogHandle<S: Service> {
    tx: ChannelSyncTx<ChannelMsg<S>>,
    db: KVDB,
}

pub enum WriteMode {
    Async,
    Sync,
}

impl<S: Service> PersistentLogHandle<S> {
    pub fn queue_committed(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
todo!()
    }

    pub fn queue_view_number(&self, write_mode: WriteMode, view_seq: SeqNo) -> Result<()> {
todo!()
    }

    pub fn queue_message(
        &self,
        write_mode: WriteMode,
        msg: StoredMessage<RequestMessage<Request<S>>>,
    ) -> Result<()> {
        match write_mode {
            Async => self
                .tx
                .send(Message::Message(msg))
                .wrapped(ErrorKind::ConsensusLogPersistent),
            Sync => write_message(&self.db, &msg),
        }
    }

    ///TODO: We definitely don't want to be cloning states around so how can we get a reference
    /// To this state without breaking everything. Might have to go unsafe for this
    pub fn queue_state(&self, write_mode: WriteMode, state: State<S>) -> Result<()> {
        match write_mode {
            Async => self
                .tx
                .send(Message::Message(msg))
                .wrapped(ErrorKind::ConsensusLogPersistent),
            Sync => Ok(()),
        }
    }

    pub fn queue_invalidate(&self, write_mode: WriteMode, seq: SeqNo) -> Result<()> {
        match write_mode {
            Async => self
                .tx
                .send(Message::Invalidate(seq))
                .wrapped(ErrorKind::ConsensusLogPersistent),
            Sync => Ok(()),
        }
    }
}

pub struct PersistentLog<S: Service> {
    ///The persistency mode for this log
    persistency_mode: PersistentLogMode<S>,

    ///The persistent KV-DB to be used
    db: KVDB,
}

pub struct PersistentLogWorker<S: Service> {
    rx: ChannelSyncRx<ChannelMsg<S>>,

    db: KVDB,
}

fn write_state<S: Service>(db: &KVDB, recovery_state: &RecoveryState<State<S>, Request<S>>) -> Result<()> {

    //Update the view number to the current view number
    update_latest_view_seq(db, recovery_state.view().sequence_number())?;

    //Write the received checkpoint into persistent storage and delete all existing 
    //Messages that were stored as they will be replaced by the new log
    write_checkpoint(db, recovery_state.checkpoint().state())?;

    for ele in recovery_state.decision_log().pre_prepares() {
        write_message(db, ele)?;
    }

    for ele in recovery_state.decision_log().prepares() {
        write_message(db, ele)?;
    }

    for ele in recovery_state.decision_log().commits() {
        write_message(db, ele)?;
    }

    Ok(())
}

fn write_checkpoint<S: Service>(db: &KVDB, state: &State<S>) -> Result<()> {
    let mut buf = Vec::new();

    <S::Data as SharedData>::serialize_state(&mut buf, &state);

    db.set(LATEST_STATE, buf)?;

    //Only remove the previous operations after persisting the checkpoint,
    //To assert no information can be lost
    let start = db.get(FIRST_SEQ)?;
    let end = db.get(LATEST_SEQ)?;

    match (&start, &end) {
        (Some(start), Some(end)) => {
            //Erase the logs from the previous
            db.erase_range(start, end)?;
        }
        (_, _) => {
            return Err(Error::simple_with_msg(
                ErrorKind::ConsensusLogPersistent,
                "Failed to get the range of values to erase",
            ));
        }
    }

    db.erase_keys([LATEST_SEQ, FIRST_SEQ])?;

    Ok(())
}

fn write_message<S: Service>(
    db: &KVDB,
    message: &StoredMessage<RequestMessage<Request<S>>>,
) -> Result<()> {
    todo!()
}

fn update_latest_seq<S: Service>(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq_no: u32 = seq.into();

    if !db.exists(FIRST_SEQ)? {
        db.set(FIRST_SEQ, seq_no.to_le_bytes())?;
    }

    db.set(LATEST_SEQ, seq_no.to_be_bytes())
}

fn update_latest_view_seq<S: Service>(db: &KVDB, seq: SeqNo) -> Result<()> {
    let seq_no: u32 = seq.into();

    db.set(LATEST_VIEW_SEQ, seq_no.to_be_bytes())
}
