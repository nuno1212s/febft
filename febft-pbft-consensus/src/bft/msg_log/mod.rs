//! A module to manage the `febft` message log.

use std::path::Path;
use std::sync::Arc;


use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_communication::message::{Header, StoredMessage};
use febft_execution::app::Service;
use febft_execution::ExecutorHandle;
use febft_messages::messages::RequestMessage;
use crate::bft::message::ConsensusMessage;
use crate::bft::msg_log::decided_log::DecidedLog;
use crate::bft::msg_log::deciding_log::{DecidingLog};
use crate::bft::msg_log::decisions::{Checkpoint, DecisionLog};
use crate::bft::msg_log::pending_decision::PendingRequestLog;

use self::persistent::{PersistentLog};
use self::persistent::{PersistentLogModeTrait};

pub mod persistent;
pub mod decisions;
pub mod deciding_log;
pub mod decided_log;
pub mod pending_decision;

/// Checkpoint period.
///
/// Every `PERIOD` messages, the message log is cleared,
/// and a new log checkpoint is initiated.
/// TODO: Move this to an env variable as it can be highly dependent on the service implemented on top of it

pub const PERIOD: u32 = 1000;

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


pub type ReadableConsensusMessage<O> = Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>;

pub fn initialize_persistent_log<S, K, T>(executor: ExecutorHandle<S>, db_path: K)
                                          -> Result<PersistentLog<S>>
    where S: Service + 'static, K: AsRef<Path>, T: PersistentLogModeTrait {
    PersistentLog::init_log::<K, T>(executor, db_path)
}

pub fn initialize_decided_log<S: Service + 'static>(persistent_log: PersistentLog<S>) -> Result<DecidedLog<S>> {

    Ok(DecidedLog::init_decided_log(persistent_log))

}

pub fn initialize_pending_request_log<S: Service + 'static>() -> Result<PendingRequestLog<S>> {
    Ok(PendingRequestLog::new())
}


pub(crate) fn initialize_deciding_log<S: Service + 'static>(node_id: NodeId) -> Result<DecidingLog<S>> {
    Ok(DecidingLog::new(node_id))

}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = header.from().into();
    let session_id: u64 = message.session_id().into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
