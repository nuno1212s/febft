//! A module to manage the `febft` message log.

use std::path::Path;
use std::sync::Arc;

use atlas_common::error::*;
use atlas_common::globals::ReadOnly;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_communication::message::{Header, StoredMessage};
use atlas_execution::ExecutorHandle;
use atlas_execution::serialize::SharedData;
use atlas_core::messages::RequestMessage;
use atlas_core::state_transfer::Checkpoint;

use crate::bft::message::ConsensusMessage;
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::decisions::DecisionLog;

pub mod decisions;
pub mod deciding_log;
pub mod decided_log;

pub fn initialize_decided_log<D: SharedData + 'static, PL>(node_id: NodeId,
                                                       persistent_log: PL,
                                                       state: Option<DecisionLog<D::Request>>) -> Result<Log<D, PL>> {
    Ok(Log::init_decided_log(node_id, persistent_log, state))
}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    operation_key_raw(header.from(), message.session_id())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
