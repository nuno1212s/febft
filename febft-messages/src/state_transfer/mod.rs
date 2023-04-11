use std::sync::Arc;
use febft_execution::serialize::SharedData;
use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::ordering::{Orderable, SeqNo};
use crate::ordering_protocol::OrderingProtocolImpl;
use crate::serialize::StateTransferMessage;


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

    /// The last sequence no represented in this checkpoint
    pub fn last_seq(&self) -> &SeqNo {
        &self.seq
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

/// A trait for the implementation of the state transfer protocol
pub trait StateTransferProtocol<D: SharedData, SOP: StatefulOrderProtocol<D>> {

    type Serialization : StateTransferMessage;

    type Config;

    fn initialize(config: Self::Config) -> Result<Self> where Self: Sized;

}

/// An order protocol
pub trait StatefulOrderProtocol<D: SharedData> {

    #[cfg(feature = "serialize_capnp")]
    type DecLog: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type DecLog: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    type ViewInfo: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type ViewInfo: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    fn view(&self) -> Self::ViewInfo;

    fn install_state(&mut self, state: D::State, view_info: Self::ViewInfo, dec_log: Self::DecLog) -> Result<(D::State, Vec<D::Request>)>;

    fn snapshot_log(&mut self) -> Result<((D::State, SeqNo), Self::ViewInfo, Self::DecLog)>;


    #[cfg(feature = "serialize_capnp")]
    fn serialize_declog_capnp(builder: febft_capnp::cst_messages_capnp::dec_log::Builder, msg: &Self::DecLog) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_declog_capnp(reader: febft_capnp::cst_messages_capnp::dec_log::Reader) -> Result<Self::DecLog>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_view_capnp(builder: febft_capnp::cst_messages_capnp::view_info::Builder, msg: &Self::ViewInfo) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_view_capnp(reader: febft_capnp::cst_messages_capnp::view_info::Reader) -> Result<Self::ViewInfo>;


}

/// The result of processing a message in the state transfer protocol
pub enum STResult {
    CstNotNeeded,
    CstRunning,
    CstFinished,
}