use std::sync::Arc;
use febft_execution::serialize::SharedData;
use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::StoredMessage;
use febft_communication::Node;
use crate::messages::{Protocol, StateTransfer};
use crate::ordering_protocol::OrderingProtocol;
use crate::serialize::{OrderingProtocolMessage, ServiceMsg, StateTransferMessage};
use crate::timeouts::Timeouts;
#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};


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
    pub fn new(seq: SeqNo, app_state: S) -> Arc<ReadOnly<Self>> {
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

/// The result of processing a message in the state transfer protocol
pub enum STResult {
    CstNotNeeded,
    CstRunning,
    CstFinished,
}

/// A trait for the implementation of the state transfer protocol
pub trait StateTransferProtocol<D, NT> {
    type Serialization: StateTransferMessage + 'static;

    type Config;

    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>) -> Result<Self>
        where Self: Sized;

    /// Handle a state transfer protocol message that was received while executing the ordering protocol
    fn handle_off_ctx_message<OP>(&mut self,
                                     order_protocol: &mut OP,
                                     message: StoredMessage<StateTransfer<<Self::Serialization as StateTransferMessage>::StateTransferMessage>>)
                                     -> Result<STResult>
        where D: SharedData + 'static,
              OP: StatefulOrderProtocol<D, NT>;

    /// Process a state transfer protocol message
    fn process_message<OP>(&mut self,
                              order_protocol: &mut OP,
                              message: StoredMessage<StateTransfer<<Self::Serialization as StateTransferMessage>::StateTransferMessage>>)
                              -> Result<STResult>
        where D: SharedData + 'static,
              OP: StatefulOrderProtocol<D, NT>;
}

/// The basic methods needed for a view
pub trait NetworkView {
    fn quorum(&self) -> usize;

    fn f(&self) -> usize;

    fn n(&self) -> usize;
}

/// An order protocol that uses the state transfer protocol to manage its state.
pub trait StatefulOrderProtocol<D: SharedData + 'static, NT>: OrderingProtocol<D, NT> {
    #[cfg(feature = "serialize_capnp")]
    type DecLog: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type DecLog: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    type ViewInfo: NetworkView + Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type ViewInfo: NetworkView + for<'a> Deserialize<'a> + Serialize + Send + Clone;

    fn view(&self) -> Self::ViewInfo;

    fn install_state(&mut self, state: Arc<ReadOnly<Checkpoint<D::State>>>, view_info: Self::ViewInfo, dec_log: Self::DecLog) -> Result<(D::State, Vec<D::Request>)>;

    fn snapshot_log(&mut self) -> Result<(Arc<ReadOnly<Checkpoint<D::State>>>, Self::ViewInfo, Self::DecLog)>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_declog_capnp(builder: febft_capnp::cst_messages_capnp::dec_log::Builder, msg: &Self::DecLog) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_declog_capnp(reader: febft_capnp::cst_messages_capnp::dec_log::Reader) -> Result<Self::DecLog>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_view_capnp(builder: febft_capnp::cst_messages_capnp::view_info::Builder, msg: &Self::ViewInfo) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_view_capnp(reader: febft_capnp::cst_messages_capnp::view_info::Reader) -> Result<Self::ViewInfo>;
}
