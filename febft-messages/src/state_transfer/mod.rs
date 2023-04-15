use std::sync::Arc;
use febft_execution::serialize::SharedData;
use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::StoredMessage;
use febft_communication::Node;
use crate::messages::{Protocol, StateTransfer};
use crate::ordering_protocol::{OrderingProtocol, View};
use crate::serialize::{NetworkView, OrderingProtocolMessage, ServiceMsg, StatefulOrderProtocolMessage, StateTransferMessage};
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
pub enum STResult<D: SharedData> {
    CstNotNeeded,
    CstRunning,
    CstFinished(D::State, Vec<D::Request>),
}

pub type CstM<M: StateTransferMessage> = <M as StateTransferMessage>::StateTransferMessage;

/// A trait for the implementation of the state transfer protocol
pub trait StateTransferProtocol<D, OP, NT> where
    D: SharedData + 'static,
    OP: StatefulOrderProtocol<D, NT> + 'static {
    type Serialization: StateTransferMessage + 'static;

    type Config;

    /// Initialize the state transferring protocol with the given configuration, timeouts and communication layer
    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>) -> Result<Self>
        where Self: Sized;

    /// Request the latest state from the rest of replicas
    fn request_latest_state(&mut self,
                            order_protocol: &mut OP) -> Result<()>
        where NT: Node<ServiceMsg<D, OP::Serialization, Self::Serialization>>;

    /// Handle a state transfer protocol message that was received while executing the ordering protocol
    fn handle_off_ctx_message(&mut self,
                              order_protocol: &mut OP,
                              message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>)
                              -> Result<()>
        where NT: Node<ServiceMsg<D, OP::Serialization, Self::Serialization>>;

    /// Process a state transfer protocol message
    fn process_message(&mut self,
                       order_protocol: &mut OP,
                       message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>)
                       -> Result<STResult<D>>
        where NT: Node<ServiceMsg<D, OP::Serialization, Self::Serialization>>;

    /// Handle having received a state from the application
    fn handle_state_received_from_app(&mut self,
                                      order_protocol: &mut OP,
                                      state: Arc<ReadOnly<Checkpoint<D::State>>>) -> Result<()>
        where NT: Node<ServiceMsg<D, OP::Serialization, Self::Serialization>>;
}

pub type DecLog<OP> = <OP as StatefulOrderProtocolMessage>::DecLog;

/// An order protocol that uses the state transfer protocol to manage its state.
pub trait StatefulOrderProtocol<D: SharedData + 'static, NT>: OrderingProtocol<D, NT> {
    /// The serialization abstraction for
    type StateSerialization: StatefulOrderProtocolMessage + 'static;

    fn view(&self) -> View<Self::Serialization>;

    /// Install a state received from other replicas in the system
    fn install_state(&mut self, state: Arc<ReadOnly<Checkpoint<D::State>>>,
                     view_info: View<Self::Serialization>,
                     dec_log: DecLog<Self::StateSerialization>) -> Result<(D::State, Vec<D::Request>)>;

    /// Install a given sequence number
    fn install_seq_no(&mut self, seq_no: SeqNo) -> Result<()>;

    /// Snapshot the current log of the replica
    fn snapshot_log(&mut self) -> Result<(Arc<ReadOnly<Checkpoint<D::State>>>,
                                          View<Self::Serialization>,
                                          DecLog<Self::StateSerialization>)>;

    /// Finalize the checkpoint of the replica
    fn finalize_checkpoint(&mut self, state: Arc<ReadOnly<Checkpoint<D::State>>>) -> Result<()>;
}
