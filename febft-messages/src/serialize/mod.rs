use std::marker::PhantomData;
use febft_common::error::*;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use crate::messages::SystemMessage;
#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};
use febft_common::node_id::NodeId;
use febft_common::ordering::Orderable;
use crate::state_transfer::StateTransferProtocol;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;


/// The basic methods needed for a view
pub trait NetworkView: Orderable {
    fn primary(&self) -> NodeId;

    fn quorum(&self) -> usize;

    fn f(&self) -> usize;

    fn n(&self) -> usize;
}

/// We do not need a serde module since serde serialization is just done on the network level.
/// The abstraction for ordering protocol messages.
pub trait OrderingProtocolMessage: Send {

    #[cfg(feature = "serialize_capnp")]
    type ViewInfo: NetworkView + Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type ViewInfo: NetworkView + for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    type ProtocolMessage: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type ProtocolMessage: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::consensus_messages_capnp::protocol_message::Builder, msg: &Self::ProtocolMessage) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::consensus_messages_capnp::protocol_message::Reader) -> Result<Self::ProtocolMessage>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_view_capnp(builder: febft_capnp::cst_messages_capnp::view_info::Builder, msg: &Self::ViewInfo) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_view_capnp(reader: febft_capnp::cst_messages_capnp::view_info::Reader) -> Result<Self::ViewInfo>;
}

/// The abstraction for state transfer protocol messages.
/// This allows us to have any state transfer protocol work with the same backbone
pub trait StateTransferMessage: Send {
    #[cfg(feature = "serialize_capnp")]
    type StateTransferMessage: Send + Clone;

    #[cfg(feature = "serialize_serde")]
    type StateTransferMessage: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> Result<()>;

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::cst_messages_capnp::cst_message::Reader) -> Result<Self::StateTransferMessage>;
}

/// The type that encapsulates all the serializing, so we don't have to constantly use SystemMessage
pub struct ServiceMsg<D: SharedData, P: OrderingProtocolMessage, S: StateTransferMessage>(PhantomData<D>, PhantomData<P>, PhantomData<S>);

impl<D: SharedData, P: OrderingProtocolMessage, S: StateTransferMessage> Serializable for ServiceMsg<D, P, S> {
    type Message = SystemMessage<D, P::ProtocolMessage, S::StateTransferMessage>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::messages_capnp::system::Builder, msg: &Self::Message) -> Result<()> {
        capnp::serialize_message::<D, P, S>(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::messages_capnp::system::Reader) -> Result<Self::Message> {
        capnp::deserialize_message::<D, P, S>(reader)
    }
}