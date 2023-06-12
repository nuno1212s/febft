#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::time::Instant;
use atlas_execution::serialize::SharedData;
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{StatefulOrderProtocol, StateTransferProtocol};
use crate::CollabStateTransfer;
use crate::message::CstMessage;

pub struct CSTMsg<D: SharedData, OP: OrderingProtocolMessage, SOP: StatefulOrderProtocolMessage>(PhantomData<(D, OP, SOP)>);

impl<D: SharedData, OP: OrderingProtocolMessage, SOP: StatefulOrderProtocolMessage> StateTransferMessage for CSTMsg<D, OP, SOP> {
    type StateTransferMessage = CstMessage<D::State, OP::ViewInfo, SOP::DecLog, OP::Proof>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: atlas_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> atlas_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: atlas_capnp::cst_messages_capnp::cst_message::Reader) -> atlas_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }
}