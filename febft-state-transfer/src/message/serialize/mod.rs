#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::time::Instant;
use febft_execution::serialize::SharedData;
use febft_messages::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use febft_messages::state_transfer::{StatefulOrderProtocol, StateTransferProtocol};
use crate::CollabStateTransfer;
use crate::message::CstMessage;

pub struct CSTMsg<D: SharedData, OP: OrderingProtocolMessage, SOP: StatefulOrderProtocolMessage>(PhantomData<(D, OP, SOP)>);

impl<D: SharedData, OP: OrderingProtocolMessage, SOP: StatefulOrderProtocolMessage> StateTransferMessage for CSTMsg<D, OP, SOP> {
    type StateTransferMessage = CstMessage<D::State, OP::ViewInfo, SOP::DecLog, SOP::Proof>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> febft_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::cst_messages_capnp::cst_message::Reader) -> febft_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }
}