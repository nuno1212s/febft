
//#[cfg(feature = "serialize_capnp")]
mod capnp;

use febft_execution::serialize::SharedData;
use febft_messages::serialize::StateTransferMessage;
use febft_messages::state_transfer::{StatefulOrderProtocol, StateTransferProtocol};
use crate::CollabStateTransfer;
use crate::message::CstMessage;

impl<D: SharedData, SOP: StatefulOrderProtocol<D>> StateTransferMessage for CollabStateTransfer<D, SOP> {

    type StateTransferMessage = CstMessage<D, SOP::DecLog, SOP::ViewInfo>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: febft_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> febft_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: febft_capnp::cst_messages_capnp::cst_message::Reader) -> febft_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }
}