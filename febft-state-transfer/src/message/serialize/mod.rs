#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::time::Instant;
use atlas_execution::serialize::ApplicationData;
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{StateTransferProtocol};
use atlas_execution::state::monolithic_state::MonolithicState;
use crate::CollabStateTransfer;
use crate::message::CstMessage;

pub struct CSTMsg<S: MonolithicState>(PhantomData<(S)>);

impl<S: MonolithicState> StateTransferMessage for CSTMsg<S> {

    type StateTransferMessage = CstMessage<S>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: atlas_capnp::cst_messages_capnp::cst_message::Builder, msg: &Self::StateTransferMessage) -> atlas_common::error::Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: atlas_capnp::cst_messages_capnp::cst_message::Reader) -> atlas_common::error::Result<Self::StateTransferMessage> {
        todo!()
    }
}