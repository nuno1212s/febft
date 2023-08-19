#[cfg(feature = "serialize_capnp")]
mod capnp;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use atlas_communication::message::Header;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::Serializable;
use atlas_execution::serialize::ApplicationData;
use atlas_core::serialize::{InternallyVerifiable, OrderingProtocolMessage, StatefulOrderProtocolMessage, StateTransferMessage};
use atlas_core::state_transfer::{StateTransferProtocol};
use atlas_execution::state::monolithic_state::MonolithicState;
use crate::CollabStateTransfer;
use crate::message::CstMessage;

pub struct CSTMsg<S: MonolithicState>(PhantomData<(S)>);

impl<S: MonolithicState> InternallyVerifiable<CstMessage<S>> for CSTMsg<S> {
    fn verify_internal_message<M, SV, NI>(network_info: &Arc<NI>, header: &Header, msg: &CstMessage<S>) -> atlas_common::error::Result<bool>
        where M: Serializable,
              SV: NetworkMessageSignatureVerifier<M, NI>,
              NI: NetworkInformationProvider {
        Ok(true)
    }
}

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