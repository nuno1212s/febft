use atlas_smr_application::serialize::ApplicationData;
use atlas_common::error::*;
use atlas_smr_application::state::divisible_state::DivisibleState;
use crate::message::CstMessage;

fn serialize_state_transfer<S>(mut state_transfer: atlas_capnp::cst_messages_capnp::cst_message::Builder,
                               msg: &CstMessage<S>) -> Result<()>
    where S: DivisibleState {
    Ok(())
}

fn deserialize_state_transfer<S>(state_transfer: atlas_capnp::cst_messages_capnp::cst_message::Reader)
                                 -> Result<CstMessage<S>>
    where S: DivisibleState {
    Err(Error::simple(ErrorKind::CommunicationSerialize))
}
