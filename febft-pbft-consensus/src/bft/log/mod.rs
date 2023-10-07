use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_communication::message::Header;
use atlas_core::messages::RequestMessage;
use atlas_smr_application::serialize::ApplicationData;
use crate::bft::log::decisions::DecisionLog;

pub mod decided;
pub mod deciding;
pub mod decisions;

pub struct Log<D> where D: ApplicationData {
    decided: DecisionLog<D::Request>,
}

impl<D> Log<D> where D: ApplicationData {

    pub fn decision_log(&self) -> &DecisionLog<D::Request> {
        &self.decided
    }

}

#[inline]
pub fn operation_key<O>(header: &Header, message: &RequestMessage<O>) -> u64 {
    operation_key_raw(header.from(), message.session_id())
}

#[inline]
pub fn operation_key_raw(from: NodeId, session: SeqNo) -> u64 {
    // both of these values are 32-bit in width
    let client_id: u64 = from.into();
    let session_id: u64 = session.into();

    // therefore this is safe, and will not delete any bits
    client_id | (session_id << 32)
}
