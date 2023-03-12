use febft_common::ordering::SeqNo;
use febft_communication::NodeId;
use febft_execution::serialize::SharedData;
use febft_messages::messages::{RequestMessage, SystemMessage};
use crate::bft::SysMsg;
use super::{ClientType, Client};

pub struct Ordered;

impl<D> ClientType<D> for Ordered where D: SharedData + 'static {
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> SysMsg<D> {
        SystemMessage::OrderedRequest(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item=NodeId>;

    fn init_targets(client: &Client<D>) -> (Self::Iter, usize) {
        (NodeId::targets(0..client.params.n()), client.params.n())
    }

    fn needed_responses(client: &Client<D>) -> usize {
        client.params.f() + 1
    }
}