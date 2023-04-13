use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_communication::{Node};
use febft_execution::serialize::SharedData;
use febft_messages::messages::{RequestMessage, SystemMessage};
use febft_messages::serialize::ClientMessage;
use super::{ClientType, Client};

pub struct Ordered;

impl<D, NT> ClientType<D, NT> for Ordered where D: SharedData + 'static {
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> ClientMessage<D> {
        SystemMessage::OrderedRequest(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item=NodeId>;

    fn init_targets(client: &Client<D, NT>) -> (Self::Iter, usize) {
        (NodeId::targets(0..client.params.n()), client.params.n())
    }

    fn needed_responses(client: &Client<D, NT>) -> usize {
        client.params.f() + 1
    }
}