use febft_common::ordering::SeqNo;
use febft_communication::NodeId;
use febft_execution::executable::{Request, Service};
use febft_messages::messages::{RequestMessage, SystemMessage};
use crate::NoProtocol;
use super::{ClientType, Client};

pub struct Ordered;

impl<S> ClientType<S> for Ordered where S: Service + 'static {
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: Request<S>,
    ) -> SystemMessage<S, NoProtocol> {
        SystemMessage::OrderedRequest(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item = NodeId>;

    fn init_targets(client: &Client<S>) -> (Self::Iter, usize) {
        (NodeId::targets(0..client.params.n()), client.params.n())
    }

    fn needed_responses(client: &Client<S>) -> usize {
        client.params.f() + 1
    }

}