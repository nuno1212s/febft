use crate::bft::{communication::{serialize::SharedData, message::{SystemMessage, RequestMessage}, NodeId}, ordering::SeqNo};

use super::{ClientType, Client};

pub struct Ordered;

impl<D> ClientType<D> for Ordered where D: SharedData + 'static {
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: <D as SharedData>::Request,
    ) -> SystemMessage<<D as SharedData>::State, <D as SharedData>::Request, <D as SharedData>::Reply> {
        SystemMessage::Request(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item = NodeId>;

    fn init_targets(client: &Client<D>) -> (Self::Iter, usize) {
        (NodeId::targets(0..client.params.n()), client.params.n())
    }

    fn needed_responses(client: &Client<D>) -> usize {
        client.params.f() + 1
    }

}