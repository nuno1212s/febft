use febft_common::ordering::SeqNo;
use febft_communication::NodeId;
use crate::bft::executable::{Reply, Request, State};
use crate::bft::message::serialize::SharedData;
use crate::bft::message::{RequestMessage, SystemMessage};
use super::{ClientType, Client};

pub struct Ordered;

impl<D> ClientType<D> for Ordered where D: SharedData + 'static {
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> SystemMessage<D::State, D::Request, D::Reply> {
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