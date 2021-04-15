//! Contains the client side core protocol logic of `febft`.

//use super::SystemParams;
//use crate::bft::collections::{self, HashMap};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::{
    Node,
    SendNode,
};

pub struct Client<D: SharedData> {
    node: SendNode<D>,
}

impl<D> Client<D>
where
    D: SharedData + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
    async fn message_recv_task(mut node: Node<D>) {
        drop(node);
        unimplemented!()
    }
}
