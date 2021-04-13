//! Contains the client side core protocol logic of `febft`.

//use std::sync::Arc;
//
//use futures::lock::Mutex;
//
//use super::SystemParams;
//use crate::bft::communication::Node;
//use crate::bft::collections::{self, HashMap};
//use crate::bft::communication::serialize::SharedData;
//
//pub struct Client<D: SharedData> {
//    node: Arc<Node<D>>,
//}
//
//impl<D> Client<D>
//where
//    D: SharedData + 'static,
//    D::Request: Send + 'static,
//    D::Reply: Send + 'static,
//{
//    /// asd.
//    async fn message_recv_task() -> Result<()> {
//    }
//}
