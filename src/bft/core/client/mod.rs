//! Contains the client side core protocol logic of `febft`.

use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use std::task::{Poll, Waker, Context};

use parking_lot::Mutex;

use super::SystemParams;

use crate::bft::crypto::signature::Signature;
use crate::bft::collections::{self, HashMap};
use crate::bft::communication::serialize::SharedData;
use crate::bft::communication::message::{
    Message,
    SystemMessage,
};
use crate::bft::communication::{
    Node,
    NodeId,
    SendNode,
};

struct ClientData<P> {
    wakers: Mutex<HashMap<Signature, Waker>>,
    ready: Mutex<HashMap<Signature, P>>,
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<D: SharedData> {
    params: SystemParams,
    node: SendNode<D>,
    data: Arc<ClientData<D::Reply>>,
}

impl<D: SharedData> Clone for Client<D> {
    fn clone(&self) -> Self {
        Self {
            params: self.params,
            node: self.node.clone(),
            data: Arc::clone(&self.data),
        }
    }
}

struct ClientRequestFut<'a, P> {
    signature: Signature,
    data: &'a ClientData<P>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = P;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<P> {
        // check if response is ready
        {
            let mut ready = self.data.ready.lock();
            if let Some(payload) = ready.remove(&self.signature) {
                // FIXME: should you remove wakers here?
                return Poll::Ready(payload);
            }
        }
        // clone waker to wake up this task when
        // the response is ready
        {
            let mut wakers = self.data.wakers.lock();
            wakers.insert(self.signature, cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<D> Client<D>
where
    D: SharedData + 'static,
    D::Request: Send + 'static,
    D::Reply: Send + 'static,
{
//    /// Updates the replicated state of the application running
//    /// on top of `febft`.
//    pub async fn update(&self, operation: D::Request) -> D::Reply {
//        // broadcast our request to the node group
//        let message = SystemMessage::Request(RequestMessage::new(
//            operation,
//        ));
//        let targets = NodeId::targets(0..self.params.n());
//        self.node.broadcast(operation
//    }

    async fn message_recv_task(
        params: SystemParams,
        data: Arc<ClientData<D::Reply>>,
        mut node: Node<D>,
    ) {
        let mut count: HashMap<Signature, usize> = collections::hash_map();
        while let Ok(message) = node.receive().await {
            match message {
                Message::System(_, message) => {
                    match message {
                        SystemMessage::Reply(message) => {
                            let (signature, payload) = message.into_inner();
                            let q = count.entry(signature).or_insert(0);

                            // register new reply received
                            *q += 1;

                            // TODO: check if we got equivalent responses by
                            // verifying the signature

                            if *q == params.quorum() {
                                // remove this counter
                                count.remove(&signature);

                                // register response
                                {
                                    let mut ready = data.ready.lock();
                                    ready.insert(signature, payload);
                                }

                                // try to wake up a waiting task
                                {
                                    let mut wakers = data.wakers.lock();
                                    if let Some(waker) = wakers.remove(&signature) {
                                        waker.wake();
                                    }
                                }
                            }
                        },
                        // FIXME: handle rogue messages on clients
                        _ => panic!("rogue message detected"),
                    }
                },
                Message::ConnectedTx(id, sock) => node.handle_connected_tx(id, sock),
                Message::ConnectedRx(id, sock) => node.handle_connected_rx(id, sock),
                // TODO: node disconnected on send side
                Message::DisconnectedTx(_id) => unimplemented!(),
                // TODO: node disconnected on receive side
                Message::DisconnectedRx(_some_id) => unimplemented!(),
                // we don't receive any other type of messages as a client node
                _ => (),
            }
        }
    }
}
