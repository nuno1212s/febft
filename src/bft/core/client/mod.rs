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
    SendNode,
};

struct ClientData<P> {
    wakers: Mutex<HashMap<Signature, Waker>>,
    ready: Mutex<HashMap<Signature, P>>,
}

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

pub struct ClientRequestFut<'a, P> {
    signature: Signature,
    data: &'a ClientData<P>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = P;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<P> {
        // check if response is ready
        {
            let mut ready = self.data.ready.lock();
            if let Some(payload) = ready.remove(&self.signature) {
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
    async fn message_recv_task(
        map: Arc<ClientData<D::Reply>>,
        mut node: Node<D>,
    ) {
        let mut count: HashMap<Signature, i32> = collections::hash_map();
        while let Ok(message) = node.receive().await {
            match message {
                Message::System(header, message) => {
                    match message {
                        SystemMessage::Reply(message) => {
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
