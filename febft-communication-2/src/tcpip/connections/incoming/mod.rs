use std::sync::Arc;
use febft_common::socket::SecureSocketRecv;
use crate::client_pooling::ConnectedPeer;
use crate::message::NetworkMessage;
use crate::serialize::Serializable;

pub mod asynchronous;
pub mod synchronous;

pub(super) fn spawn_incoming_task_handler<M: Serializable>(connected_peer: Arc<ConnectedPeer<NetworkMessage<M>>>,
                                                           socket: SecureSocketRecv) {
    match socket {
        SecureSocketRecv::Async(asynchronous) => {
            asynchronous::spawn_incoming_task(connected_peer, asynchronous);
        }
        SecureSocketRecv::Sync(synchronous) => {
            synchronous::spawn_incoming_thread(connected_peer, synchronous);
        }
    }
}