use std::sync::Arc;
use febft_common::socket::{SecureReadHalf};
use crate::client_pooling::ConnectedPeer;
use crate::message::NetworkMessage;
use crate::serialize::Serializable;
use crate::tcpip::connections::{ConnHandle, PeerConnection};

pub mod asynchronous;
pub mod synchronous;

pub(super) fn spawn_incoming_task_handler<M: Serializable>(
    conn_handle: ConnHandle,
    connected_peer: Arc<PeerConnection<M>>,
    socket: SecureReadHalf) {

    match socket {
        SecureReadHalf::Async(asynchronous) => {
            asynchronous::spawn_incoming_task(conn_handle, connected_peer, asynchronous);
        }
        SecureReadHalf::Sync(synchronous) => {
            synchronous::spawn_incoming_thread(conn_handle, connected_peer, synchronous);
        }
    }
}