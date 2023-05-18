mod asynchronous;

use std::sync::Arc;
use febft_common::socket::SecureSocket;
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::PeerConnection;
use crate::tcpip::connections::ConnHandle;

pub(super) fn spawn_incoming_task_handler<M: Serializable>(
    conn_handle: ConnHandle,
    connected_peer: Arc<PeerConnection<M>>,
    socket: SecureSocket) {

    match socket {
        SecureSocket::Async(asynchronous) => {
            asynchronous::spawn_incoming_task(conn_handle, connected_peer, asynchronous);
        }
        SecureSocket::Sync(synchronous) => {
            todo!()
            //synchronous::spawn_incoming_thread(conn_handle, connected_peer, synchronous);
        }
    }
}