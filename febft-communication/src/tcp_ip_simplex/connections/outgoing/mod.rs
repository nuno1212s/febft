
use std::sync::Arc;
use febft_common::socket::SecureSocketAsync;
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::PeerConnection;
use crate::tcpip::connections::ConnHandle;

mod asynchronous;

pub(super) fn spawn_outgoing_task_handler<M: Serializable>(
    conn_handle: ConnHandle,
    connection: Arc<PeerConnection<M>>,
    socket: SecureSocketAsync) {
    match socket {
        SecureSocketAsync::Async(asynchronous) => {
            asynchronous::spawn_outgoing_task(conn_handle, connection, asynchronous);
        }
        SecureSocketAsync::Sync(synchronous) => {
            //synchronous::spawn_outgoing_thread(conn_handle, connection, synchronous);
        }
    }
}