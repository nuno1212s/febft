use std::sync::Arc;
use febft_common::channel;
use febft_common::socket::{SecureSocket, SecureSocketAsync};
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::PeerConnection;
use crate::tcp_ip_simplex::connections::ping_handler::PingHandler;
use crate::tcpip::connections::ConnHandle;

mod asynchronous;


pub(super) fn spawn_outgoing_task_handler<M: Serializable>(
    conn_handle: ConnHandle,
    connection: Arc<PeerConnection<M>>,
    ping: Arc<PingHandler>,
    socket: SecureSocket) {
    let rx = ping.register_ping_channel(connection.peer_node_id, conn_handle.id());

    match socket {
        SecureSocket::Async(asynchronous) => {
            asynchronous::spawn_outgoing_task(conn_handle, ping, rx, connection, asynchronous);
        }
        SecureSocket::Sync(synchronous) => {
            //synchronous::spawn_outgoing_thread(conn_handle, connection, synchronous);
        }
    }
}