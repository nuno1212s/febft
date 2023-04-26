use std::sync::Arc;
use febft_common::channel::ChannelMixedRx;
use febft_common::socket::{SecureWriteHalf, SecureWriteHalfSync};
use crate::serialize::Serializable;

use crate::tcpip::connections::{ConnHandle, PeerConnection, SerializedMessage};

pub mod asynchronous;
pub mod synchronous;

pub(super) fn spawn_outgoing_task_handler<M: Serializable>(
    conn_handle: ConnHandle,
    connection: Arc<PeerConnection<M>>,
    socket: SecureWriteHalf) {
    match socket {
        SecureWriteHalf::Async(asynchronous) => {
            asynchronous::spawn_outgoing_task(conn_handle, connection, asynchronous);
        }
        SecureWriteHalf::Sync(synchronous) => {
            synchronous::spawn_outgoing_thread(conn_handle, connection, synchronous);
        }
    }
}