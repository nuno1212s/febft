use std::sync::Arc;
use log::{debug, error, info};

use febft_common::async_runtime as rt;
use febft_common::socket::SecureWriteHalfAsync;
use crate::serialize::Serializable;

use crate::tcpip::connections::{ConnHandle, PeerConnection, SerializedMessage};

pub (super) fn spawn_outgoing_task<M: Serializable + 'static>(
    conn_handle: ConnHandle,
    peer: Arc<PeerConnection<M>>,
    mut socket: SecureWriteHalfAsync) {

    rt::spawn(async move {
        let mut rx = peer.to_send_handle().clone();

        loop {
            let (to_send, callback) = match rx.recv_async().await {
                Ok(message) => {message}
                Err(error_kind) => {
                    error!("Failed to receive message to send. {:?}", error_kind);

                    break;
                }
            };

            // If the connection has received an error, disconnect this TX part
            // (As it might have been stuck waiting for a message, and now it
            // would just get an error while trying to write)
            if conn_handle.is_cancelled() {

                // Put the taken request back into the send queue
                if let Err(err) = peer.peer_msg_return_async(to_send, callback).await {
                    error!("Failed to return message because {:?}", err);
                }

                // Return as we don't want to call delete connection again
                return
            }

            debug!("{:?} // Sending message to peer {:?}", to_send.header().from(), to_send.header().to());

            match to_send.write_to(&mut socket, true).await {
                Ok(_) => {
                    //TODO: Statistics

                    if let Some(callback) = callback {
                        callback(true);
                    }
                }
                Err(error_kind) => {
                    error!("Failed to write message to socket. {:?}", error_kind);

                    // Put the taken request back into the send queue
                    if let Err(err) = peer.peer_msg_return_async(to_send, callback).await {
                        error!("Failed to return message because {:?}", err);
                    }

                    break
                }
            }
        }

        peer.delete_connection(conn_handle.id());
    });
}

