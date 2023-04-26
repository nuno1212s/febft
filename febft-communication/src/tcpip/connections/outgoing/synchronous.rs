use std::sync::Arc;
use log::error;

use febft_common::channel::ChannelMixedRx;
use febft_common::socket::SecureWriteHalfSync;
use crate::serialize::Serializable;

use crate::tcpip::connections::{ConnHandle, PeerConnection, SerializedMessage};

pub(super) fn spawn_outgoing_thread<M: Serializable>(
    conn_handle: ConnHandle,
    mut peer: Arc<PeerConnection<M>>,
    mut socket: SecureWriteHalfSync) {
    std::thread::Builder::new()
        .name(format!("Outgoing connection thread"))
        .spawn(move || {
            let rx = peer.to_send_handle().clone();

            loop {
                let (to_send, callback) = match rx.recv() {
                    Ok(message) => { message }
                    Err(error_kind) => {
                        error!("Failed to receive message to send. {:?}", error_kind);

                        break;
                    }
                };

                if conn_handle.is_cancelled() {
                    peer.peer_message(to_send, callback).unwrap();

                    return;
                }

                match to_send.write_to_sync(&mut socket, true) {
                    Ok(_) => {
                        //TODO: Statistics

                        if let Some(callback) = callback {
                            callback(true);
                        }

                    }
                    Err(error_kind) => {
                        error!("Failed to write message to socket. {:?}", error_kind);

                        peer.peer_message(to_send, callback).unwrap();

                        break;
                    }
                }
            }

            peer.delete_connection(conn_handle.id());
        }).unwrap();
}