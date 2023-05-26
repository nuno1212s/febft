use std::sync::Arc;
use log::error;

use febft_common::channel::ChannelMixedRx;
use febft_common::socket::SecureWriteHalfSync;
use febft_metrics::metrics::metric_duration;
use crate::metric::COMM_REQUEST_SEND_TIME_ID;
use crate::serialize::Serializable;

use crate::tcpip::connections::{ConnHandle, PeerConnection, NetworkSerializedMessage};

pub(super) fn spawn_outgoing_thread<M: Serializable>(
    conn_handle: ConnHandle,
    mut peer: Arc<PeerConnection<M>>,
    mut socket: SecureWriteHalfSync) {
    std::thread::Builder::new()
        .name(format!("Outgoing connection thread"))
        .spawn(move || {
            let rx = peer.to_send_handle().clone();

            loop {
                let (to_send, callback, dispatch_time, flush, send_rq_time) = match rx.recv() {
                    Ok(message) => { message }
                    Err(error_kind) => {
                        error!("Failed to receive message to send. {:?}", error_kind);

                        break;
                    }
                };

                if conn_handle.is_cancelled() {
                    peer.peer_message(to_send, callback, flush, send_rq_time).unwrap();

                    return;
                }

                match to_send.write_to_sync(&mut socket, true) {
                    Ok(_) => {
                        metric_duration(COMM_REQUEST_SEND_TIME_ID, dispatch_time.elapsed());

                        if let Some(callback) = callback {
                            callback(true);
                        }

                    }
                    Err(error_kind) => {
                        error!("Failed to write message to socket. {:?}", error_kind);

                        peer.peer_message(to_send, callback, flush, send_rq_time).unwrap();

                        break;
                    }
                }
            }

            peer.delete_connection(conn_handle.id());
        }).unwrap();
}