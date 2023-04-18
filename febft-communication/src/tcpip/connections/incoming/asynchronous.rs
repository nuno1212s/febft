use std::sync::Arc;

use bytes::BytesMut;
use either::Either;
use futures::AsyncReadExt;
use log::{debug, error, info};

use febft_common::async_runtime as rt;
use febft_common::socket::SecureReadHalfAsync;

use crate::client_pooling::ConnectedPeer;
use crate::cpu_workers;
use crate::message::{Header, NetworkMessage};
use crate::serialize::Serializable;
use crate::tcpip::connections::{ConnHandle, PeerConnection};

pub(super) fn spawn_incoming_task<M: Serializable + 'static>(
    conn_handle: ConnHandle,
    peer: Arc<PeerConnection<M>>,
    mut socket: SecureReadHalfAsync) {
    rt::spawn(async move {
        let client_pool_buffer = Arc::clone(peer.client_pool_peer());
        let mut read_buffer = BytesMut::with_capacity(Header::LENGTH);
        let peer_id = client_pool_buffer.client_id().clone();

        loop {
            read_buffer.resize(Header::LENGTH, 0);

            if let Err(err) = socket.read_exact(&mut read_buffer[..Header::LENGTH]).await {
                // errors reading -> faulty connection;
                // drop this socket
                error!("{:?} // Failed to read header from socket, faulty connection {:?}", conn_handle.my_id, err);
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&read_buffer[..Header::LENGTH]).unwrap();

            // reserve space for message
            //
            //FIXME: add a max bound on the message payload length;
            // if the length is exceeded, reject connection;
            // the bound can be application defined, i.e.
            // returned by `SharedData`
            read_buffer.clear();
            read_buffer.reserve(header.payload_length());
            read_buffer.resize(header.payload_length(), 0);

            // read the peer's payload
            if let Err(err) = socket.read_exact(&mut read_buffer[..header.payload_length()]).await {
                // errors reading -> faulty connection;
                // drop this socket
                error!("{:?} // Failed to read payload from socket, faulty connection {:?}", conn_handle.my_id, err);
                break;
            }

            if conn_handle.my_id.0 < 1000 && peer_id.0 < 1000 {
                debug!("{:?} // Received message from peer {:?} with payload {}",
                    conn_handle.my_id, peer_id, header.payload_length());
            }

            // Use the threadpool for CPU intensive work in order to not block the IO threads
            let result = cpu_workers::deserialize_message(header.clone(),
                                                          read_buffer).await.unwrap();

            let message = match result {
                Ok((message, bytes)) => {
                    read_buffer = bytes;

                    message
                }
                Err(err) => {
                    // errors deserializing -> faulty connection;
                    // drop this socket
                    error!("{:?} // Failed to deserialize message {:?}", conn_handle.my_id,err);
                    break;
                }
            };

            let msg = NetworkMessage::new(header, message);

            if let Err(inner) = client_pool_buffer.push_request(msg) {
                error!("{:?} // Channel closed, closing tcp connection as well to peer {:?}. {:?}",
                    conn_handle.my_id,
                    peer_id,
                    inner,
                );

                break;
            };

            //TODO: Statistics
        }

        peer.delete_connection(conn_handle.id());
    });
}
