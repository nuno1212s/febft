use std::io::Read;
use std::sync::Arc;
use bytes::BytesMut;
use log::error;
use febft_common::socket::{SecureReadHalfSync};
use crate::client_pooling::ConnectedPeer;
use crate::cpu_workers;
use crate::message::{Header, NetworkMessage};
use crate::serialize::Serializable;

pub(super) fn spawn_incoming_thread<M: Serializable + 'static>(peer: Arc<ConnectedPeer<NetworkMessage<M>>>,
                                                               mut socket: SecureReadHalfSync) {
    std::thread::Builder::new()
        .spawn(|| {
            loop {
                let mut read_buffer = BytesMut::with_capacity(Header::LENGTH);

                loop {
                    read_buffer.resize(Header::LENGTH, 0);

                    if let Err(err) = socket.read_exact(&mut read_buffer[..Header::LENGTH]) {
                        // errors reading -> faulty connection;
                        // drop this socket
                        error!("{:?} // Failed to read header from socket, faulty connection {:?}", self.id, err);
                        break;
                    }

                    // we are passing the correct length, safe to use unwrap()
                    let header = Header::deserialize_from(&buf[..Header::LENGTH]).unwrap();

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
                    if let Err(err) = socket.read_exact(&mut read_buffer[..header.payload_length()]) {
                        // errors reading -> faulty connection;
                        // drop this socket
                        error!("{:?} // Failed to read payload from socket, faulty connection {:?}", self.id, err);
                        break;
                    }

                    // Use the threadpool for CPU intensive work in order to not block the IO threads
                    let result = cpu_workers::deserialize_message(header.clone(),
                                                                  read_buffer).recv().unwrap();

                    let message = match result {
                        Ok((message, bytes)) => {
                            read_buffer = bytes;

                            message
                        }
                        Err(err) => {
                            // errors deserializing -> faulty connection;
                            // drop this socket
                            error!("{:?} // Failed to deserialize message {:?}", self.id(), err);
                            break;
                        }
                    };

                    let msg = NetworkMessage::new(header, message);

                    if let Err(inner) = peer.push_request(msg) {
                        error!(
                            "{:?} // Channel closed, closing tcp connection as well to peer {:?}. {:?}",
                            self.id(),
                            peer_id,
                            inner);

                        break;
                    };

                    //TODO: Stats
                }
            }
        }).unwrap();
}