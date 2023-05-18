use std::sync::Arc;
use std::time::Instant;
use bytes::BytesMut;

use futures::{AsyncReadExt, AsyncWriteExt, select};
use futures::FutureExt;
use log::{error, warn};

use febft_common::async_runtime as rt;
use febft_common::channel::{ChannelMixedRx, OneShotTx};
use febft_common::error::*;
use febft_common::socket::SecureSocketAsync;
use febft_metrics::metrics::metric_duration;
use crate::cpu_workers;
use crate::cpu_workers::{serialize_digest_no_threadpool, serialize_digest_threadpool_return_msg};
use crate::message::{Header, NetworkMessageKind, PingMessage, WireMessage};

use crate::metric::{COMM_REQUEST_SEND_TIME_ID, COMM_RQ_SEND_CLI_PASSING_TIME_ID, COMM_RQ_SEND_PASSING_TIME_ID, COMM_RQ_TIME_SPENT_IN_MOD_ID};
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::{ConnectionDirection, PeerConnection};
use crate::tcp_ip_simplex::connections::ping_handler::{PingChannelReceiver, PingHandler};
use crate::tcpip::connections::{ConnHandle, NetworkSerializedMessage};

pub(super) fn spawn_outgoing_task<M: Serializable + 'static>(
    conn_handle: ConnHandle,
    ping_handler: Arc<PingHandler>,
    mut ping_orders: PingChannelReceiver,
    peer: Arc<PeerConnection<M>>,
    mut socket: SecureSocketAsync) {
    rt::spawn(async move {
        let mut rx = peer.to_send_handle().clone();

        loop {
            let to_send = select! {
                to_send = rx.recv_async().fuse() => {
                    match to_send {
                        Ok(message) => { message }
                        Err(error_kind) => {
                            error!("{:?} // Failed to receive message to send. {:?}", conn_handle.my_id(), error_kind);

                            break;
                        }
                    }
                },
                ping_rq = ping_orders.recv_async().fuse() => {
                    if let Ok(ping_rq) = ping_rq {
                        if let Err(err) = make_ping_rq(&peer, &mut socket, &conn_handle).await {
                            error!("{:?} // Failed to send ping request to peer {:?} through connection {}. {:?}", 
                                conn_handle.my_id(), peer.peer_node_id, conn_handle.id(), err);

                            ping_handler.handle_ping_failed(peer.peer_node_id, conn_handle.id());
                            // Make us delete this connection by breaking out of the loop
                            break;
                        }
                        
                        ping_handler.handle_ping_received(peer.peer_node_id, conn_handle.id());
                        continue;
                    } else {
                        break;
                    }
                }
            };

            let dispatch_time = &to_send.2;

            if peer.peer_node_id.id() < 1000 {
                metric_duration(COMM_RQ_SEND_PASSING_TIME_ID, dispatch_time.elapsed());
            } else {
                metric_duration(COMM_RQ_SEND_CLI_PASSING_TIME_ID, dispatch_time.elapsed());
            }

            // If the connection has received an error, disconnect this TX part
            // (As it might have been stuck waiting for a message, and now it
            // would just get an error while trying to write)
            if conn_handle.is_cancelled() {
                warn!("{:?} // Conn {} has been cancelled, returning message to queue", conn_handle.my_id(), conn_handle.id());

                // Put the taken request back into the send queue
                if let Err(err) = peer.peer_msg_return_async(to_send).await {
                    error!("{:?} // Failed to return message because {:?}",conn_handle.my_id(), err);
                }

                // Return as we don't want to call delete connection again
                return;
            }

            if let Err(_) = send_message(&peer, &mut socket, &conn_handle, to_send, false).await {
                break;
            }

            // Attempt to send all pending messages from the queue in order to avoid doing many sys calls
            while let Ok(to_send) = rx.try_recv() {
                if let Err(_) = send_message(&peer, &mut socket, &conn_handle, to_send, false).await {
                    break;
                }
            }

            // Only flush when there are no more messages to send
            if let Err(_) = socket.flush().await {
                break;
            }
        }

        peer.delete_connection(conn_handle.id(), ConnectionDirection::Outgoing);
        ping_handler.remove_ping_channel(peer.peer_node_id, conn_handle.id());
    });
}

async fn send_message<M: Serializable + 'static>(peer: &Arc<PeerConnection<M>>,
                                                 socket: &mut SecureSocketAsync,
                                                 conn_handle: &ConnHandle,
                                                 to_send: NetworkSerializedMessage,
                                                 flush: bool) -> Result<()> {
    let start = Instant::now();

    let (to_send, callback, dispatch_time, _, send_rq_time) = to_send;

    match to_send.write_to(socket, flush).await {
        Ok(_) => {
            if let Some(callback) = callback {
                callback(true);
            }

            metric_duration(COMM_REQUEST_SEND_TIME_ID, start.elapsed());

            metric_duration(COMM_RQ_TIME_SPENT_IN_MOD_ID, send_rq_time.elapsed());

            Ok(())
        }
        Err(error_kind) => {
            error!("{:?} // Failed to write message to socket. {:?}", conn_handle.my_id(), error_kind);

            // Put the taken request back into the send queue
            if let Err(err) = peer.peer_msg_return_async((to_send, callback, dispatch_time, flush, send_rq_time)).await {
                error!("{:?} // Failed to return message because {:?}", conn_handle.my_id(), err);
            }

            Err(Error::simple(ErrorKind::Communication))
        }
    }
}

/// Make a ping request
async fn make_ping_rq<M: Serializable + 'static>(peer: &Arc<PeerConnection<M>>,
                                                 mut socket: &mut SecureSocketAsync,
                                                 conn_handle: &ConnHandle) -> Result<()> {
    let ping = NetworkMessageKind::<M>::Ping(PingMessage::new(true));

    let (_, result) = serialize_digest_threadpool_return_msg::<M>(ping).await.wrapped(ErrorKind::CommunicationPingHandler)?;

    let (payload, digest) = result?;

    let nonce = fastrand::u64(..);

    let wm = WireMessage::new(conn_handle.my_id(), peer.peer_node_id, payload, nonce, Some(digest), None);

    wm.write_to(&mut socket, true).await?;

    let mut read_buffer = BytesMut::with_capacity(Header::LENGTH);

    read_buffer.resize(Header::LENGTH, 0);

    socket.read_exact(&mut read_buffer[..Header::LENGTH]).await?;

    // we are passing the correct length, safe to use unwrap()
    let header = Header::deserialize_from(&read_buffer[..Header::LENGTH]).unwrap();

    read_buffer.clear();
    read_buffer.reserve(header.payload_length());
    read_buffer.resize(header.payload_length(), 0);

    // read the peer's payload
    socket.read_exact(&mut read_buffer[..header.payload_length()]).await?;

    // Use the threadpool for CPU intensive work in order to not block the IO threads
    let (pong, payload) = cpu_workers::deserialize_message::<M>(header.clone(),
                                                                read_buffer).await.unwrap()?;

    return match pong {
        NetworkMessageKind::Ping(ping) => {
            if !ping.is_request() {
                Ok(())
            } else {
                Err(Error::simple(ErrorKind::CommunicationPingHandler))
            }
        }
        _ => { Err(Error::simple(ErrorKind::CommunicationPingHandler)) }
    };
}