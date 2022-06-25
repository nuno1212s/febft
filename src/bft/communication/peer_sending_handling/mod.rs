use std::sync::Arc;
use std::time::Instant;
use either::Either;
use log::error;
use bft::communication::message::Message;
use crate::bft;
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::NodeId;
use crate::bft::communication::NodeShared;
use crate::bft::communication::message::{Header, SerializedMessage, SystemMessage, WireMessage};
use crate::bft::communication::socket::{SecureSocketSend, SocketSendAsync, SocketSendSync};

use crate::bft::async_runtime as rt;
use crate::bft::communication::peer_receiving_handling::ConnectedPeer;
use crate::bft::communication::serialize::{Buf, DigestData, SharedData};
use crate::bft::crypto::hash::Digest;

///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 128;

pub type MessageType<D> = MessageData<D>;

pub type SendMessage<D: SharedData + 'static> = SendMessageTypes<D>;

pub enum SendMessageTypes<D> where D: SharedData + 'static {
    Serialized((Header, SerializedMessage<SystemMessage<D::State, D::Request, D::Reply>>)),
    /// The given message can be of 2 types:
    ///  We can either give the raw message, with an optional serialized buf and digest (if this was already computed)
    /// If it was not computed yet, then it will be computed by the sending thread. This message object is mandatory
    /// for when we are sending to ourselves
    ///  The other option is providing it with the already serialized message and digest, without actually providing the message
    /// object.
    Normal(Either<(SystemMessage<D::State, D::Request, D::Reply>, Option<(Buf, Digest)>), (Buf, Digest)>),
}

pub struct MessageData<D> where D: SharedData + 'static {
    message: SendMessage<D>,
    nonce: u64,
    //What time the message was instructed to be sent
    start_time: Instant,
    //Should we flush the buffer?
    flush: bool,
    //Should the message be signed?
    sign: bool,
}

pub enum Connection<D> where D: SharedData + 'static {
    Peer(SecureSocketSend),
    Me(Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>),
}

pub struct PeerConnectionInfo {
    dest_node_id: NodeId,
    own_node_id: NodeId,
    first_cli: NodeId,
    comm_stats: Option<Arc<CommStats>>,
    node_shared: Option<Arc<NodeShared>>,
}

pub enum ConnectionHandle<D> where D: SharedData + 'static {
    Sync(ChannelSyncTx<MessageType<D>>),
    Async(ChannelAsyncTx<MessageType<D>>),
}

impl<D> Clone for ConnectionHandle<D> where D: SharedData + 'static {
    fn clone(&self) -> Self {
        match self {
            ConnectionHandle::Sync(tx) => {
                ConnectionHandle::Sync(tx.clone())
            }
            ConnectionHandle::Async(tx) => {
                ConnectionHandle::Async(tx.clone())
            }
        }
    }
}

impl<D> MessageData<D> where D: SharedData + 'static {
    pub fn new(message: SendMessage<D>,
               nonce: u64, start_time: Instant, flush: bool,
               sign: bool) -> Self {
        Self { message, nonce, start_time, flush, sign }
    }
}

impl PeerConnectionInfo {
    pub fn new(dest_node_id: NodeId, own_node_id: NodeId, first_cli: NodeId,
               comm_stats: Option<Arc<CommStats>>, node_shared: Option<Arc<NodeShared>>) -> Self {
        Self {
            dest_node_id,
            own_node_id,
            first_cli,
            comm_stats,
            node_shared,
        }
    }
}

impl<D> ConnectionHandle<D> where D: SharedData + 'static {
    pub fn send(&self, message: MessageType<D>) -> Result<(), ()> {
        let channel = match self {
            ConnectionHandle::Sync(channel) => {
                channel
            }
            ConnectionHandle::Async(_) => {
                panic!("Cannot send asynchronously on synchronous channel!");
            }
        };

        match channel.send(message) {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(())
            }
        }
    }

    pub async fn async_send(&mut self, message: MessageType<D>) -> Result<(), ()> {
        let channel = match self {
            ConnectionHandle::Sync(_) => {
                panic!("Cannot send synchronously on asynchronous channel");
            }
            ConnectionHandle::Async(channel) => {
                channel
            }
        };

        match channel.send(message).await {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(())
            }
        }
    }
}

pub fn initialize_sync_loopback_thread<D>(node_info: PeerConnectionInfo,
                                          socket: Arc<ConnectedPeer<Message<D::State, D::Request, D::Reply>>>)
                                          -> ConnectionHandle<D>
    where D: SharedData + 'static {
    let (tx, rx) = bft::communication::channel::new_bounded_sync(QUEUE_SPACE);

    std::thread::Builder::new()
        .name(format!("Node {:?} loopback thread", node_info.own_node_id))
        .spawn(move || {
            sync_sending_thread(node_info, Connection::Me(socket), rx)
        }).expect(format!("Failed to start loopback thread for peer {:?}", node_info.own_node_id).as_str());

    ConnectionHandle::Sync(tx)
}

///Initialize the synchronous sending thread for the given peer
///This thread is only responsible for sending messages to that given peer
pub fn initialize_sync_sending_thread_for<D>(node_info: PeerConnectionInfo,
                                             socket: SocketSendSync, ) -> ConnectionHandle<D>
    where D: SharedData + 'static {
    let (tx, rx) = bft::communication::channel::new_bounded_sync(QUEUE_SPACE);

    let dest_id = node_info.dest_node_id;

    std::thread::Builder::new()
        .name(format!("Peer {:?} sending thread", dest_id))
        .spawn(move || {
            sync_sending_thread(node_info, Connection::Peer(SecureSocketSend::Sync(socket)), rx)
        }).expect(format!("Failed to start sending thread for peer {:?}", dest_id).as_str());

    ConnectionHandle::Sync(tx)
}

///Receives requests from the queue and sends them using the provided socket
fn sync_sending_thread<D>(peer_conn_info: PeerConnectionInfo, mut socket: Connection<D>, recv: ChannelSyncRx<MessageType<D>>)
    where D: SharedData + 'static {
    let own_node_id = peer_conn_info.own_node_id;
    let dest_node_id = peer_conn_info.dest_node_id;
    let key = peer_conn_info.node_shared.as_ref().map(|ref sh| &sh.my_key);
    let comm_stats = peer_conn_info.comm_stats;

    loop {
        let recv_result = recv.recv();

        let to_send = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_conn_info.dest_node_id);
                break;
            }
        };

        let nonce = to_send.nonce;

        let start_time = to_send.start_time;

        let flush = to_send.flush;

        let before_sending = Instant::now();

        let should_sign = to_send.sign;

        let (wm, m) = match to_send.message {
            SendMessage::Serialized((h, message)) => {
                //Serialized messages do not require calculation of signatures or digests.
                let (original, payload) = message.into_inner();

                let wire_message = WireMessage::from_parts(h, payload)
                    .expect("Given faulty Serialized message!!!!!!");

                (wire_message, Some(original))
            }
            SendMessage::Normal(to_send) => {
                match to_send {
                    Either::Left((message, serialized)) => {
                        let (buf, digest) = if let Some((buf, digest)) = serialized {
                            (buf, digest)
                        } else {
                            //If we do not have the digest and
                            let start_serialization = Instant::now();

                            let mut buf: Buf = Buf::new();

                            let digest = <D as DigestData>::serialize_digest(
                                &to_send.message,
                                &mut buf,
                            ).unwrap();

                            if let Some(comm_stats) = &comm_stats {
                                let time_taken_signing = Instant::now().duration_since(start_serialization).as_nanos();

                                //Broadcasts are always for replicas, so make this
                                comm_stats.insert_message_signing_time(NodeId::from(0u32), time_taken_signing);
                            }

                            (buf, digest)
                        };

                        //The wire message will calculate the signature and etc..
                        (WireMessage::new(
                            own_node_id,
                            dest_node_id,
                            buf,
                            nonce,
                            Some(digest),
                            if should_sign { key } else { None },
                        ), Some(message))
                    }
                    Either::Right((buf, digest)) => {
                        // create wire msg, therefore calculating signature
                        (WireMessage::new(
                            own_node_id,
                            dest_node_id,
                            buf,
                            nonce,
                            Some(digest),
                            if should_sign { key } else { None },
                        ), None)
                    }
                }
            }
        };

        // send
        //
        // FIXME: sending may hang forever, because of network
        // problems; add a timeout

        match &mut socket {
            Connection::Peer(socket) => {
                match wm.write_to_sync(socket.mut_socket(), flush) {
                    Ok(_) => {}
                    Err(error) => {
                        error!("Failed to write to socket on client {:?}", dest_node_id);
                        break;
                    }
                }
            }
            Connection::Me(peer_conn) => {
                if let Some(message) = m {
                    //Get the header with the digest and signature
                    let (header, _) = wm.into_inner();

                    peer_conn.push_request_sync(Message::System(header, message));
                } else {
                    unreachable!()
                }
            }
        }

        if let Some(comm_stats) = &comm_stats {
            let message_passing = before_sending.duration_since(start_time).as_nanos();

            let dur_sending = Instant::now().duration_since(before_sending).as_nanos();

            comm_stats.insert_message_passing_latency(dest_node_id, message_passing);
            comm_stats.insert_message_sending_time(dest_node_id, dur_sending);

            comm_stats.register_rq_sent(dest_node_id);
        }
    }
}

pub fn initialize_async_sending_task_for<D>(peer_info: PeerConnectionInfo,
                                            socket: SocketSendAsync) -> ConnectionHandle<D>
    where D: SharedData + 'static {
    let (tx, rx) = bft::communication::channel::new_bounded_async(QUEUE_SPACE);

    rt::spawn(async_sending_task(peer_info, Connection::Peer(SecureSocketSend::Async(socket)), rx));

    ConnectionHandle::Async(tx)
}

///Receives requests from the queue and sends them using the provided socket
async fn async_sending_task<D>(peer_conn_info: PeerConnectionInfo, mut socket: Connection<D>, mut recv: ChannelAsyncRx<MessageType<D>>)
    where D: SharedData + 'static {
    let own_node_id = peer_conn_info.own_node_id;
    let dest_node_id = peer_conn_info.dest_node_id;
    let key = peer_conn_info.node_shared.as_ref().map(|ref sh| &sh.my_key);
    let comm_stats = peer_conn_info.comm_stats;

    loop {
        let recv_result = recv.recv().await;

        let to_send = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", dest_node_id);
                break;
            }
        };

        let nonce = to_send.nonce;

        let start_time = to_send.start_time;

        let flush = to_send.flush;

        let before_sending = Instant::now();

        let (wm, m) = match to_send.message {
            Either::Left((message, serialized)) => {
                let (buf, digest) = if let Some((buf, digest)) = serialized {
                    (buf, digest)
                } else {
                    let start_serialization = Instant::now();

                    let mut buf: Buf = Buf::new();

                    let digest = <D as DigestData>::serialize_digest(
                        &to_send.message,
                        &mut buf,
                    ).unwrap();

                    if let Some(comm_stats) = &comm_stats {
                        let time_taken_signing = Instant::now().duration_since(start_serialization).as_nanos();

                        //Broadcasts are always for replicas, so make this
                        comm_stats.insert_message_signing_time(NodeId::from(0u32), time_taken_signing);
                    }

                    (buf, digest)
                };

                (WireMessage::new(
                    own_node_id,
                    dest_node_id,
                    buf,
                    nonce,
                    Some(digest),
                    key,
                ), Some(message))
            }
            Either::Right((buf, digest)) => {
                // create wire msg
                (WireMessage::new(
                    own_node_id,
                    dest_node_id,
                    buf,
                    nonce,
                    Some(digest),
                    key,
                ), None)
            }
        };

        match &mut socket {
            Connection::Peer(socket) => {
                match wm.write_to(socket.mut_socket(), flush).await {
                    Ok(_) => {}
                    Err(error) => {
                        error!("Failed to write to socket on client {:?}", dest_node_id);
                        break;
                    }
                }
            }
            Connection::Me(peer_conn) => {
                if let Some(message) = m {
                    let (header, _) = wm.into_inner();

                    peer_conn.push_request(Message::System(header, message)).await;
                } else {
                    unreachable!()
                }
            }
        }

        if let Some(comm_stats) = &comm_stats {
            let message_passing = before_sending.duration_since(start_time).as_nanos();

            let dur_sending = Instant::now().duration_since(before_sending).as_nanos();

            comm_stats.insert_message_passing_latency(dest_node_id, message_passing);
            comm_stats.insert_message_sending_time(dest_node_id, dur_sending);

            comm_stats.register_rq_sent(dest_node_id);
        }
    }
}