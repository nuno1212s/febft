use crate::bft;
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelSyncRx, ChannelSyncTx, RecvError, SendError};
use crate::bft::communication::message::WireMessage;
use crate::bft::communication::socket::{SocketSendAsync, SocketSendSync};
use crate::bft::communication::{channel, Node, NodeId, PeerAddr};
use dashmap::DashMap;
use log::error;
use std::sync::Arc;
use std::time::Instant;
use either::Either;

use crate::bft::async_runtime as rt;
use crate::bft::communication::serialize::SharedData;
use crate::bft::error::{Error, ErrorKind};

use super::NodeConnector;

///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 128;

pub type SendMessage = SendMessageType;

pub type PingResponse = Result<()>;

pub enum SendMessageType {
    Ping(Either<ChannelSyncTx<PingResponse>, ChannelAsyncTx<PingResponse>>),
    Message(WireMessage, Instant, Option<u64>),
}

#[derive(Clone)]
pub enum ConnectionHandle {
    Sync(ChannelSyncTx<SendMessage>),
    Async(ChannelAsyncTx<SendMessage>),
}

impl ConnectionHandle {
    pub fn send(&self, message: WireMessage, rq_key: Option<u64>) -> Result<(), ()> {
        let channel = match self {
            ConnectionHandle::Sync(channel) => channel,
            ConnectionHandle::Async(_) => {
                panic!("Cannot send asynchronously on synchronous channel!");
            }
        };

        match channel.send((message, Instant::now(), rq_key)) {
            Ok(_) => Ok(()),
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(())
            }
        }
    }

    pub async fn async_send(&mut self, message: WireMessage) -> Result<(), ()> {
        let channel = match self {
            ConnectionHandle::Sync(_) => {
                panic!("Cannot send synchronously on asynchronous channel");
            }
            ConnectionHandle::Async(channel) => channel,
        };

        match channel.send(Message(message, Instant::now(), None)).await {
            Ok(_) => Ok(()),
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(())
            }
        }
    }

    pub fn is_active_sync(&self) -> bool {
        let channel = match self {
            ConnectionHandle::Sync(channel) => {
                channel
            }
            ConnectionHandle::Async(_) => {
                panic!("Cannot send asynchronously on synchronous channel!");
            }
        };

        let (response_channel_tx, response_channel_rx) = channel::new_bounded_sync(1);

        match channel.send(SendMessageType::Ping(Either::Left(response_channel_tx))) {
            Ok(_) => { Ok(()) }
            Err(_) => {}
        }

        match response_channel_rx.recv() {
            Ok(Ok(())) => {
                return true;
            }
            _ => {
                return false;
            }
        };
    }

    pub async fn is_active_async(&self) -> bool {
        let channel = match self {
            ConnectionHandle::Sync(_) => {
                panic!("Cannot send synchronously on asynchronous channel!");
            }
            ConnectionHandle::Async(channel) => {
                channel
            }
        };

        let (response_channel_tx, response_channel_rx) = channel::new_bounded_async(1);

        match channel.send(SendMessageType::Ping(Either::Right(response_channel_tx))).await {
            Ok(_) => { Ok(()) }
            Err(_) => {}
        }

        match response_channel_rx.recv().await {
            Ok(Ok(())) => {
                return true;
            }
            _ => {
                return false;
            }
        };
    }

    pub fn close(self) {
        //By taking ownership of ourselves and then allowing ourselves to be dropped, the channels
        //Will close when all other handles are also closed
    }
}

pub fn initialize_sync_sending_thread_for<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    socket: SocketSendSync,
    comm_stats: Option<Arc<CommStats>>,
    sent_rqs: Option<Arc<Vec<DashMap<u64, ()>>>>,
) -> ConnectionHandle
    where
        D: SharedData + 'static,
{
    let (tx, rx) = bft::communication::channel::new_bounded_sync(QUEUE_SPACE);

    std::thread::Builder::new()
        .name(format!("Peer {:?} sending thread", peer_id))
        .spawn(move || sync_sending_thread(node, peer_id, socket, rx, comm_stats, sent_rqs))
        .expect(format!("Failed to start sending thread for client {:?}", peer_id).as_str());

    ConnectionHandle::Sync(tx)
}

///Receives requests from the queue and sends them using the provided socket
fn sync_sending_thread<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    mut socket: SocketSendSync,
    recv: ChannelSyncRx<SendMessage>,
    comm_stats: Option<Arc<CommStats>>,
    sent_rqs: Option<Arc<Vec<DashMap<u64, ()>>>>,
) where
    D: SharedData + 'static,
{
    loop {
        let recv_result = recv.recv();

        let to_send = match recv_result {
            Ok(to_send) => to_send,
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        match to_send {
            SendMessage::Ping(tx_channel) => {
                let tx_channel = match tx_channel {
                    Either::Left(tx_channel) => {
                        tx_channel
                    }
                    _ => { panic!("") }
                };

                let wm = WireMessage::new(node.id(), peer_id, Vec::new(), 0, None, None);

                match wm.write_to_sync(socket.mut_socket(), true) {
                    Ok(_) => {
                        tx_channel.send(Ok(())).unwrap();
                    }
                    Err(err) => {
                        error!("Failed to write to socket on client {:?} {:?}", peer_id, error);

                        tx_channel.send(Err(Error::wrapped(ErrorKind::CommunicationPeerSendingThreads, err))).unwrap();

                        break;
                    }
                };
            }
            SendMessage::Message(to_send, init_time, rq_key) => {
                let before_send = Instant::now();

                // send
                //
                // FIXME: sending may hang forever, because of network
                // problems; add a timeout
                match to_send.write_to_sync(socket.mut_socket(), true) {
                    Ok(_) => {}
                    Err(error) => {
                        error!("Failed to write to socket on client {:?} {:?}", peer_id, error);
                        break;
                    }
                }

                if let Some(comm_stats) = &comm_stats {
                    let time_taken_passing = before_send.duration_since(init_time).as_nanos();

                    let time_taken_sending = Instant::now().duration_since(before_send).as_nanos();

                    comm_stats
                        .insert_message_passing_to_send_thread(to_send.header.to(), time_taken_passing);
                    comm_stats.insert_message_sending_time(to_send.header.to(), time_taken_sending);
                    comm_stats.register_rq_sent(to_send.header.to());
                }

                if let Some(sent_rqs) = &sent_rqs {
                    if let Some(rq_key) = rq_key {
                        sent_rqs[rq_key as usize % sent_rqs.len()].insert(rq_key, ());
                    }
                }
            }
        }
    }

}

pub fn initialize_async_sending_task_for<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    socket: SocketSendAsync,
    comm_stats: Option<Arc<CommStats>>,
) -> ConnectionHandle
    where
        D: SharedData + 'static,
{
    let (tx, rx) = crate::bft::communication::channel::new_bounded_async(QUEUE_SPACE);

    rt::spawn(async_sending_task(node, peer_id, socket, rx, comm_stats));

    ConnectionHandle::Async(tx)
}

///Receives requests from the queue and sends them using the provided socket
async fn async_sending_task<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    mut socket: SocketSendAsync,
    mut recv: ChannelAsyncRx<SendMessage>,
    comm_stats: Option<Arc<CommStats>>,
) where
    D: SharedData + 'static,
{
    loop {
        let recv_result = recv.recv().await;

        let to_send = match recv_result {
            Ok(to_send) => to_send,
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        match to_send {
            SendMessage::Ping(tx_channel) => {

                let tx_channel = match tx_channel { Either::Right(tx_channel) => {
                    tx_channel
                }
                _ => {
                    panic!("Wrong channel for ping");
                }};

                let wm = WireMessage::new(node.id(), peer_id, Vec::new(), 0, None, None);

                match wm.write_to(socket.mut_socket(), true).await {
                    Ok(_) => {
                        tx_channel.send(Ok(())).await.unwrap();
                    }
                    Err(err) => {
                        error!("Failed to write to socket on client {:?} {:?}", peer_id, error);

                        tx_channel.send(Err(Error::wrapped(ErrorKind::CommunicationPeerSendingThreads, err)))
                            .await.unwrap();

                        break;
                    }
                };
            }

            SendMessage::Message(to_send, init_time, _) => {
                let before_send = Instant::now();

                // send
                //
                // FIXME: sending may hang forever, because of network
                // problems; add a timeout
                match to_send.write_to(socket.mut_socket(), true).await {
                    Ok(_) => {}
                    Err(error) => {
                        error!("Failed to write to socket on client {:?}", peer_id);
                        break;
                    }
                }

                if let Some(comm_stats) = &comm_stats {
                    let time_taken_passing = before_send.duration_since(init_time).as_nanos();

                    let time_taken_sending = Instant::now().duration_since(before_send).as_nanos();

                    comm_stats
                        .insert_message_passing_to_send_thread(to_send.header.to(), time_taken_passing);
                    comm_stats.insert_message_sending_time(to_send.header.to(), time_taken_sending);
                    comm_stats.register_rq_sent(to_send.header.to());
                }
            }
        }
    }
}
