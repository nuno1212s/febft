use crate::bft;
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::WireMessage;
use crate::bft::communication::socket::{SocketSendAsync, SocketSendSync};
use crate::bft::communication::{Node, NodeId};
use crate::bft::error::*;
use dashmap::DashMap;
use log::{error};
use std::sync::Arc;
use std::time::Instant;


use crate::bft::async_runtime as rt;
use crate::bft::communication::serialize::SharedData;



///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 128;

pub type SendMessage = SendMessageType;

pub type PingResponse = Result<()>;

pub enum SendMessageType {
    Message(WireMessage, Instant, Option<u64>),
}

///A handle for the connection in question
///When the connection fails for some reason, the RX side of
/// this handle will be dropped and as such, this will
/// error out when attempting to send the next message
#[derive(Clone)]
pub enum ConnectionHandle {
    Sync(ChannelSyncTx<SendMessage>),
    Async(ChannelAsyncTx<SendMessage>),
}

impl ConnectionHandle {
    pub fn send(&self, message: WireMessage, rq_key: Option<u64>) -> Result<()> {
        let channel = match self {
            ConnectionHandle::Sync(channel) => channel,
            ConnectionHandle::Async(_) => {
                panic!("Cannot send asynchronously on synchronous channel!");
            }
        };

        match channel.send(SendMessageType::Message(message, Instant::now(), rq_key)) {
            Ok(_) => Ok(()),
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(Error::wrapped(ErrorKind::CommunicationPeerSendingThreads, error))
            }
        }
    }

    pub async fn async_send(&mut self, message: WireMessage) -> Result<()> {
        let mut channel = match self {
            ConnectionHandle::Sync(_) => {
                panic!("Cannot send synchronously on asynchronous channel");
            }
            ConnectionHandle::Async(channel) => channel.clone(),
        };

        match channel.send(SendMessageType::Message(message, Instant::now(), None)).await {
            Ok(_) => Ok(()),
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(Error::wrapped(ErrorKind::CommunicationPeerSendingThreads, error))
            }
        }
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
            Err(_recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        match to_send {
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

    node.tx_connect_node_sync(peer_id, None);
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
            Err(_recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        match to_send {
            SendMessage::Message(to_send, init_time, _) => {
                let before_send = Instant::now();

                // send
                //
                // FIXME: sending may hang forever, because of network
                // problems; add a timeout
                match to_send.write_to(socket.mut_socket(), true).await {
                    Ok(_) => {}
                    Err(_error) => {
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

    node.tx_connect_node_async(peer_id, None);
}