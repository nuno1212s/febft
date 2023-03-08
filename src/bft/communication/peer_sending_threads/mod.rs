
use crate::{bft, message_peer_sending_thread_sent, start_measurement};
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelMixedRx, ChannelMixedTx, ChannelSyncRx, ChannelSyncTx};

use crate::bft::communication::message::WireMessage;
use crate::bft::communication::socket::{SocketSendAsync, SocketSendSync};
use crate::bft::communication::{Node, NodeId};
use crate::bft::error::*;
use dashmap::DashMap;
use log::{debug, error};
use std::sync::Arc;
use std::time::Instant;


use crate::bft::async_runtime as rt;
use crate::bft::communication::serialize::SharedData;



///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 16384;

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
pub struct ConnectionHandle {
    channel: ChannelMixedTx<SendMessage>,
}

impl ConnectionHandle {
    pub fn send(&self, message: WireMessage, rq_key: Option<u64>) -> Result<()> {
        match self.channel.send(SendMessageType::Message(message, Instant::now(), rq_key)) {

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
    let (tx, rx) = bft::communication::channel::new_bounded_mixed(QUEUE_SPACE);


    std::thread::Builder::new()
        .name(format!("Peer {:?} sending thread", peer_id))
        .spawn(move || sync_sending_thread(node, peer_id, socket, rx, comm_stats, sent_rqs))
        .expect(format!("Failed to start sending thread for client {:?}", peer_id).as_str());

    ConnectionHandle { channel: tx }
}

///Receives requests from the queue and sends them using the provided socket
fn sync_sending_thread<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    mut socket: SocketSendSync,
    recv: ChannelMixedRx<SendMessage>,
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

        let flush = recv.len() == 0;

        match to_send {
            SendMessage::Message(to_send, init_time, rq_key) => {
                start_measurement!(before_send);

                // send
                //
                // FIXME: sending may hang forever, because of network
                // problems; add a timeout

                match to_send.write_to_sync(socket.mut_socket(), flush) {

                    Ok(_) => {}
                    Err(error) => {
                        error!("Failed to write to socket on client {:?} {:?}", peer_id, error);
                        break;
                    }
                }

                message_peer_sending_thread_sent!(&comm_stats, init_time, before_send, to_send);


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
    let (tx, rx) = bft::communication::channel::new_bounded_mixed(QUEUE_SPACE);


    rt::spawn(async_sending_task(node, peer_id, socket, rx, comm_stats));

    ConnectionHandle { channel: tx }
}


///Receives requests from the queue and sends them using the provided socket
async fn async_sending_task<D>(
    node: Arc<Node<D>>,
    peer_id: NodeId,
    mut socket: SocketSendAsync,
    mut recv: ChannelMixedRx<SendMessage>,
    comm_stats: Option<Arc<CommStats>>,
) where
    D: SharedData + 'static,
{
    loop {
        let recv_result = recv.recv_async().await;

        let to_send = match recv_result {
            Ok(to_send) => to_send,
            Err(_recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        let flush = true;

        match to_send {
            SendMessage::Message(to_send, init_time, _) => {
                start_measurement!(before_send);

                // send
                //
                // FIXME: sending may hang forever, because of network
                // problems; add a timeout
                match to_send.write_to(socket.mut_socket(), flush).await {

                    Ok(_) => {}
                    Err(_error) => {
                        error!("Failed to write to socket on client {:?}", peer_id);
                        break;
                    }
                }

                message_peer_sending_thread_sent!(&comm_stats, init_time, before_send, to_send);
            }
        }
    }

    node.tx_connect_node_async(peer_id, None);
}