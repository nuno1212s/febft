use std::sync::Arc;
use log::error;
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::{NodeId};
use crate::bft::communication::message::WireMessage;
use crate::bft::communication::socket::{SocketSendAsync, SocketSendSync};

use crate::bft::async_runtime as rt;

///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 128;

#[derive(Clone)]
pub enum ConnectionHandle {
    Sync(ChannelSyncTx<WireMessage>),
    Async(ChannelAsyncTx<WireMessage>),
}

impl ConnectionHandle {

    pub fn send(&self, message: WireMessage) -> Result<(), ()>{
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

    pub async fn async_send(&mut self, message: WireMessage) -> Result<(), ()>{
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

pub fn initialize_sync_sending_thread_for(peer_id: NodeId, socket: SocketSendSync,
                                             comm_stats: Option<Arc<CommStats>>) -> ConnectionHandle {
    let (tx, rx) = crate::bft::communication::channel::new_bounded_sync(QUEUE_SPACE);

    std::thread::Builder::new()
        .name(format!("Peer {:?} sending thread", peer_id))
        .spawn(move || {
            sync_sending_thread(peer_id, socket, rx, comm_stats)
        }).expect(format!("Failed to start sending thread for client {:?}", peer_id).as_str());

    ConnectionHandle::Sync(tx)
}

///Receives requests from the queue and sends them using the provided socket
fn sync_sending_thread(peer_id: NodeId, mut socket: SocketSendSync, recv: ChannelSyncRx<WireMessage>,
                          comm_stats: Option<Arc<CommStats>>) {
    loop {
        let recv_result = recv.recv();

        let to_send = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

        // send
        //
        // FIXME: sending may hang forever, because of network
        // problems; add a timeout
        match to_send.write_to_sync(socket.mut_socket(), true) {
            Ok(_) => {}
            Err(error) => {
                error!("Failed to write to socket on client {:?}", peer_id);
                break;
            }
        }
    }
}

pub fn initialize_async_sending_task_for(peer_id: NodeId, socket: SocketSendAsync,
                                            comm_stats: Option<Arc<CommStats>>) -> ConnectionHandle {
    let (tx, rx) = crate::bft::communication::channel::new_bounded_async(QUEUE_SPACE);

    rt::spawn(async_sending_task(peer_id, socket, rx, comm_stats));

    ConnectionHandle::Async(tx)
}

///Receives requests from the queue and sends them using the provided socket
async fn async_sending_task(peer_id: NodeId, mut socket: SocketSendAsync, mut recv: ChannelAsyncRx<WireMessage>,
                            comm_stats: Option<Arc<CommStats>>) {
    loop {
        let recv_result = recv.recv().await;

        let to_send = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };

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
    }
}