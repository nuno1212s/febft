use std::sync::Arc;
use std::time::Instant;
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

pub type SendMessage = (WireMessage, Instant);

#[derive(Clone)]
pub enum ConnectionHandle {
    Sync(ChannelSyncTx<SendMessage>),
    Async(ChannelAsyncTx<SendMessage>),
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

        match channel.send((message, Instant::now())) {
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

        match channel.send((message, Instant::now())).await {
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
fn sync_sending_thread(peer_id: NodeId, mut socket: SocketSendSync, recv: ChannelSyncRx<SendMessage>,
                          comm_stats: Option<Arc<CommStats>>) {
    loop {
        let recv_result = recv.recv();

        let (to_send, init_time) = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };
        
        let before_send = Instant::now();
        
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
        
        if let Some(comm_stats) = &comm_stats {
            let time_taken_passing = before_send.duration_since(init_time).as_nanos();

            let time_taken_sending = Instant::now().duration_since(before_send).as_nanos();
            
            comm_stats.insert_message_passing_to_send_thread(to_send.header.to(), time_taken_passing);
            comm_stats.insert_message_sending_time(to_send.header.to(), time_taken_sending);
            comm_stats.register_rq_sent(to_send.header.to());
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
async fn async_sending_task(peer_id: NodeId, mut socket: SocketSendAsync, mut recv: ChannelAsyncRx<SendMessage>,
                            comm_stats: Option<Arc<CommStats>>) {
    loop {
        let recv_result = recv.recv().await;

        let (to_send, init_time) = match recv_result {
            Ok(to_send) => { to_send }
            Err(recv_err) => {
                error!("Sending channel for client {:?} has disconnected!", peer_id);
                break;
            }
        };
        
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

            comm_stats.insert_message_passing_to_send_thread(to_send.header.to(), time_taken_passing);
            comm_stats.insert_message_sending_time(to_send.header.to(), time_taken_sending);
            comm_stats.register_rq_sent(to_send.header.to());
        }
    }
}