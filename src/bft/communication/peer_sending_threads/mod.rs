use std::sync::Arc;
use std::time::Instant;
use dashmap::DashMap;
use log::error;
use crate::bft;
use crate::bft::benchmarks::CommStats;
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelAsyncTx, ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::{Node, NodeId, PeerAddr};
use crate::bft::communication::message::WireMessage;
use crate::bft::communication::socket::{SocketSendAsync, SocketSendSync};

use crate::bft::async_runtime as rt;
use crate::bft::communication::serialize::SharedData;

///Implements the behaviour where each connection has it's own dedicated thread that will handle
///Sending messages from it
const QUEUE_SPACE: usize = 128;

pub type SendMessage = (WireMessage, Instant, Option<u64>);

#[derive(Clone)]
pub enum ConnectionHandle {
    Sync(ChannelSyncTx<SendMessage>),
    Async(ChannelAsyncTx<SendMessage>),
}

impl ConnectionHandle {
    pub fn send(&self, message: WireMessage, rq_key: Option<u64>) -> Result<(), ()> {
        let channel = match self {
            ConnectionHandle::Sync(channel) => {
                channel
            }
            ConnectionHandle::Async(_) => {
                panic!("Cannot send asynchronously on synchronous channel!");
            }
        };

        match channel.send((message, Instant::now(), rq_key)) {
            Ok(_) => {
                Ok(())
            }
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
            ConnectionHandle::Async(channel) => {
                channel
            }
        };

        match channel.send((message, Instant::now(), None)).await {
            Ok(_) => {
                Ok(())
            }
            Err(error) => {
                error!("Failed to send to the channel! {:?}", error);

                Err(())
            }
        }
    }

    pub fn close(self) {
        //By taking ownership of ourselves and then allowing ourselves to be dropped, the channels
        //Will close when all other handles are also closed
    }
}

pub fn initialize_sync_sending_thread_for<D>(node: Arc<Node<D>>, peer_id: NodeId, socket: SocketSendSync,
                                             comm_stats: Option<Arc<CommStats>>,
                                             sent_rqs: Option<Arc<Vec<DashMap<u64, ()>>>>, ) -> ConnectionHandle
    where D: SharedData + 'static {
    let (tx, rx) = bft::communication::channel::new_bounded_sync(QUEUE_SPACE);

    std::thread::Builder::new()
        .name(format!("Peer {:?} sending thread", peer_id))
        .spawn(move || {
            sync_sending_thread(node, peer_id, socket, rx, comm_stats, sent_rqs)
        }).expect(format!("Failed to start sending thread for client {:?}", peer_id).as_str());

    ConnectionHandle::Sync(tx)
}

///Receives requests from the queue and sends them using the provided socket
fn sync_sending_thread<D>(node: Arc<Node<D>>, peer_id: NodeId, mut socket: SocketSendSync, recv: ChannelSyncRx<SendMessage>,
                          comm_stats: Option<Arc<CommStats>>,
                          sent_rqs: Option<Arc<Vec<DashMap<u64, ()>>>>, ) where D: SharedData + 'static {
    loop {
        let recv_result = recv.recv();

        let (to_send, init_time, rq_key) = match recv_result {
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

        if let Some(sent_rqs) = &sent_rqs {
            if let Some(rq_key) = rq_key {
                sent_rqs[rq_key as usize % sent_rqs.len()].insert(rq_key, ());
            }
        }
    }

    /*
    On disconnections we want to attempt to reconnect
     */
    let mut state = bft::prng::State::new();

    let peer_addr = node.peer_addrs.get(peer_id.id() as u64);

    match peer_addr {
        None => {
            error!("{:?} // Failed to find address for node {:?}", node.id(), peer_id);
        }
        Some(addr) => {
            if node.id() < node.first_client_id() && peer_id < node.first_client_id() {
                //Both nodes are replicas, so attempt to connect to the replica
                if let Some(addr) = &addr.replica_addr {
                    let id = node.id();
                    let first_cli = node.first_client_id();
                    let sync_conn = Arc::clone(&node.sync_connector);
                    let addr = addr.clone();

                    //If we have the replica only IP (port) of the replicas, then we are probably a replica and
                    node.tx_side_connect_task_sync(id, first_cli,
                                                   peer_id, state.next_state(),
                                                   sync_conn, addr);
                } else {
                    error!("{:?} // Failed to connect because no IP was present for replica {:?}", node.id(), peer_id);
                }
            } else {
                let id = node.id();
                let first_cli = node.first_client_id();
                let sync_conn = Arc::clone(&node.sync_connector);
                let addr = addr.client_addr.clone();

                //If we are not a replica connecting to a replica, we will always use the "client" ports.
                node.tx_side_connect_task_sync(id, first_cli,
                                               peer_id, state.next_state(),
                                               sync_conn, addr);
            }
        }
    }
}

pub fn initialize_async_sending_task_for<D>(node: Arc<Node<D>>, peer_id: NodeId, socket: SocketSendAsync,
                                            comm_stats: Option<Arc<CommStats>>) -> ConnectionHandle
    where D: SharedData + 'static {
    let (tx, rx) = crate::bft::communication::channel::new_bounded_async(QUEUE_SPACE);

    rt::spawn(async_sending_task(node, peer_id, socket, rx, comm_stats));

    ConnectionHandle::Async(tx)
}

///Receives requests from the queue and sends them using the provided socket
async fn async_sending_task<D>(node: Arc<Node<D>>, peer_id: NodeId, mut socket: SocketSendAsync, mut recv: ChannelAsyncRx<SendMessage>,
                               comm_stats: Option<Arc<CommStats>>) where D: SharedData + 'static {
    loop {
        let recv_result = recv.recv().await;

        let (to_send, init_time, _) = match recv_result {
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

    /*
    On disconnections we want to attempt to reconnect
     */
    let mut state = bft::prng::State::new();

    let addr = node.peer_addrs.get(peer_id.id() as u64);

    match addr {
        None => {
            error!("{:?} // Failed to find address for node {:?}", node.id(), peer_id);
        }
        Some(addr) => {
            if node.id() < node.first_client_id() && peer_id < node.first_client_id() {
                //Both nodes are replicas, so attempt to connect to the replica
                if let Some(addr) = &addr.replica_addr {
                    let id = node.id();
                    let first_cli = node.first_client_id();
                    let conn = node.connector.clone();
                    let addr = addr.clone();

                    //If we have the replica only IP (port) of the replicas, then we are probably a replica and
                    node.tx_side_connect_task(id, first_cli,
                                              peer_id, state.next_state(),
                                              conn, addr).await;
                } else {
                    error!("{:?} // Failed to connect because no IP was present for replica {:?}", node.id(), peer_id);
                }
            } else {
                let id = node.id();
                let first_cli = node.first_client_id();
                let conn = node.connector.clone();
                let addr = addr.client_addr.clone();

                //If we are not a replica connecting to a replica, we will always use the "client" ports.
                node.tx_side_connect_task(id, first_cli,
                                          peer_id, state.next_state(),
                                          conn, addr).await;
            }
        }
    }
}