use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use dashmap::DashMap;

use febft_common::channel::{ChannelMixedRx, ChannelMixedTx, new_bounded_mixed};
use febft_common::error::*;
use febft_common::socket::SecureReadHalf;
use febft_common::socket::SecureWriteHalf;

use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, WireMessage};
use crate::NodeId;
use crate::serialize::Serializable;
use crate::tcpip::{NodeConnectionAcceptor, TlsNodeAcceptor, TlsNodeConnector};
use crate::tcpip::connections::conn_establish::ConnectionHandler;

mod incoming;
mod outgoing;
mod conn_establish;

pub type Callback = Option<Box<dyn FnOnce(bool) -> () + Send>>;

pub type SerializedMessage = (WireMessage, Callback);

/// How many slots the outgoing queue has for messages.
const TX_CONNECTION_QUEUE: usize = 1024;

/// Represents a connection between two peers
/// We can have multiple underlying tcp connections for a given connection between two peers
pub enum PeerConnection<M: Serializable + 'static> {
    Me {
        // A loopback to ourselves
        client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    },
    Peer {
        //The client we are connected to
        client: Arc<ConnectedPeer<NetworkMessage<M>>>,
        //The channel used to send serialized messages to the tasks that are meant to handle them
        tx: ChannelMixedTx<SerializedMessage>,
        // Counter to assign unique IDs to each of the underlying Tcp streams
        conn_id_generator: AtomicU32,
        // A map to manage the currently active connections and a cached size value to prevent
        // concurrency for simple length checks
        active_connection_count: AtomicUsize,
        active_connections: Mutex<BTreeMap<u32, ConnHandle>>,
        // The RX handle corresponding to the tx channel above. This is so we can quickly associate new
        // TX connections to a given connection, as we just have to clone this handle
        rx: ChannelMixedRx<SerializedMessage>,
    },
}

#[derive(Clone)]
struct ConnHandle {
    id: u32,
    cancelled: Arc<AtomicBool>,
}

impl ConnHandle {
    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

impl<M> PeerConnection<M> where M: Serializable {
    pub fn new_me(client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Arc<Self> {
        Arc::new(Self::Me {
            client
        })
    }

    pub fn new_peer(client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Arc<Self> {
        let (tx, rx) = new_bounded_mixed(TX_CONNECTION_QUEUE);

        Arc::new(Self::Peer {
            client,
            tx,
            rx,
            conn_id_generator: AtomicU32::new(0),
            active_connection_count: AtomicUsize::new(0),
            active_connections: Mutex::new(BTreeMap::new()),
        })
    }

    /// Send a message through this connection
    pub(crate) fn send_message(&self, msg: WireMessage, callback: Callback) -> Result<()> {
        if let Err(_) = self.tx.send((msg, callback)) {
            //TODO: There are no TX connections available. Close this connection
        }

        Ok(())
    }

    /// Initialize a new Connection Handle for a new TCP Socket
    fn init_conn_handle(&self) -> ConnHandle {
        match self {
            PeerConnection::Peer { conn_id_generator, .. } => {
                let conn_id = conn_id_generator.fetch_add(1, Ordering::Relaxed);

                ConnHandle {
                    id: conn_id,
                    cancelled: Arc::new(AtomicBool::new(false)),
                }
            }
            PeerConnection::Me { .. } => { unreachable!() }
        }
    }

    /// Get the current connection count for this connection
    pub fn connection_count(&self) -> usize {
        match self {
            PeerConnection::Me { .. } => {
                1
            }
            PeerConnection::Peer { active_connection_count, .. } => {
                active_connection_count.load(Ordering::Relaxed)
            }
        }
    }

    /// Accept a TCP Stream and insert it into this connection
    pub(crate) fn insert_new_connection(self: &Arc<Self>, socket: (SecureWriteHalf, SecureReadHalf)) {
        match self {
            PeerConnection::Peer { active_connection_count, active_connections, .. } => {
                // Create a handle for this new connection
                let conn_handle = self.init_conn_handle();

                let previous = {
                    //Store the connection in the map
                    let mut guard = active_connections.lock().unwrap();

                    guard.insert(conn_handle.id, conn_handle.clone())
                };

                if let None = previous {
                    active_connection_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    todo!("How do we handle this really?
                        This probably means it went all the way around u32 connections, really weird")
                }

                //Spawn the corresponding handlers for each side of the connection
                outgoing::spawn_outgoing_task_handler(conn_handle.clone(), Arc::clone(self), socket.0);
                incoming::spawn_incoming_task_handler(conn_handle, Arc::clone(self), socket.1);
            }
            PeerConnection::Me { .. } => { unreachable!() }
        }
    }

    /// Delete a given tcp stream from this connection
    pub(crate) fn delete_connection(&self, conn_id: u32) {
        match self {
            PeerConnection::Peer { active_connection_count, active_connections, .. } => {
                // Remove the corresponding connection from the map
                let conn_handle = {
                    // Do it inside a tiny scope to minimize the time the mutex is accessed
                    let mut guard = active_connections.lock().unwrap();

                    guard.remove(&conn_id)
                };

                if let Some(conn_handle) = conn_handle {
                    active_connection_count.fetch_sub(1, Ordering::Relaxed);

                    //Setting the cancelled variable to true causes all associated threads to be
                    //killed (as soon as they see the warning)
                    conn_handle.cancelled.store(true, Ordering::Relaxed);
                }
            }
            PeerConnection::Me { .. } => { unreachable!() }
        }
    }

    /// Get the handle to the client pool buffer
    fn client_pool_peer(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        match self {
            PeerConnection::Peer { client, .. } => {
                client
            }
            PeerConnection::Me { .. } => {
                unreachable!()
            }
        }
    }

    /// Get the handle to the receiver for transmission
    fn to_send_handle(&self) -> &ChannelMixedRx<SerializedMessage> {
        match self {
            PeerConnection::Peer { rx, .. } => {
                rx
            }
            PeerConnection::Me { .. } => {
                unreachable!()
            }
        }
    }
}

/// Stores all of the connections that this peer currently has established.
#[derive(Clone)]
pub struct PeerConnections<M: Serializable + 'static> {
    id: NodeId,
    loopback: Arc<PeerConnection<M>>,
    connection_map: Arc<DashMap<NodeId, Arc<PeerConnection<M>>>>,
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
    connection_establisher: Arc<ConnectionHandler>,
}

impl<M: Serializable + 'static> PeerConnections<M> {
    pub fn new(peer_id: NodeId, first_cli: NodeId,
               node_connector: TlsNodeConnector,
               node_acceptor: TlsNodeAcceptor,
               client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>) -> Arc<Self> {
        let connection_establish = ConnectionHandler::new(peer_id, first_cli, node_connector, node_acceptor);

        Arc::new(Self {
            id: peer_id,
            loopback: PeerConnection::new_me(Arc::clone(client_pooling.loopback_connection())),
            connection_map: Arc::new(DashMap::new()),
            client_pooling,
            connection_establisher: connection_establish,
        })
    }

    /// Get the amount of connected nodes at this time
    pub fn connected_nodes(&self) -> usize {
        self.connection_map.len()
    }

    /// Setup a tcp listener inside this peer connections object.
    pub(super) fn setup_tcp_listener(self: Arc<Self>, node_acceptor: NodeConnectionAcceptor) {
        self.connection_establisher.clone().setup_conn_worker(node_acceptor, self)
    }

    /// Are we currently connected to a given client?
    pub fn is_connected_to(&self, node: &NodeId) -> bool {
        self.connection_map.contains_key(node)
    }

    /// Get the current amount of concurrent TCP connections between nodes
    pub fn current_connection_count_of(&self, node: &NodeId) -> Option<usize> {
        self.connection_map.get(node).map(|connection| {
            connection.value().connection_count()
        })
    }

    /// Get the connection to a given node
    pub fn get_connection(&self, node: &NodeId) -> Option<Arc<PeerConnection<M>>> {
        if node == self.id {
            return Some(self.loopback.clone());
        }

        let option = self.connection_map.get(node);

        option.map(|conn| conn.value()).cloned()
    }

    /// Handle a new connection being established (either through a "client" or a "server" connection)
    /// This will either create the corresponding peer connection or add the connection to the already existing
    /// connection
    pub(crate) fn handle_connection_established(&self, node: NodeId, socket: (SecureWriteHalf, SecureReadHalf)) {
        let option = self.connection_map.entry(node);

        let peer_conn = option.or_insert_with(||
            { PeerConnection::new_peer(self.client_pooling.init_peer_conn(node)) });

        peer_conn.insert_new_connection(socket);
    }
}
