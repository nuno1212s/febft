use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use dashmap::DashMap;
use intmap::IntMap;

use febft_common::channel::{ChannelMixedRx, ChannelMixedTx, new_bounded_mixed, new_oneshot_channel, OneShotRx};
use febft_common::error::*;
use febft_common::socket::SecureReadHalf;
use febft_common::socket::SecureWriteHalf;

use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, WireMessage};
use crate::{NodeConnections, NodeId};
use crate::serialize::Serializable;
use crate::tcpip::{NodeConnectionAcceptor, PeerAddr, TlsNodeAcceptor, TlsNodeConnector};
use crate::tcpip::connections::conn_establish::ConnectionHandler;

mod incoming;
mod outgoing;
mod conn_establish;

pub type Callback = Option<Box<dyn FnOnce(bool) -> () + Send>>;

pub type SerializedMessage = (WireMessage, Callback);

/// How many slots the outgoing queue has for messages.
const TX_CONNECTION_QUEUE: usize = 1024;

/// The amount of parallel TCP connections we should try to maintain for
/// each connection
struct ConnCounts {
    replica_connections: usize,
    client_connections: usize,
}

impl ConnCounts {
    fn get_connections_to_node(&self, my_id: NodeId, other_id: NodeId, first_cli: NodeId) -> usize {
        if my_id < first_cli && other_id < first_cli {
            self.replica_connections
        } else {
            self.client_connections
        }
    }
}

/// Represents a connection between two peers
/// We can have multiple underlying tcp connections for a given connection between two peers
pub struct PeerConnection<M: Serializable + 'static> {
    node_connections: Arc<PeerConnections<M>>,

    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The channel used to send serialized messages to the tasks that are meant to handle them
    tx: ChannelMixedTx<SerializedMessage>,
    // The RX handle corresponding to the tx channel above. This is so we can quickly associate new
// TX connections to a given connection, as we just have to clone this handle
    rx: ChannelMixedRx<SerializedMessage>,
    // Counter to assign unique IDs to each of the underlying Tcp streams
    conn_id_generator: AtomicU32,
    // A map to manage the currently active connections and a cached size value to prevent
// concurrency for simple length checks
    active_connection_count: AtomicUsize,
    active_connections: Mutex<BTreeMap<u32, ConnHandle>>,
}

#[derive(Clone)]
pub struct ConnHandle {
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
    pub fn new_peer(node_conns: Arc<PeerConnections<M>>, client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Arc<Self> {
        let (tx, rx) = new_bounded_mixed(TX_CONNECTION_QUEUE);

        Arc::new(Self {
            node_connections: node_conns,
            client,
            tx,
            rx,
            conn_id_generator: AtomicU32::new(0),
            active_connection_count: AtomicUsize::new(0),
            active_connections: Mutex::new(BTreeMap::new()),
        })
    }

    /// Send a message through this connection. Only valid for peer connections
    pub(crate) fn peer_message(&self, msg: WireMessage, callback: Callback) -> Result<()> {
        if let Err(_) = self.tx.send((msg, callback)) {
            //TODO: There are no TX connections available. Close this connection
        }

        Ok(())
    }

    async fn peer_msg_return_async(&self, msg: WireMessage, callback: Callback) -> Result<()> {
        let send = self.tx.clone();

        if let Err(_) = send.send_async((msg, callback)).await {
            return Err(Error::simple(ErrorKind::Communication));
        }

        Ok(())
    }

    /// Initialize a new Connection Handle for a new TCP Socket
    fn init_conn_handle(&self) -> ConnHandle {
        let conn_id = self.conn_id_generator.fetch_add(1, Ordering::Relaxed);

        ConnHandle {
            id: conn_id,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get the current connection count for this connection
    pub fn connection_count(&self) -> usize {
        self.active_connection_count.load(Ordering::Relaxed)
    }

    /// Accept a TCP Stream and insert it into this connection
    pub(crate) fn insert_new_connection(self: &Arc<Self>, socket: (SecureWriteHalf, SecureReadHalf), conn_limit: usize) {
        let conn_handle = self.init_conn_handle();

        let previous = {
            //Store the connection in the map
            let mut guard = self.active_connections.lock().unwrap();

            guard.insert(conn_handle.id, conn_handle.clone())
        };

        if let None = previous {
            let current_connections = self.active_connection_count.fetch_add(1, Ordering::Relaxed);

            if current_connections > conn_limit {
                self.active_connection_count.fetch_sub(1, Ordering::Relaxed);

                return;
            }
        } else {
            todo!("How do we handle this really?
                        This probably means it went all the way around u32 connections, really weird")
        }

        //Spawn the corresponding handlers for each side of the connection
        outgoing::spawn_outgoing_task_handler(conn_handle.clone(), Arc::clone(self), socket.0);
        incoming::spawn_incoming_task_handler(conn_handle, Arc::clone(self), socket.1);
    }

    /// Delete a given tcp stream from this connection
    pub(crate) fn delete_connection(&self, conn_id: u32) -> usize {
        // Remove the corresponding connection from the map
        let conn_handle = {
            // Do it inside a tiny scope to minimize the time the mutex is accessed
            let mut guard = self.active_connections.lock().unwrap();

            guard.remove(&conn_id)
        };

        if let Some(conn_handle) = conn_handle {
            let conn_count = self.active_connection_count.fetch_sub(1, Ordering::Relaxed);

            //Setting the cancelled variable to true causes all associated threads to be
            //killed (as soon as they see the warning)
            conn_handle.cancelled.store(true, Ordering::Relaxed);

            conn_count
        } else {
            self.active_connection_count.load(Ordering::Relaxed)
        }
    }

    /// Get the handle to the client pool buffer
    fn client_pool_peer(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        &self.client
    }

    /// Get the handle to the receiver for transmission
    fn to_send_handle(&self) -> &ChannelMixedRx<SerializedMessage> {
        &self.rx
    }
}

/// Stores all of the connections that this peer currently has established.
#[derive(Clone)]
pub struct PeerConnections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    concurrent_conn: ConnCounts,
    address_management: IntMap<PeerAddr>,
    connection_map: Arc<DashMap<NodeId, Arc<PeerConnection<M>>>>,
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
    connection_establisher: Arc<ConnectionHandler>,
}

impl<M: Serializable + 'static> NodeConnections for PeerConnections<M> {
    fn is_connected_to_node(&self, node: &NodeId) -> bool {
        self.connection_map.contains_key(node)
    }

    fn connected_nodes(&self) -> usize {
        self.connection_map.len()
    }

    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> OneShotRx<Result<()>> {
        let option = self.address_management.get(node.0 as u64);

        match option {
            None => {
                let (tx, rx) = new_oneshot_channel();

                tx.send(Err(Error::simple(ErrorKind::CommunicationPeerNotFound))).unwrap();

                rx
            }
            Some(addr) => {
                self.connection_establisher.connect_to_node(self, node, addr.clone())
            }
        }
    }

    async fn disconnect_from_node(&self, node: &NodeId) -> Result<()> {
        if let Some((id, conn)) = self.connection_map.remove(node) {
            todo!();

            Ok(())
        } else {
            Err(Error::simple(ErrorKind::CommunicationPeerNotFound))
        }
    }
}

impl<M: Serializable + 'static> PeerConnections<M> {
    pub fn new(peer_id: NodeId, first_cli: NodeId,
               addrs: IntMap<PeerAddr>,
               node_connector: TlsNodeConnector,
               node_acceptor: TlsNodeAcceptor,
               client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>) -> Arc<Self> {
        let connection_establish = ConnectionHandler::new(peer_id, first_cli, node_connector, node_acceptor);

        Arc::new(Self {
            id: peer_id,
            first_cli,
            address_management: addrs,
            connection_map: Arc::new(DashMap::new()),
            client_pooling,
            connection_establisher: connection_establish,
        })
    }

    /// Setup a tcp listener inside this peer connections object.
    pub(super) fn setup_tcp_listener(self: Arc<Self>, node_acceptor: NodeConnectionAcceptor) {
        self.connection_establisher.clone().setup_conn_worker(node_acceptor, self)
    }

    /// Get the current amount of concurrent TCP connections between nodes
    pub fn current_connection_count_of(&self, node: &NodeId) -> Option<usize> {
        self.connection_map.get(node).map(|connection| {
            connection.value().connection_count()
        })
    }

    /// Get the connection to a given node
    pub fn get_connection(&self, node: &NodeId) -> Option<Arc<PeerConnection<M>>> {
        let option = self.connection_map.get(node);

        option.map(|conn| conn.value().clone())
    }

    /// Handle a new connection being established (either through a "client" or a "server" connection)
    /// This will either create the corresponding peer connection or add the connection to the already existing
    /// connection
    pub(crate) fn handle_connection_established(self: &Arc<Self>, node: NodeId, socket: (SecureWriteHalf, SecureReadHalf)) {
        let option = self.connection_map.entry(node);

        let peer_conn = option.or_insert_with(||
            { PeerConnection::new_peer(Arc::clone(self), self.client_pooling.init_peer_conn(node)) });

        let concurrency_level = self.concurrent_conn.get_connections_to_node(self.id, node, self.first_cli);

        peer_conn.insert_new_connection(socket, concurrency_level);
    }

    /// Handle us losing a TCP connection to a given node.
    /// Also accepts the current amount of connections available in that node
    fn handle_conn_lost(self: &Arc<Self>, node: &NodeId, remaining_conns: usize) {
        let concurrency_level = self.concurrent_conn.get_connections_to_node(self.id, node.clone(), self.first_cli);

        if remaining_conns > 0 {
            //The node is no longer accessible. We will remove it until a new TCP connection
            // Has been established
            let option = self.connection_map.remove(node);



        }

        if remaining_conns < concurrency_level {
            let addr = self.address_management.get(node.0 as u64).unwrap();

            for i in 0..concurrency_level - remaining_conns {
                self.connection_establisher.connect_to_node(self, node.clone(), addr.clone())
            }
        }
    }
}
