pub mod conn_establish;
pub mod outgoing;
pub mod incoming;
mod ping_handler;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;
use dashmap::DashMap;
use intmap::IntMap;
use log::{debug, error, warn};
use febft_common::channel::{ChannelMixedRx, ChannelMixedTx, new_bounded_mixed, new_oneshot_channel, OneShotRx};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::socket::{SecureSocket, SecureSocketAsync};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, WireMessage};
use crate::NodeConnections;
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::conn_establish::ConnectionHandler;
use crate::tcp_ip_simplex::connections::ping_handler::PingHandler;
use crate::tcpip::connections::{Callback, ConnCounts, ConnHandle, NetworkSerializedMessage};
use crate::tcpip::{NodeConnectionAcceptor, PeerAddr, TlsNodeAcceptor, TlsNodeConnector};

/// How many slots the outgoing queue has for messages.
const TX_CONNECTION_QUEUE: usize = 1024;

pub struct SimplexConnections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    address_map: IntMap<PeerAddr>,
    conn_counts: ConnCounts,
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
    connection_map: DashMap<NodeId, Arc<PeerConnection<M>>>,
    connection_establishing: Arc<ConnectionHandler>,
    ping_handler: Arc<PingHandler>,
}

pub struct PeerConnection<M: Serializable + 'static> {
    peer_node_id: NodeId,
    // Connections of this given node
    node_connections: Arc<SimplexConnections<M>>,
    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The channel used to send serialized messages to the tasks that are meant to handle them
    tx: ChannelMixedTx<NetworkSerializedMessage>,
    // The RX handle corresponding to the tx channel above. This is so we can quickly associate new
    // TX connections to a given connection, as we just have to clone this handle
    rx: ChannelMixedRx<NetworkSerializedMessage>,
    // Counter to assign unique IDs to each of the underlying Tcp streams
    conn_id_generator: AtomicU32,
    // Controls the incoming connections
    outgoing_connections: Connections,
    // Controls the outgoing connections
    incoming_connections: Connections,
}

/// The connections of a given node (Only represent one way)
pub struct Connections {
    // A map to manage the currently active connections and a cached size value to prevent
    // concurrency for simple length checks
    active_connection_count: AtomicUsize,
    active_connections: Mutex<BTreeMap<u32, ConnHandle>>,
}

/// Which direction is a certain connection going in
enum ConnectionDirection {
    Incoming,
    Outgoing,
}

impl<M> NodeConnections for SimplexConnections<M> where M: Serializable + 'static {
    fn is_connected_to_node(&self, node: &NodeId) -> bool {
        self.connection_map.contains_key(node)
    }

    fn connected_nodes_count(&self) -> usize {
        self.connection_map.len()
    }

    fn connected_nodes(&self) -> Vec<NodeId> {
        self.connection_map.iter().map(|val| {
            *val.key()
        }).collect()
    }

    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<Result<()>>> {
        let option = self.address_map.get(node.0 as u64);

        match option {
            None => {
                let (tx, rx) = new_oneshot_channel();

                tx.send(Err(Error::simple(ErrorKind::CommunicationPeerNotFound))).unwrap();

                vec![rx]
            }
            Some(addr) => {
                let conns_to_have = self.conn_counts.get_connections_to_node(self.id, node, self.first_cli);
                let connections = self.current_connection_count_of(&node).unwrap_or(0);

                let mut oneshots = Vec::with_capacity(conns_to_have);

                for _ in connections..conns_to_have {
                    oneshots.push(self.connection_establishing.connect_to_node(self, node, addr.clone()));
                }

                oneshots
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

impl<M> SimplexConnections<M> where M: Serializable + 'static {

    pub fn new(peer_id: NodeId, first_cli: NodeId,
               conn_counts: ConnCounts,
               addrs: IntMap<PeerAddr>,
               node_connector: TlsNodeConnector,
               node_acceptor: TlsNodeAcceptor,
               client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>) -> Arc<Self> {
        let connection_establish = ConnectionHandler::new(peer_id, first_cli, conn_counts.clone(),
                                                          node_connector, node_acceptor);

        Arc::new(Self {
            id: peer_id,
            first_cli,
            address_map: addrs,
            connection_map: DashMap::new(),
            connection_establishing: connection_establish,
            client_pooling,
            conn_counts,
            ping_handler: PingHandler::new(),
        })
    }
    
    /// Setup a tcp listener inside this peer connections object.
    pub(super) fn setup_tcp_listener(self: Arc<Self>, node_acceptor: NodeConnectionAcceptor) {
        self.connection_establishing.clone().setup_conn_worker(node_acceptor, self)
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

    /// Handle the connection being established
    fn handle_connection_established(self: &Arc<Self>, peer_id: NodeId, direction: ConnectionDirection, socket: SecureSocket) {
        debug!("{:?} // Handling established connection to {:?}", self.id, peer_id);

        let option = self.connection_map.entry(peer_id);

        let peer_conn = option.or_insert_with(||
            {
                let con = PeerConnection::new_peer(Arc::clone(self), self.client_pooling.init_peer_conn(peer_id));

                debug!("{:?} // Creating new peer connection to {:?}. {:?}", self.id, peer_id,
                    con.client_pool_peer().client_id());

                con
            });

        let concurrency_level = self.conn_counts.get_connections_to_node(self.id, peer_id, self.first_cli);

        match direction {
            ConnectionDirection::Incoming => {
                // if we are the ones receiving the connection, then we have to attempt to also establish our TX side
                let mut current_outgoing_connections = peer_conn.outgoing_connection_count();

                while current_outgoing_connections < concurrency_level {
                    let addr = self.address_map.get(peer_id.0 as u64).unwrap();

                    let _ = self.connection_establishing.connect_to_node(self, peer_id, addr.clone());

                    current_outgoing_connections += 1;
                }
            }
            ConnectionDirection::Outgoing => {}
        }

        peer_conn.insert_new_connection(&self.ping_handler, socket, direction, concurrency_level);
    }

    /// Handle a connection that has been lost
    fn handle_conn_lost(self: &Arc<Self>, node: NodeId, remaining_conns: usize) {
        let concurrency_level = self.conn_counts.get_connections_to_node(self.id, node.clone(), self.first_cli);

        if remaining_conns <= 0 {
            //The node is no longer accessible. We will remove it until a new TCP connection
            // Has been established
            let _ = self.connection_map.remove(&node);
        }

        // Attempt to re-establish all of the missing connections
        if remaining_conns < concurrency_level {
            let addr = self.address_map.get(node.0 as u64).unwrap();

            for _ in 0..concurrency_level - remaining_conns {
                self.connection_establishing.connect_to_node(self, node.clone(), addr.clone());
            }
        }
    }
}

impl<M> PeerConnection<M> where M: Serializable + 'static {
    pub fn new_peer(node_conns: Arc<SimplexConnections<M>>, client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Arc<Self> {
        let (tx, rx) = new_bounded_mixed(TX_CONNECTION_QUEUE);

        Arc::new(Self {
            peer_node_id: client.client_id().clone(),
            node_connections: node_conns,
            client,
            tx,
            rx,
            conn_id_generator: AtomicU32::new(0),
            outgoing_connections: Connections { active_connection_count: AtomicUsize::new(0), active_connections: Mutex::new(Default::default()) },
            incoming_connections: Connections { active_connection_count: AtomicUsize::new(0), active_connections: Mutex::new(Default::default()) },
        })
    }

    /// Get the amount of connections to this connected node
    fn connection_count(&self) -> usize {
        let active_incoming_conns = self.incoming_connections.active_connection_count.load(Ordering::Relaxed);

        let active_outgoing_conns = self.outgoing_connections.active_connection_count.load(Ordering::Relaxed);

        active_incoming_conns + active_outgoing_conns
    }

    /// Get the current amount of incoming connections
    fn incoming_connection_count(&self) -> usize {
        self.incoming_connections.active_connection_count.load(Ordering::Relaxed)
    }

    /// Get the current amount of outgoing connections
    fn outgoing_connection_count(&self) -> usize {
        self.outgoing_connections.active_connection_count.load(Ordering::Relaxed)
    }

    /// Insert a new connection
    fn insert_new_connection(self: &Arc<Self>, ping_handler: &Arc<PingHandler>, socket: SecureSocket, direction: ConnectionDirection, conn_limit: usize) {
        let conn_id = self.conn_id_generator.fetch_add(1, Ordering::Relaxed);

        let conn_handle = ConnHandle::new(conn_id, self.node_connections.id);

        let mut conns = match direction {
            ConnectionDirection::Incoming => &self.incoming_connections,
            ConnectionDirection::Outgoing => &self.outgoing_connections
        };

        {
            let mut active_conns = conns.active_connections.lock().unwrap();

            active_conns.insert(conn_id, conn_handle.clone());
        }

        conns.active_connection_count.fetch_add(1, Ordering::Relaxed);

        match direction {
            ConnectionDirection::Incoming => {
                incoming::spawn_incoming_task_handler(conn_handle, Arc::clone(self), socket)
            }
            ConnectionDirection::Outgoing => {
                outgoing::spawn_outgoing_task_handler(conn_handle, Arc::clone(self), Arc::clone(ping_handler), socket);
            }
        }

        debug!("{:?} // Inserted new connection {:?} to {:?}. Current connection count: {:?}",
            self.peer_node_id, conn_id, self.peer_node_id, self.connection_count());
    }

    /// Delete a connection from this peers connection map
    fn delete_connection(&self, conn_id: u32, direction: ConnectionDirection) -> usize {
        // Remove the corresponding connection from the map
        let conn_handle = {
            let mut active_connections = match direction {
                ConnectionDirection::Incoming => self.incoming_connections.active_connections.lock(),
                ConnectionDirection::Outgoing => self.outgoing_connections.active_connections.lock()
            }.unwrap();

            // Do it inside a tiny scope to minimize the time the mutex is accessed
            active_connections.remove(&conn_id)
        };

        let active_connections = match direction {
            ConnectionDirection::Incoming => &self.incoming_connections.active_connection_count,
            ConnectionDirection::Outgoing => &self.outgoing_connections.active_connection_count
        };

        let remaining_conns = if let Some(conn_handle) = conn_handle {
            let conn_count = active_connections.fetch_sub(1, Ordering::Relaxed);

            //Setting the cancelled variable to true causes all associated threads to be
            //killed (as soon as they see the warning)
            conn_handle.cancelled.store(true, Ordering::Relaxed);

            conn_count
        } else {
            active_connections.load(Ordering::Relaxed)
        };

        // Retry to establish the connections if possible
        self.node_connections.handle_conn_lost(self.peer_node_id, remaining_conns);

        warn!("{:?} // Connection {} with peer {:?} has been deleted", self.node_connections.id,
            conn_id,self.peer_node_id);

        remaining_conns
    }

    /// Send a message through this connection. Only valid for peer connections
    pub(crate) fn peer_message(&self, msg: WireMessage, callback: Callback, should_flush: bool, send_rq_time: Instant) -> Result<()> {
        let from = msg.header().from();
        let to = msg.header().to();

        if let Err(_) = self.tx.send((msg, callback, Instant::now(), should_flush, send_rq_time)) {
            error!("{:?} // Failed to send peer message to {:?}", from,
                to);

            return Err(Error::simple(ErrorKind::Communication));
        }

        Ok(())
    }

    async fn peer_msg_return_async(&self, to_send: NetworkSerializedMessage) -> Result<()> {
        let send = self.tx.clone();

        if let Err(_) = send.send_async(to_send).await {
            return Err(Error::simple(ErrorKind::Communication));
        }

        Ok(())
    }

    /// The client pool peer handle for the our peer connection
    pub fn client_pool_peer(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        &self.client
    }

    /// Get the handle to the receiver for transmission
    fn to_send_handle(&self) -> &ChannelMixedRx<NetworkSerializedMessage> {
        &self.rx
    }
}