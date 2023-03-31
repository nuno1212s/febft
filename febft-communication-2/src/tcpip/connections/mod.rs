use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

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

pub type SerializedMessage = WireMessage;

pub const CONNECTION_QUEUE: usize = 1024;

/// Represents a connection between two peers
/// We can have multiple underlying tcp connections for a given connection between two peers
#[derive(Clone)]
pub struct PeerConnection<M: Serializable + 'static> {
    //The client we are connected to
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The channel used to send serialized messages to the tasks that are meant to handle them
    tx: ChannelMixedTx<SerializedMessage>,
    // Counter to know how many underlying Tcp streams are currently allocated to this connection
    active_connections: Arc<AtomicU32>,
    // The RX handle corresponding to the tx channel above. This is so we can quickly associate new
    // TX connections to a given connection, as we just have to clone this handle
    rx: ChannelMixedRx<SerializedMessage>,
}

pub(crate) struct ConnHandle {
    id: u32,
    cancelled: AtomicBool,
}

impl<M> PeerConnection<M> where M: Serializable {
    pub fn new(client: Arc<ConnectedPeer<NetworkMessage<M>>>) -> Self {
        let (tx, rx) = new_bounded_mixed(CONNECTION_QUEUE);

        Self {
            client,
            tx,
            rx,
            active_connections: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn send_message(&self, msg: WireMessage) -> Result<()> {
        if let Err(_) = self.tx.send(msg) {
            //TODO: There are no TX connections available. Close this connection
        }

        Ok(())
    }

    pub fn insert_new_connection(&self, socket: (SecureWriteHalf, SecureReadHalf)) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        //Spawn the corresponding handlers for each side of the connection
        outgoing::spawn_outgoing_task_handler(self.rx.clone(), socket.0);
        incoming::spawn_incoming_task_handler(self.client.clone(), socket.1);
    }

    //TODO: Handle disconnections
}

/// Stores all of the connections that this peer currently has established.
#[derive(Clone)]
pub struct PeerConnections<M: Serializable + 'static> {
    connection_map: Arc<DashMap<NodeId, PeerConnection<M>>>,
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
            connection_map: Arc::new(DashMap::new()),
            client_pooling,
            connection_establisher: connection_establish,
        })
    }

    pub fn connection_count(&self) -> usize {
        self.connection_map.len()
    }

    pub(super) fn setup_tcp_listener(self: Arc<Self>, node_acceptor: NodeConnectionAcceptor) {
        self.connection_establisher.clone().setup_conn_worker(node_acceptor, self)
    }

    pub fn is_connected_to(&self, node: &NodeId) -> bool {
        self.connection_map.contains_key(node)
    }

    pub fn get_connection(&self, node: &NodeId) -> Option<PeerConnection<M>> {
        let option = self.connection_map.get(node);

        option.map(|conn| conn.value()).cloned()
    }

    pub(crate) fn handle_connection_established(&self, node: NodeId, socket: (SecureWriteHalf, SecureReadHalf)) {
        let option = self.connection_map.entry(node);

        let peer_conn = option.or_insert_with(||
            { PeerConnection::new(self.client_pooling.init_peer_conn(node)) });

        peer_conn.insert_new_connection(socket);
    }
}
