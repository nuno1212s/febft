use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use either::Either;
use febft_common::channel::OneShotRx;
use febft_common::socket::{AsyncSocket, SyncSocket};
use febft_common::error::*;
use crate::NodeId;
use crate::serialize::Serializable;
use crate::tcpip::{NodeConnectionAcceptor, PeerAddr, TlsNodeAcceptor, TlsNodeConnector};
use crate::tcpip::connections::{ConnCounts, PeerConnection, PeerConnections};

mod synchronous;
mod asynchronous;

/// Connection handler
pub struct ConnectionHandler {
    peer_id: NodeId,
    first_cli: NodeId,
    connector: TlsNodeConnector,
    tls_acceptor: TlsNodeAcceptor,
    concurrent_conn: ConnCounts,
    currently_connecting: Mutex<BTreeMap<NodeId, usize>>,
}

impl ConnectionHandler {
    pub fn new(peer_id: NodeId, first_cli: NodeId,
               conn_counts: ConnCounts,
               node_connector: TlsNodeConnector, node_acceptor: TlsNodeAcceptor) -> Arc<Self> {
        Arc::new(
            ConnectionHandler {
                peer_id,
                first_cli,
                connector: node_connector,
                tls_acceptor: node_acceptor,
                concurrent_conn: conn_counts,
                currently_connecting: Mutex::new(Default::default()),
            }
        )
    }

    pub(super) fn setup_conn_worker<M: Serializable + 'static>(self: Arc<Self>,
                                                               listener: NodeConnectionAcceptor,
                                                               peer_connections: Arc<PeerConnections<M>>) {
        match listener {
            NodeConnectionAcceptor::Async(async_listener) => {
                asynchronous::setup_conn_acceptor_task(async_listener, self, peer_connections)
            }
            NodeConnectionAcceptor::Sync(sync_listener) => {
                synchronous::setup_conn_acceptor_thread(sync_listener, self, peer_connections)
            }
        }
    }

    pub fn id(&self) -> NodeId {
        self.peer_id
    }

    pub fn first_cli(&self) -> NodeId {
        self.first_cli
    }

    fn register_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let mut connecting_guard = self.currently_connecting.lock().unwrap();

        let value = connecting_guard.entry(peer_id).or_insert(0);

        *value += 1;

        if *value > self.concurrent_conn.get_connections_to_node(self.id(), peer_id, self.first_cli) {
            *value -= 1;

            false
        } else {
            true
        }
    }

    fn done_connecting_to_node(&self, peer_id: &NodeId) {
        let mut connection_guard = self.currently_connecting.lock().unwrap();

        connection_guard.entry(peer_id.clone());
    }

    pub fn connect_to_node<M: Serializable + 'static>(self: &Arc<Self>, peer_connections: &Arc<PeerConnections<M>>,
                                                      peer_id: NodeId, peer_addr: PeerAddr) -> OneShotRx<Result<()>> {
        match &self.connector {
            TlsNodeConnector::Async(_) => {
                asynchronous::connect_to_node_async(Arc::clone(self),
                                                    Arc::clone(&peer_connections),
                                                    peer_id, peer_addr)
            }
            TlsNodeConnector::Sync(_) => {
                synchronous::connect_to_node_sync(Arc::clone(self),
                                                  Arc::clone(&peer_connections),
                                                  peer_id, peer_addr)
            }
        }
    }

    pub fn accept_conn<M: Serializable + 'static>(self: &Arc<Self>, peer_connections: &Arc<PeerConnections<M>>, socket: Either<AsyncSocket, SyncSocket>) {
        match socket {
            Either::Left(asynchronous) => {
                asynchronous::handle_server_conn_established(Arc::clone(self),
                                                             peer_connections.clone(),
                                                             asynchronous, );
            }
            Either::Right(synchronous) => {
                synchronous::handle_server_conn_established(Arc::clone(self),
                                                            peer_connections.clone(),
                                                            synchronous);
            }
        }
    }
}