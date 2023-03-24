use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use either::Either;
use febft_common::socket::{AsyncSocket, SyncSocket};
use febft_common::error::*;
use crate::NodeId;
use crate::serialize::Serializable;
use crate::tcpip::{TlsNodeAcceptor, TlsNodeConnector};
use crate::tcpip::connections::PeerConnections;

mod synchronous;
mod asynchronous;

/// Connection handler
pub(super) struct ConnectionHandler<M: Serializable + 'static> {
    peer_id: NodeId,
    first_cli: NodeId,
    peer_connections: Arc<PeerConnections<M>>,
    connector: TlsNodeConnector,
    tls_acceptor: TlsNodeAcceptor,
    currently_connecting: Mutex<BTreeSet<NodeId>>,

}

impl<M: Serializable> ConnectionHandler<M> {
    pub fn new(peer_id: NodeId, first_cli: NodeId,
               peer_connections: Arc<PeerConnections<M>>,
               node_connector: TlsNodeConnector, node_acceptor: TlsNodeAcceptor) -> Arc<Self> {
        Arc::new(
            ConnectionHandler {
                peer_id,
                first_cli,
                connector: node_connector,
                tls_acceptor: node_acceptor,
                currently_connecting: Mutex::new(BTreeSet::new()),
                peer_connections,
            }
        )
    }

    pub fn id(&self) -> NodeId {
        self.peer_id
    }

    pub fn first_cli(&self) -> NodeId {
        self.first_cli
    }

    fn register_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let mut connecting_guard = self.currently_connecting.lock().unwrap();

        connecting_guard.insert(peer_id)
    }

    fn done_connecting_to_node(&self, peer_id: &NodeId) {
        let mut connection_guard = self.currently_connecting.lock().unwrap();

        connection_guard.remove(peer_id);
    }

    pub fn accept_conn(self: &Arc<Self>, socket: Either<(AsyncSocket), (SyncSocket)>) {
        match socket {
            Either::Left(asynchronous) => {
                asynchronous::handle_server_conn_established(Arc::clone(self),
                                                             self.peer_connections.clone(),
                                                             asynchronous, );
            }
            Either::Right(_) => {
                todo!()
            }
        }
    }
}