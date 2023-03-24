use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use async_tls::{TlsAcceptor, TlsConnector};
use intmap::IntMap;
use log::debug;
use rustls::{ClientConfig, ServerConfig};
use febft_common::prng::ThreadSafePrng;
use febft_common::socket::{AsyncListener, SyncListener};
use febft_common::error::*;
use febft_common::async_runtime as rt;
use crate::{Node, NodeId};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::serialize::Serializable;

pub mod connections; 

///Represents the server addresses of a peer
///Clients will only have 1 address while replicas will have 2 addresses (1 for facing clients,
/// 1 for facing replicas)
pub struct PeerAddr {
    replica_facing_socket: (SocketAddr, String),
    client_facing_socket: Option<(SocketAddr, String)>,
}

impl PeerAddr {
    pub fn new(client_addr: (SocketAddr, String)) -> Self {
        Self {
            replica_facing_socket: client_addr,
            client_facing_socket: None,
        }
    }

    pub fn new_replica(
        client_addr: (SocketAddr, String),
        replica_addr: (SocketAddr, String),
    ) -> Self {
        Self {
            replica_facing_socket: client_addr,
            client_facing_socket: Some(replica_addr),
        }
    }
}

/// The connection type used for connections
/// Stores the connector needed
#[derive(Clone)]
pub enum TlsNodeConnector {
    Async(TlsConnector),
    Sync(Arc<ClientConfig>),
}

/// Establish safe tls node connections
#[derive(Clone)]
pub enum TlsNodeAcceptor {
    Async(TlsAcceptor),
    Sync(Arc<ServerConfig>),
}

/// Accept node connections
pub enum NodeConnectionAcceptor {
    Async(AsyncListener),
    Sync(SyncListener),
}

/// The node based on the TCP/IP protocol stack
pub struct TcpNode<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The thread safe pseudo random number generator
    rng: ThreadSafePrng,
    // The connector used to establish connections to other nodes
    connector: TlsNodeConnector,
    //Handles the incoming connections' buffering and request collection
    //This is polled by the proposer for client requests and by the
    client_pooling: PeerIncomingRqHandling<NetworkMessage<M>>,
    //A set of the nodes we are currently attempting to connect to
    currently_connecting: Mutex<BTreeSet<NodeId>>,
}

pub trait ConnectionType {
    fn setup_connector(
        sync_connector: Arc<ClientConfig>,
        async_connector: TlsConnector) -> TlsNodeConnector;

    fn setup_acceptor(
        sync_acceptor: Arc<ServerConfig>,
        async_acceptor: TlsAcceptor, ) -> TlsNodeAcceptor;

    async fn setup_socket(
        id: &NodeId,
        server_addr: &SocketAddr, ) -> Result<NodeConnectionAcceptor>;
}

impl<M: Serializable + 'static> TcpNode<M> {
    async fn setup_client_facing_socket<T>(
        id: NodeId,
        cfg: &NodeConfig,
    ) -> Result<NodeConnectionAcceptor> where T: ConnectionType {
        debug!("{:?} // Attempt to setup client facing socket.", id);

        let peer_addr = cfg.addrs.get(id.into()).ok_or(Error::simple_with_msg(
            ErrorKind::Communication,
            "Failed to get client facing IP",
        ))?;

        let server_addr = &peer_addr.client_addr;

        T::setup_socket(&id, &server_addr.0).await
    }

    async fn setup_replica_facing_socket<T>(
        id: NodeId,
        cfg: &NodeConfig,
    ) -> Result<Option<NodeConnectionAcceptor>>
        where T: ConnectionType {
        let peer_addr = cfg.addrs.get(id.into()).ok_or(Error::simple_with_msg(
            ErrorKind::Communication,
            "Failed to get replica facing IP",
        ))?;

        //Initialize the replica<->replica facing server
        let replica_listener = if id >= cfg.first_cli {
            //Clients don't have a replica<->replica facing server
            None
        } else {
            let replica_facing_addr =
                peer_addr
                    .replica_addr
                    .as_ref()
                    .ok_or(Error::simple_with_msg(
                        ErrorKind::Communication,
                        "Failed to get replica facing IP",
                    ))?;

            Some(T::setup_socket(&id, &replica_facing_addr.0).await?)
        };

        Ok(replica_listener)
    }

    /// Sets up the thread or task (depending on runtime) to receive new connection attempts
    fn setup_socket_connection_workers_socket(
        self: Arc<Self>,
        node_connector: TlsNodeAcceptor,
        listener: NodeConnectionAcceptor,
    ) {
        match (node_connector, listener) {
            (TlsNodeAcceptor::Sync(cfg), NodeConnectionAcceptor::Sync(sync_listener)) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let sync_acceptor = cfg.clone();

                std::thread::Builder::new()
                    .name(format!("{:?} connection acceptor", self.id()))
                    .spawn(move || {
                        self.rx_side_accept_sync(first_cli, my_id, sync_listener, sync_acceptor);
                    })
                    .expect("Failed to start replica's connection acceptor");
            }
            (TlsNodeAcceptor::Async(cfg), NodeConnectionAcceptor::Async(async_listener)) => {
                let first_cli = self.first_client_id();

                let my_id = self.id();

                let async_acceptor = cfg.clone();

                rt::spawn(self.rx_side_accept(first_cli, my_id, async_listener, async_acceptor));
            }
            (_, _) => {
                unreachable!()
            }
        }
    }
}

impl<M: Serializable + 'static> Node<M> for TcpNode<M> {
    async fn bootstrap() -> Result<(Arc<Self>, Vec<NetworkMessage<M>>)> {
        todo!()
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn first_cli(&self) -> NodeId {
        self.first_cli
    }

    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) {
        todo!()
    }

    fn send_signed(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) {
        todo!()
    }

    fn broadcast(&self, message: NetworkMessageKind<M>, targets: impl Iterator<Item=NodeId>) {
        todo!()
    }

    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item=NodeId>) {
        todo!()
    }

    fn broadcast_serialized(&self, messages: IntMap<StoredSerializedNetworkMessage<M>>) {
        todo!()
    }

    fn loopback_channel(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        self.client_pooling.loopback_connection()
    }

    fn receive_from_clients(&self, timeout: Option<Duration>) -> Result<Vec<NetworkMessage<M>>> {
        self.client_pooling.receive_from_clients(timeout)
    }

    fn try_recv_from_clients(&self) -> Result<Option<Vec<NetworkMessage<M>>>> {
        self.client_pooling.try_receive_from_clients()
    }

    fn receive_from_replicas(&self) -> Result<NetworkMessage<M>> {
        self.client_pooling.receive_from_replicas()
    }
}