use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_tls::{TlsAcceptor, TlsConnector};
use futures_timer::Delay;
use intmap::IntMap;
use log::debug;
use rustls::{ClientConfig, ServerConfig};

use febft_common::{async_runtime as rt, socket, threadpool};
use febft_common::error::*;
use febft_common::prng::ThreadSafePrng;
use febft_common::socket::{AsyncListener, SyncListener};

use crate::{Node, NodeId};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::config::{NodeConfig, TlsConfig};
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::message_signing::NodePKShared;
use crate::serialize::Serializable;
use crate::tcpip::connections::{PeerConnection, PeerConnections};

pub mod connections;

///Represents the server addresses of a peer
///Clients will only have 1 address while replicas will have 2 addresses (1 for facing clients,
/// 1 for facing replicas)
#[derive(Clone)]
pub struct PeerAddr {
    client_socket: (SocketAddr, String),
    replica_socket: Option<(SocketAddr, String)>,
}

impl PeerAddr {
    pub fn new(client_addr: (SocketAddr, String)) -> Self {
        Self {
            client_socket: client_addr,
            replica_socket: None,
        }
    }

    pub fn new_replica(
        client_addr: (SocketAddr, String),
        replica_addr: (SocketAddr, String),
    ) -> Self {
        Self {
            client_socket: client_addr,
            replica_socket: Some(replica_addr),
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
    // The connections that are currently being maintained by us to other peers
    peer_connections: Arc<PeerConnections<M>>,
    //Handles the incoming connections' buffering and request collection
    //This is polled by the proposer for client requests and by the
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
}


impl<M: Serializable + 'static> TcpNode<M> {
    async fn setup_client_facing_socket<T>(
        id: NodeId,
        addr: PeerAddr,
    ) -> Result<NodeConnectionAcceptor> where T: ConnectionType {
        debug!("{:?} // Attempt to setup client facing socket.", id);
        let server_addr = &addr.client_socket;

        T::setup_socket(&id, &server_addr.0).await
    }

    async fn setup_replica_facing_socket<T>(
        id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<Option<NodeConnectionAcceptor>>
        where T: ConnectionType {

        if let Some((socket, _)) = peer_addr.replica_socket {
            Ok(Some(T::setup_socket(&id, &socket).await?))
        } else {
            Ok(None)
        }
    }

    async fn setup_network<CT>(id: NodeId, addr: PeerAddr, cfg: TlsConfig) ->
    (TlsNodeConnector, TlsNodeAcceptor, Result<NodeConnectionAcceptor>, Result<Option<NodeConnectionAcceptor>>)
        where CT: ConnectionType
    {
        debug!("Initializing TLS configurations.");

        let async_acceptor: TlsAcceptor = cfg.async_server_config.into();
        let async_connector: TlsConnector = cfg.async_client_config.into();

        let sync_acceptor = Arc::new(cfg.sync_server_config);
        let sync_connector = Arc::new(cfg.sync_client_config);

        let connector = CT::setup_connector(sync_connector, async_connector);

        let acceptor = CT::setup_acceptor(sync_acceptor, async_acceptor);

        //Initialize the client facing server
        let client_listener = Self::setup_client_facing_socket::<AsyncConn>(id, addr.clone()).await;

        let replica_listener = Self::setup_replica_facing_socket::<AsyncConn>(id, addr.clone()).await;

        (connector, acceptor, client_listener, replica_listener)
    }
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

pub struct SyncConn;
pub struct AsyncConn;

impl ConnectionType for SyncConn {
    fn setup_connector(sync_connector: Arc<ClientConfig>, _: TlsConnector) -> TlsNodeConnector {
        TlsNodeConnector::Sync(sync_connector)
    }

    fn setup_acceptor(sync_acceptor: Arc<ServerConfig>, _: TlsAcceptor) -> TlsNodeAcceptor {
        TlsNodeAcceptor::Sync(sync_acceptor)
    }

    async fn setup_socket(id: &NodeId, server_addr: &SocketAddr) -> Result<NodeConnectionAcceptor> {
        Ok(NodeConnectionAcceptor::Sync(socket::bind_sync_server(server_addr.clone())?))
    }
}

impl ConnectionType for AsyncConn {
    fn setup_connector(_: Arc<ClientConfig>, async_connector: TlsConnector) -> TlsNodeConnector {
        TlsNodeConnector::Async(async_connector)
    }

    fn setup_acceptor(_: Arc<ServerConfig>, async_acceptor: TlsAcceptor) -> TlsNodeAcceptor {
        TlsNodeAcceptor::Async(async_acceptor)
    }

    async fn setup_socket(id: &NodeId, server_addr: &SocketAddr) -> Result<NodeConnectionAcceptor> {
        Ok(NodeConnectionAcceptor::Async(socket::bind_async_server(server_addr.clone()).await?))
    }
}

impl<M: Serializable + 'static> Node<M> for TcpNode<M> {
    async fn bootstrap(cfg: NodeConfig) -> Result<Arc<Self>> {
        let id = cfg.id;

        debug!("Initializing sockets.");

        let tcp_config = cfg.tcp_config;

        let addr = tcp_config.addrs.get(id.0 as u64).expect("Failed to get my own IP address").clone();

        let network = tcp_config.network_config;

        let (connector, acceptor,
            client_socket, replica_socket) =
            Self::setup_network(id, addr, network).await;

        //Setup all the peer message reception handling.
        let peers = Arc::new(PeerIncomingRqHandling::new(
            cfg.id,
            cfg.first_cli,
            cfg.client_pool_config,
        ));

        let peer_connections = PeerConnections::new(id, cfg.first_cli,
                                                    connector, acceptor, peers.clone());


        debug!("Initializing connection listeners");
        peer_connections.clone().setup_tcp_listener(client_socket?);

        if let Some(replica) = replica_socket? {
            peer_connections.clone().setup_tcp_listener(replica);
        }

        let shared = NodePKShared::from_config(cfg.pk_crypto_config);

        let rng = ThreadSafePrng::new();

        debug!("{:?} // Initializing node reference", id);

        let node = Arc::new(TcpNode {
            id,
            first_cli: cfg.first_cli,
            rng,
            peer_connections: peer_connections,
            client_pooling: peers,
        });

        // success
        Ok(node)
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn first_cli(&self) -> NodeId {
        self.first_cli
    }

    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) {
        match self.peer_connections.get_connection(&target) {
            None => {}
            Some(connection) => {
                
            }
        }
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