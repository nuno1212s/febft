use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use log::debug;
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::prng::ThreadSafePrng;
use febft_common::socket;
use febft_common::socket::SyncListener;
use crate::client_pooling::PeerIncomingRqHandling;
use crate::config::MioConfig;
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::message_signing::{NodePKCrypto, NodePKShared};
use crate::mio_tcp::connections::Connections;
use crate::mio_tcp::connections::epoll_group::{init_worker_group_handle, initialize_worker_group};
use crate::Node;
use crate::serialize::Serializable;
use crate::tcpip::connections::ConnCounts;
use crate::tcpip::PeerAddr;

mod connections;

/// The node that handles the TCP connections
pub struct MIOTcpNode<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The thread safe random number generator
    rng: Arc<ThreadSafePrng>,
    // The keys of the node
    keys: NodePKCrypto,
    // The connections that are currently being maintained by us to other peers
    connections: Arc<Connections<M>>,
    //Handles the incoming connections' buffering and request collection
    //This is polled by the proposer for client requests and by the
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
}

impl<M: Serializable + 'static> MIOTcpNode<M> {
    fn setup_connection(id: &NodeId, server_addr: &SocketAddr) -> Result<SyncListener> {
        socket::bind_sync_server(server_addr.clone()).wrapped(ErrorKind::Communication)
    }

    fn setup_client_facing_socket(
        id: NodeId,
        addr: PeerAddr,
    ) -> Result<SyncListener> {
        debug!("{:?} // Attempt to setup client facing socket.", id);
        let server_addr = &addr.replica_facing_socket;

        Self::setup_connection(&id, &server_addr.0)
    }

    fn setup_replica_facing_socket(
        id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<Option<SyncListener>>{
        if let Some((socket, _)) = peer_addr.client_facing_socket {
            Ok(Some(Self::setup_connection(&id, &socket)?))
        } else {
            Ok(None)
        }
    }
}

impl<M: Serializable + 'static> Node<M> for MIOTcpNode<M> {
    type Config = MioConfig;
    type ConnectionManager = Connections<M>;
    type Crypto = NodePKCrypto;
    type IncomingRqHandler = PeerIncomingRqHandling<NetworkMessage<M>>;

    async fn bootstrap(node_config: Self::Config) -> febft_common::error::Result<Arc<Self>> {
        let MioConfig { node_config: cfg, worker_count } = node_config;

        let id = cfg.id;

        debug!("Initializing sockets.");

        let tcp_config = cfg.tcp_config;

        let conn_counts = ConnCounts::from_tcp_config(&tcp_config);

        let addr = tcp_config.addrs.get(id.0 as u64).expect(format!("Failed to get my own IP address ({})", id.0).as_str()).clone();

        let network = tcp_config.network_config;

        let shared = NodePKCrypto::new(NodePKShared::from_config(cfg.pk_crypto_config));

        let rng = Arc::new(ThreadSafePrng::new());

        debug!("{:?} // Initializing node reference", id);


        //Setup all the peer message reception handling.
        let peers = Arc::new(PeerIncomingRqHandling::new(
            cfg.id,
            cfg.first_cli,
            cfg.client_pool_config,
        ));

        let (handle, receivers) = init_worker_group_handle(worker_count as u32);

        let connections = Arc::new(Connections::initialize_connections(
            cfg.id,
            cfg.first_cli,
            tcp_config.addrs,
            handle.clone(),
            conn_counts.clone(),
            peers.clone(),
        )?);

        initialize_worker_group(connections.clone(), receivers)?;

        let client_listener = Self::setup_client_facing_socket(cfg.id.clone(), addr.clone())?;

        let replica_listener = Self::setup_replica_facing_socket(cfg.id.clone(), addr.clone())?;

        connections.setup_tcp_server_worker(client_listener);

        replica_listener.map(|listener| connections.setup_tcp_server_worker(listener));

        Ok(Arc::new(Self {
            id,
            first_cli: cfg.first_cli,
            rng,
            keys: shared,
            connections,
            client_pooling: peers,
        }))
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn first_cli(&self) -> NodeId {
        self.first_cli
    }

    fn node_connections(&self) -> &Arc<Self::ConnectionManager> {
        &self.connections
    }

    fn pk_crypto(&self) -> &Self::Crypto {
        &self.keys
    }

    fn node_incoming_rq_handling(&self) -> &Arc<Self::IncomingRqHandler> {
        &self.client_pooling
    }

    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> febft_common::error::Result<()> {
        todo!()
    }

    fn send_signed(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> febft_common::error::Result<()> {
        todo!()
    }

    fn broadcast(&self, message: NetworkMessageKind<M>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        todo!()
    }

    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item=NodeId>) ->  std::result::Result<(), Vec<NodeId>> {
        todo!()
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) ->  std::result::Result<(), Vec<NodeId>> {
        todo!()
    }
}