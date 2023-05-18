pub mod connections;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use either::Either;
use log::{debug, error};
use rustls::{ClientConfig, ServerConfig};
use smallvec::SmallVec;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use febft_common::crypto::hash::Digest;
use febft_common::node_id::NodeId;
use febft_common::prng::ThreadSafePrng;
use febft_common::error::*;
use febft_common::{socket, threadpool};
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::config::{NodeConfig, TcpConfig, TlsConfig};
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage, WireMessage};
use crate::message_signing::{NodePKCrypto, NodePKShared};
use crate::Node;
use crate::serialize::{Buf, Serializable};
use crate::tcp_ip_simplex::connections::{PeerConnection, SimplexConnections};
use crate::tcpip::connections::ConnCounts;
use crate::tcpip::{AsyncConn, ConnectionType, NodeConnectionAcceptor, PeerAddr, TlsNodeAcceptor, TlsNodeConnector};

const NODE_QUORUM_SIZE: usize = 1024;

type SendTos<M> = SmallVec<[SendTo<M>; NODE_QUORUM_SIZE]>;

pub struct TCPSimplexNode<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The thread safe pseudo random number generator
    rng: Arc<ThreadSafePrng>,
    // Our public key cryptography information
    keys: NodePKCrypto,
    // The client pooling for this node
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,

    connections: Arc<SimplexConnections<M>>
}

impl<M> TCPSimplexNode<M> where M: Serializable + 'static {

    async fn setup_client_facing_socket<T>(
        id: NodeId,
        addr: PeerAddr,
    ) -> Result<NodeConnectionAcceptor> where T: ConnectionType {
        debug!("{:?} // Attempt to setup client facing socket.", id);
        let server_addr = &addr.replica_facing_socket;

        T::setup_socket(&id, &server_addr.0).await
    }

    async fn setup_replica_facing_socket<T>(
        id: NodeId,
        peer_addr: PeerAddr,
    ) -> Result<Option<NodeConnectionAcceptor>>
        where T: ConnectionType {
        if let Some((socket, _)) = peer_addr.client_facing_socket {
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

        let async_acceptor: TlsAcceptor = Arc::new(cfg.async_server_config).into();
        let async_connector: TlsConnector = Arc::new(cfg.async_client_config).into();

        let sync_acceptor = Arc::new(cfg.sync_server_config);
        let sync_connector = Arc::new(cfg.sync_client_config);

        let connector = CT::setup_connector(sync_connector, async_connector);

        let acceptor = CT::setup_acceptor(sync_acceptor, async_acceptor);

        //Initialize the client facing server
        let client_listener = Self::setup_client_facing_socket::<CT>(id, addr.clone()).await;

        let replica_listener = Self::setup_replica_facing_socket::<CT>(id, addr.clone()).await;

        (connector, acceptor, client_listener, replica_listener)
    }


    /// Create the send tos for a given target
    fn send_tos(&self, shared: Option<&NodePKCrypto>, targets: impl Iterator<Item=NodeId>, flush: bool)
                -> (Option<SendTo<M>>, Option<SendTos<M>>, Vec<NodeId>) {
        let mut send_to_me = None;
        let mut send_tos: Option<SendTos<M>> = None;

        let mut failed = Vec::new();

        let my_id = self.id();

        let nonce = self.rng.next_state();

        for id in targets {
            if id == my_id {
                send_to_me = Some(SendTo {
                    my_id,
                    peer_id: id,
                    shared: shared.cloned(),
                    nonce,
                    peer_cnn: SendToPeer::Me(self.loopback_channel().clone()),
                    flush,
                    rq_send_time: Instant::now(),
                })
            } else {
                match self.connections.get_connection(&id) {
                    None => {
                        failed.push(id)
                    }
                    Some(conn) => {
                        if let Some(send_tos) = &mut send_tos {
                            send_tos.push(SendTo {
                                my_id,
                                peer_id: id.clone(),
                                shared: shared.cloned(),
                                nonce,
                                peer_cnn: SendToPeer::Peer(conn),
                                flush,
                                rq_send_time: Instant::now()
                            })
                        } else {
                            let mut send = SmallVec::new();

                            send.push(SendTo {
                                my_id,
                                peer_id: id.clone(),
                                shared: shared.cloned(),
                                nonce,
                                peer_cnn: SendToPeer::Peer(conn),
                                flush,
                                rq_send_time: Instant::now()
                            });

                            send_tos = Some(send)
                        }
                    }
                }
            }
        }

        (send_to_me, send_tos, failed)
    }

    fn serialize_send_impl(send_to_me: Option<SendTo<M>>, send_to_others: Option<SendTos<M>>,
                           message: NetworkMessageKind<M>) {
        threadpool::execute(move || {

            match crate::cpu_workers::serialize_digest_no_threadpool(&message) {
                Ok((buffer, digest)) => {
                    Self::send_impl(send_to_me, send_to_others, message, buffer, digest);
                }
                Err(err) => {
                    error!("Failed to serialize message {:?}", err);
                }
            }
        });
    }

    fn send_impl(send_to_me: Option<SendTo<M>>, send_to_others: Option<SendTos<M>>,
                 msg: NetworkMessageKind<M>, buffer: Buf, digest: Digest, ) {

        if let Some(send_to) = send_to_me {
            send_to.value(Either::Left((msg, buffer.clone(), digest.clone())));
        }

        if let Some(send_to) = send_to_others {
            for send in send_to {
                send.value(Either::Right((buffer.clone(), digest.clone())));
            }
        }
    }

    fn send_serialized_impl(send_to_me: Option<SendTo<M>>, send_to_others: Option<SendTos<M>>,
                            mut messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) {
        if let Some(send_to) = send_to_me {
            let message = messages.remove(&send_to.peer_id).unwrap();

            send_to.value_serialized(message);
        }

        if let Some(send_to) = send_to_others {
            for send in send_to {
                let message = messages.remove(&send.peer_id).unwrap();

                send.value_serialized(message);
            }
        }
    }

    fn loopback_channel(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>> {
        self.client_pooling.loopback_connection()
    }
}

impl<M: Serializable + 'static> Node<M> for TCPSimplexNode<M> {
    type Config = NodeConfig;
    type ConnectionManager = SimplexConnections<M>;
    type Crypto = NodePKCrypto;
    type IncomingRqHandler = PeerIncomingRqHandling<NetworkMessage<M>>;

    async fn bootstrap(cfg: Self::Config) -> febft_common::error::Result<Arc<Self>> {

        let id = cfg.id;

        debug!("Initializing sockets.");

        let tcp_config = cfg.tcp_config;

        let conn_counts = ConnCounts::from_tcp_config(&tcp_config);

        let addr = tcp_config.addrs.get(id.0 as u64).expect(format!("Failed to get my own IP address ({})", id.0).as_str()).clone();

        let network = tcp_config.network_config;

        let (connector, acceptor,
            client_socket, replica_socket) =
            Self::setup_network::<AsyncConn>(id, addr, network).await;

        //Setup all the peer message reception handling.
        let peers = Arc::new(PeerIncomingRqHandling::new(
            cfg.id,
            cfg.first_cli,
            cfg.client_pool_config,
        ));


        let peer_connections = SimplexConnections::new(id, cfg.first_cli,
                                                    conn_counts,
                                                    tcp_config.addrs,
                                                    connector, acceptor, peers.clone());


        debug!("Initializing connection listeners");
        peer_connections.clone().setup_tcp_listener(client_socket?);

        if let Some(replica) = replica_socket? {
            peer_connections.clone().setup_tcp_listener(replica);
        }

        let shared = NodePKCrypto::new(NodePKShared::from_config(cfg.pk_crypto_config));

        let rng = Arc::new(ThreadSafePrng::new());

        debug!("{:?} // Initializing node reference", id);

        let node = Arc::new(TCPSimplexNode {
            id,
            first_cli: cfg.first_cli,
            rng,
            keys: shared,
            client_pooling: peers,
            connections: peer_connections,
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

    fn node_connections(&self) -> &Arc<Self::ConnectionManager> {
        &self.connections
    }

    fn pk_crypto(&self) -> &Self::Crypto {
        &self.keys
    }

    fn node_incoming_rq_handling(&self) -> &Arc<PeerIncomingRqHandling<NetworkMessage<M>>> {
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

    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        todo!()
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) -> std::result::Result<(), Vec<NodeId>> {
        todo!()
    }


}

/// Some information about a message about to be sent to a peer
struct SendTo<M: Serializable + 'static> {
    my_id: NodeId,
    peer_id: NodeId,
    shared: Option<NodePKCrypto>,
    nonce: u64,
    peer_cnn: SendToPeer<M>,
    flush: bool,
    rq_send_time: Instant
}

/// The information about the connection itself which can either be a loopback
/// or a peer connection
enum SendToPeer<M: Serializable + 'static> {
    Me(Arc<ConnectedPeer<NetworkMessage<M>>>),
    Peer(Arc<PeerConnection<M>>),
}

impl<M: Serializable + 'static> SendTo<M> {
    fn value(self, msg: Either<(NetworkMessageKind<M>, Buf, Digest), (Buf, Digest)>) {
        let key_pair = if let Some(node_shared) = &self.shared {
            Some(node_shared.my_key())
        } else {
            None
        };

        match (self.peer_cnn, msg) {
            (SendToPeer::Me(conn), Either::Left((msg, buf, digest))) => {
                let message = WireMessage::new(self.my_id, self.peer_id,
                                               buf, self.nonce, Some(digest), key_pair);

                let (header, _) = message.into_inner();

                conn.push_request(NetworkMessage::new(header, msg)).unwrap();
            }
            (SendToPeer::Peer(peer), Either::Right((buf, digest))) => {
                let message = WireMessage::new(self.my_id, self.peer_id,
                                               buf, self.nonce, Some(digest), key_pair);

                peer.peer_message(message, None, self.flush, self.rq_send_time).unwrap();
            }
            (_, _) => { unreachable!() }
        }
    }

    fn value_serialized(self, msg: StoredSerializedNetworkMessage<M>) {
        match self.peer_cnn {
            SendToPeer::Me(peer_conn) => {
                let (header, msg) = msg.into_inner();

                let (msg, _) = msg.into_inner();

                peer_conn.push_request(NetworkMessage::new(header, msg)).unwrap();
            }
            SendToPeer::Peer(peer_cnn) => {
                let (header, msg) = msg.into_inner();

                let (_, buf) = msg.into_inner();

                let wm = WireMessage::from_parts(header, buf).unwrap();

                peer_cnn.peer_message(wm, None, self.flush, self.rq_send_time).unwrap();
            }
        }
    }
}