use std::collections::BTreeMap;
use std::iter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use either::Either;
use log::{debug, error};
use smallvec::SmallVec;
use febft_common::crypto::hash::Digest;
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::prng::ThreadSafePrng;
use febft_common::{socket, threadpool};
use febft_common::socket::SyncListener;
use febft_metrics::metrics::metric_duration;
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::config::MioConfig;
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage, WireMessage};
use crate::message_signing::{NodePKCrypto, NodePKShared};
use crate::metric::THREADPOOL_PASS_TIME_ID;
use crate::mio_tcp::connections::{Connections, PeerConnection};
use crate::mio_tcp::connections::epoll_group::{init_worker_group_handle, initialize_worker_group};
use crate::Node;
use crate::serialize::{Buf, Serializable};
use crate::tcpip::connections::ConnCounts;
use crate::tcpip::PeerAddr;

mod connections;
const NODE_QUORUM_SIZE: usize = 32;

type SendTos<M> = SmallVec<[SendTo<M>; NODE_QUORUM_SIZE]>;

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
                    peer_cnn: SendToPeer::Me(self.client_pooling.loopback_connection().clone()),
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
                                rq_send_time: Instant::now(),
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
                                rq_send_time: Instant::now(),
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

        let start = Instant::now();

        threadpool::execute(move || {
            metric_duration(THREADPOOL_PASS_TIME_ID, start.elapsed());

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


    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> Result<()> {
        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, iter::once(target), flush);

        if !failed.is_empty() {
            return Err(Error::simple(ErrorKind::CommunicationPeerNotFound));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, message);

        Ok(())
    }

    fn send_signed(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> Result<()> {
        let keys = Some(&self.keys);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, iter::once(target), flush);

        if !failed.is_empty() {
            return Err(Error::simple(ErrorKind::CommunicationPeerNotFound));
        }

        Self::serialize_send_impl(send_to_me, send_to_others, message);

        Ok(())
    }

    fn broadcast(&self, message: NetworkMessageKind<M>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let (send_to_me, send_to_others, failed) =
            self.send_tos(None, targets, true);

        Self::serialize_send_impl(send_to_me, send_to_others, message);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>> {
        let keys = Some(&self.keys);

        let (send_to_me, send_to_others, failed) =
            self.send_tos(keys, target, true);

        Self::serialize_send_impl(send_to_me, send_to_others, message);

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) -> std::result::Result<(), Vec<NodeId>> {
        let targets = messages.keys().cloned().into_iter();

        let (send_to_me, send_to_others, failed) = self.send_tos(None,
                                                                 targets, true);
        threadpool::execute(move || {
            Self::send_serialized_impl(send_to_me, send_to_others, messages);
        });

        if !failed.is_empty() {
            Err(failed)
        } else {
            Ok(())
        }
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
    rq_send_time: Instant,
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

                peer.peer_message(message, None).unwrap();
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

                peer_cnn.peer_message(wm, None).unwrap();
            }
        }
    }
}