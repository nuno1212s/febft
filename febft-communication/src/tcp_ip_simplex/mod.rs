use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use febft_common::node_id::NodeId;
use febft_common::prng::ThreadSafePrng;
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::config::TcpConfig;
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::message_signing::NodePKCrypto;
use crate::Node;
use crate::serialize::Serializable;
use crate::tcpip::connections::PeerConnections;

pub struct TCPSimplexNode<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The thread safe pseudo random number generator
    rng: Arc<ThreadSafePrng>,
    // Our public key cryptography information
    keys: NodePKCrypto,
    // The client pooling for this node
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
}

impl<M: Serializable + 'static> Node<M> for TCPSimplexNode<M> {
    type Config = TcpConfig;
    type ConnectionManager = PeerConnections<M>;
    type Crypto = NodePKCrypto;
    type IncomingRqHandler = PeerIncomingRqHandling<NetworkMessage<M>>;

    async fn bootstrap(node_config: Self::Config) -> febft_common::error::Result<Arc<Self>> {
        todo!()
    }

    fn id(&self) -> NodeId {
        todo!()
    }

    fn first_cli(&self) -> NodeId {
        todo!()
    }

    fn node_connections(&self) -> &Arc<Self::ConnectionManager> {
        todo!()
    }

    fn pk_crypto(&self) -> &Self::Crypto {
        todo!()
    }

    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> febft_common::error::Result<()> {
        todo!()
    }

    fn send_signed(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> febft_common::error::Result<()> {
        todo!()
    }

    fn broadcast(&self, message: NetworkMessageKind<M>, targets: impl Iterator<Item=NodeId>) -> Result<(), Vec<NodeId>> {
        todo!()
    }

    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item=NodeId>) -> Result<(), Vec<NodeId>> {
        todo!()
    }

    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) -> Result<(), Vec<NodeId>> {
        todo!()
    }

    fn node_incoming_rq_handling(&self) -> &Arc<PeerIncomingRqHandling<NetworkMessage<M>>> {
        todo!()
    }

}