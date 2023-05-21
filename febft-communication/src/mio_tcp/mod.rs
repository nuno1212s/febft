use std::collections::BTreeMap;
use std::sync::Arc;
use febft_common::node_id::NodeId;
use febft_common::prng::ThreadSafePrng;
use crate::client_pooling::PeerIncomingRqHandling;
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::message_signing::NodePKCrypto;
use crate::mio_tcp::connections::Connections;
use crate::Node;
use crate::serialize::Serializable;

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
    client_pooling: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>
}

impl<M: Serializable + 'static> Node<M> for MIOTcpNode<M> {
    type Config = ();
    type ConnectionManager = Connections<M>;
    type Crypto = NodePKCrypto;
    type IncomingRqHandler = PeerIncomingRqHandling<NetworkMessage<M>>;

    async fn bootstrap(node_config: Self::Config) -> febft_common::error::Result<Arc<Self>> {
        todo!()
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
        &self.connections
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

}