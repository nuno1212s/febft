#![feature(async_fn_in_trait)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use intmap::IntMap;
use rustls::{ClientConfig, ServerConfig};
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::serialize::Serializable;
use febft_common::error::*;
use crate::client_pooling::ConnectedPeer;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use febft_common::channel::OneShotRx;
use febft_common::crypto::signature::{KeyPair, PublicKey};
use febft_common::node_id::NodeId;
use crate::config::NodeConfig;
use crate::message_signing::SignDetached;
use crate::tcpip::{ConnectionType, NodeConnectionAcceptor, TlsNodeAcceptor, TlsNodeConnector};

pub mod serialize;
pub mod message;
pub mod tcpip;
pub mod cpu_workers;
pub mod client_pooling;
pub mod config;
pub mod message_signing;

/// A trait defined that indicates how the connections are managed
/// Allows us to verify various things about our current connections as well
/// as establishing new ones.
pub trait NodeConnections {

    /// Are we currently connected to a given node?
    fn is_connected_to_node(&self, node: &NodeId) -> bool;

    /// How many nodes are we currently connected to in this node
    fn connected_nodes_count(&self) -> usize;

    /// Get the nodes we are connected to at this time
    fn connected_nodes(&self) -> Vec<NodeId>;

    /// Connect this node to another node.
    /// Returns a vec with the results of each of the attempted connections
    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<Result<()>>>;

    /// Disconnect this node from another node
    async fn disconnect_from_node(&self, node: &NodeId) -> Result<()>;

}

pub trait NodePK {

    /// Detached info for signatures
    fn sign_detached(&self) -> SignDetached;

    /// Get the public key for a given node
    fn get_public_key(&self, node: &NodeId) -> Option<PublicKey>;

    /// Get our own key pair
    fn get_key_pair(&self) -> &KeyPair;

}

/// A network node. Handles all the connections between nodes.
pub trait Node<M: Serializable + 'static> : Send + Sync {

    type Config;

    type ConnectionManager : NodeConnections;

    type Crypto: NodePK;

    /// Bootstrap the node
    async fn bootstrap(node_config: Self::Config) -> Result<Arc<Self>>;

    /// Reports the id of this `Node`.
    fn id(&self) -> NodeId;

    /// Reports the first Id
    fn first_cli(&self) -> NodeId;

    /// Get a handle to the connection manager of this node.
    fn node_connections(&self) -> &Arc<Self::ConnectionManager>;

    /// Crypto
    fn pk_crypto(&self) -> &Self::Crypto;

    /// Sends a message to a given target.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> Result<()>;

    /// Sends a signed message to a given target
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the target or err if not. No other checks are made
    /// on the success of the message dispatch
    fn send_signed(&self, message: NetworkMessageKind<M>, target: NodeId, flush: bool) -> Result<()>;

    /// Broadcast a message to all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast(&self, message: NetworkMessageKind<M>, targets: impl Iterator<Item=NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Broadcast a signed message for all of the given targets
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_signed(&self, message: NetworkMessageKind<M>, target: impl Iterator<Item = NodeId>) -> std::result::Result<(), Vec<NodeId>>;

    /// Broadcast the serialized messages provided.
    /// Does not block on the message sent. Returns a result that is
    /// Ok if there is a current connection to the targets or err if not. No other checks are made
    /// on the success of the message dispatch
    fn broadcast_serialized(&self, messages: BTreeMap<NodeId, StoredSerializedNetworkMessage<M>>) -> std::result::Result<(), Vec<NodeId>>;

    /// Get a reference to our loopback channel
    fn loopback_channel(&self) -> &Arc<ConnectedPeer<NetworkMessage<M>>>;

    /// Receive messages from the clients we are connected to
    /// Blocks if there are no pending requests to collect.
    /// If timeout is reached without requests, returns an empty vector
    fn receive_from_clients(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Vec<NetworkMessage<M>>>;

    /// Try to receive messages from the clients, without blocking if there are no requests available.
    fn try_recv_from_clients(
        &self,
    ) -> Result<Option<Vec<NetworkMessage<M>>>>;

    //Receive messages from the replicas we are connected to
    fn receive_from_replicas(&self, timeout: Option<Duration>) -> Result<Option<NetworkMessage<M>>>;

    /// Receive from replicas without a timeout
    fn receive_from_replicas_no_timeout(&self) -> Result<NetworkMessage<M>> {
        // We can unwrap it since we know it will always contain a value, unless there was an error
        Ok(self.receive_from_replicas(None)?.unwrap())
    }

}