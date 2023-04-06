#![feature(async_fn_in_trait)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use async_tls::{TlsAcceptor, TlsConnector};
use intmap::IntMap;
use rustls::{ClientConfig, ServerConfig};
use crate::message::{NetworkMessage, NetworkMessageKind, StoredSerializedNetworkMessage};
use crate::serialize::Serializable;
use febft_common::error::*;
use crate::client_pooling::ConnectedPeer;
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use febft_common::channel::OneShotRx;
use crate::config::NodeConfig;
use crate::tcpip::{ConnectionType, NodeConnectionAcceptor, TlsNodeAcceptor, TlsNodeConnector};

pub mod serialize;
pub mod message;
pub mod tcpip;
pub mod cpu_workers;
pub mod client_pooling;
pub mod config;
pub mod message_signing;

/// A `NodeId` represents the id of a process in the BFT system.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[repr(transparent)]
pub struct NodeId(pub u32);

impl NodeId {
    pub fn targets_u32<I>(into_iterator: I) -> impl Iterator<Item=Self>
        where
            I: IntoIterator<Item=u32>,
    {
        into_iterator.into_iter().map(Self)
    }

    pub fn targets<I>(into_iterator: I) -> impl Iterator<Item=Self>
        where
            I: IntoIterator<Item=usize>,
    {
        into_iterator.into_iter().map(NodeId::from)
    }

    pub fn id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for NodeId {
    #[inline]
    fn from(id: u32) -> NodeId {
        NodeId(id)
    }
}

impl From<u64> for NodeId {
    #[inline]
    fn from(id: u64) -> NodeId {
        NodeId(id as u32)
    }
}

impl From<usize> for NodeId {
    #[inline]
    fn from(id: usize) -> NodeId {
        NodeId(id as u32)
    }
}

impl From<NodeId> for usize {
    #[inline]
    fn from(id: NodeId) -> usize {
        id.0 as usize
    }
}

impl From<NodeId> for u64 {
    #[inline]
    fn from(id: NodeId) -> u64 {
        id.0 as u64
    }
}

impl From<NodeId> for u32 {
    #[inline]
    fn from(id: NodeId) -> u32 {
        id.0 as u32
    }
}

pub trait NodeConnections {

    /// Are we currently connected to a given node?
    fn is_connected_to_node(&self, node: &NodeId) -> bool;

    /// How many nodes are we currently connected to in this node
    fn connected_nodes(&self) -> usize;

    /// Connect this node to another node.
    /// Returns a vec with the results of each of the attempted connections
    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<Result<()>>>;

    /// Disconnect this node from another node
    async fn disconnect_from_node(&self, node: &NodeId) -> Result<()>;

}

/// A network node. Handles all the connections between nodes.
pub trait Node<M: Serializable + 'static> {

    type ConnectionManager : NodeConnections;

    /// Bootstrap the node
    async fn bootstrap(node_config: NodeConfig) -> Result<Arc<Self>>;

    /// Reports the id of this `Node`.
    fn id(&self) -> NodeId;

    /// Reports the first Id
    fn first_cli(&self) -> NodeId;

    /// Get a handle to the connection manager of the operation
    fn node_connections(&self) -> &Arc<Self::ConnectionManager>;

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
    fn receive_from_clients(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Vec<NetworkMessage<M>>>;

    /// Try to receive messages from the clients, without blocking if there are no requests available.
    fn try_recv_from_clients(
        &self,
    ) -> Result<Option<Vec<NetworkMessage<M>>>>;

    //Receive messages from the replicas we are connected to
    fn receive_from_replicas(&self) -> Result<NetworkMessage<M>>;

}