mod epoll_workers;
mod conn_establish;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use dashmap::DashMap;
use intmap::IntMap;
use mio::{Registry, Token, Waker};
use febft_common::channel::{ChannelSyncTx, OneShotRx};
use febft_common::node_id::NodeId;
use crate::client_pooling::ConnectedPeer;
use crate::message::NetworkMessage;
use crate::mio_tcp::connections::epoll_workers::EpollWorkerMessage;
use crate::NodeConnections;
use crate::serialize::Serializable;
use crate::tcpip::PeerAddr;

pub struct Connections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    // The map of registered connections
    registered_connections: DashMap<NodeId, Arc<PeerConnection<M>>>,
    // A map of addresses to our known peers
    address_map: IntMap<PeerAddr>,
    // A reference to the worker group that handles the epoll workers
    worker_group: EpollWorkerGroup<M>
}

impl<M> NodeConnections for Connections<M> where M: Serializable + 'static {
    fn is_connected_to_node(&self, node: &NodeId) -> bool {
        self.registered_connections.contains_key(node)
    }

    fn connected_nodes_count(&self) -> usize {
        self.registered_connections.len()
    }

    fn connected_nodes(&self) -> Vec<NodeId> {
        self.registered_connections.iter().map(|entry| entry.key().clone()).collect()
    }

    fn connect_to_node(self: &Arc<Self>, node: NodeId) -> Vec<OneShotRx<febft_common::error::Result<()>>> {
        todo!()
    }

    async fn disconnect_from_node(&self, node: &NodeId) -> febft_common::error::Result<()> {
        todo!()
    }
}

pub struct PeerConnection<M: Serializable + 'static> {
    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The map connecting each connection to a token in the MIO Workers
    connections: Mutex<BTreeMap<u32, ConnHandle>>,
}

/// The worker group that handles the epoll events
#[derive(Clone)]
struct EpollWorkerGroup<M: Serializable + 'static> {
    workers: Vec<ChannelSyncTx<EpollWorkerMessage<M>>>,

}

#[derive(Clone)]
pub struct ConnHandle {
    id: u32,
    my_id: NodeId,
    peer_id: NodeId,
    waker: Arc<Waker>,
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl ConnHandle {
    pub fn new(id: u32, my_id: NodeId, peer_id: NodeId, waker: Arc<Waker>) -> Self {
        Self {
            id,
            my_id,
            peer_id,
            cancelled: Arc::new(AtomicBool::new(false)),
            waker,
        }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    #[inline]
    pub fn my_id(&self) -> NodeId {
        self.my_id
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn peer_id(&self) -> NodeId {
        self.peer_id
    }

    #[inline]
    pub fn cancelled(&self) -> &Arc<AtomicBool> {
        &self.cancelled
    }
}