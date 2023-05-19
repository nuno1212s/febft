mod epoll_workers;
mod conn_establish;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use dashmap::DashMap;
use intmap::IntMap;
use mio::{Registry, Token};
use febft_common::node_id::NodeId;
use crate::client_pooling::ConnectedPeer;
use crate::message::NetworkMessage;
use crate::serialize::Serializable;
use crate::tcpip::PeerAddr;

pub struct Connections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    registered_connections: DashMap<NodeId, Arc<PeerConnection<M>>>,
    address_map: IntMap<PeerAddr>,
}

pub struct PeerConnection<M: Serializable + 'static> {
    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The map connecting each connection to a token in the MIO Workers
    connections: Mutex<BTreeMap<u32, ConnHandle>>,
}


#[derive(Clone)]
pub struct ConnHandle {
    id: u32,
    my_id: NodeId,
    peer_id: NodeId,
    token: Token,
    registry: Registry,
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl ConnHandle {
    pub fn new(id: u32, my_id: NodeId, peer_id: NodeId, token: Token, registry: Registry) -> Self {
        Self {
            id,
            my_id,
            peer_id,
            token,
            registry,
            cancelled: Arc::new(AtomicBool::new(false)),
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
    pub fn token(&self) -> Token {
        self.token
    }

    #[inline]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    #[inline]
    pub fn cancelled(&self) -> &Arc<AtomicBool> {
        &self.cancelled
    }
}