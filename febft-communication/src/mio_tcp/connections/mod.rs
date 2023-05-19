mod epoll_workers;
mod conn_establish;

use std::collections::BTreeMap;
use std::sync::Arc;
use dashmap::DashMap;
use intmap::IntMap;
use mio::Token;
use febft_common::node_id::NodeId;
use crate::tcpip::PeerAddr;

pub struct Connections {
    id: NodeId,
    first_cli: NodeId,
    registered_connections: DashMap<NodeId, Arc<PeerConnection>>,
    address_map: IntMap<PeerAddr>,

}

pub struct PeerConnection {

    connections: BTreeMap<u32, Token>,

}

