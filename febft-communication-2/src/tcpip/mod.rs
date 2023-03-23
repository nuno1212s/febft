use std::net::SocketAddr;
use crate::NodeId;
use crate::serialize::Serializable;

///Represents the server addresses of a peer
///Clients will only have 1 address while replicas will have 2 addresses (1 for facing clients,
/// 1 for facing replicas)
pub struct PeerAddr {
    client_addr: (SocketAddr, String),
    replica_addr: Option<(SocketAddr, String)>,
}

impl PeerAddr {
    pub fn new(client_addr: (SocketAddr, String)) -> Self {
        Self {
            client_addr,
            replica_addr: None,
        }
    }

    pub fn new_replica(
        client_addr: (SocketAddr, String),
        replica_addr: (SocketAddr, String),
    ) -> Self {
        Self {
            client_addr,
            replica_addr: Some(replica_addr),
        }
    }
}

pub struct TcpNode<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
}

