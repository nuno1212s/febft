use intmap::IntMap;
use rustls::{ClientConfig, ServerConfig};
use febft_common::crypto::signature::{KeyPair, PublicKey};
use crate::NodeId;
use crate::tcpip::PeerAddr;

/// The configuration of the network node
pub struct NodeConfig {
    /// The id of this `Node`.
    pub id: NodeId,
    /// The ID of the first client in the network
    /// Every peer with id < first_cli is a replica and every peer with id > first_cli is
    /// a client
    pub first_cli: NodeId,
    /// The list of public keys of all nodes in the system.
    pub pk: IntMap<PublicKey>,
    /// The secret key of this particular `Node`.
    pub sk: KeyPair,
    /// TCP specific configuration
    pub tcp_config: TcpConfig,
    ///The configurations of the client pool config
    pub client_pool_config: ClientPoolConfig,
}

pub struct TcpConfig {
    /// The addresses of all nodes in the system (including clients),
    /// as well as the domain name associated with each address.
    ///
    /// For any `NodeConfig` assigned to `c`, the IP address of
    /// `c.addrs[&c.id]` should be equivalent to `localhost`.
    pub addrs: IntMap<PeerAddr>,
    /// Configurations specific to the networking
    pub network_config: TlsConfig,
}

pub struct TlsConfig {
    /// The TLS configuration used to connect to replica nodes. (from client nodes)
    pub async_client_config: ClientConfig,
    /// The TLS configuration used to accept connections from client nodes.
    pub async_server_config: ServerConfig,
    ///The TLS configuration used to accept connections from replica nodes (Synchronously)
    pub sync_server_config: ServerConfig,
    ///The TLS configuration used to connect to replica nodes (from replica nodes) (Synchronousy)
    pub sync_client_config: ClientConfig,
}

pub struct ClientPoolConfig {
    ///The max size for batches of client operations
    pub batch_size: usize,
    ///How many clients should be placed in a single collecting pool (seen in incoming_peer_handling)
    pub clients_per_pool: usize,
    ///The timeout for batch collection in each client pool.
    /// (The first to reach between batch size and timeout)
    pub batch_timeout_micros: u64,
    ///How long should a client pool sleep for before attempting to collect requests again
    /// (It actually will sleep between 3/4 and 5/4 of this value, to make sure they don't all sleep / wake up at the same time)
    pub batch_sleep_micros: u64,
}