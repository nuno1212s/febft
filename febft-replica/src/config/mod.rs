use febft_common::ordering::SeqNo;
use febft_communication::config::NodeConfig;
use febft_execution::app::Service;
use febft_messages::ordering_protocol::OrderingProtocol;
use febft_messages::state_transfer::{StatefulOrderProtocol, StateTransferProtocol};

/// Represents a configuration used to bootstrap a `Replica`.
pub struct ReplicaConfig<S: Service + 'static, NT, OP: StatefulOrderProtocol<S::Data, NT> + 'static, ST: StateTransferProtocol<S::Data, OP, NT> + 'static> {
    /// The application logic.
    pub service: S,

    pub n: usize,
    pub f: usize,

    //TODO: These two values should be loaded from storage
    /// The sequence number for the current view.
    pub view: SeqNo,
    /// Next sequence number attributed to a request by
    /// the consensus layer.
    pub next_consensus_seq: SeqNo,

    /// The configuration for the ordering protocol
    pub op_config: OP::Config,
    /// The configuration for the State transfer protocol
    pub st_config: ST::Config,

    /// The path to the persistent database
    pub db_path: String,
    /// Check out the docs on `NodeConfig`.
    pub node: NodeConfig,
}