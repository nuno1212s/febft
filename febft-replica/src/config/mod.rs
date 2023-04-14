use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_communication::config::NodeConfig;
use febft_communication::Node;
use febft_execution::app::Service;
use febft_messages::ordering_protocol::OrderingProtocol;
use febft_messages::serialize::ServiceMsg;
use febft_messages::state_transfer::{StatefulOrderProtocol, StateTransferProtocol};

/// Represents a configuration used to bootstrap a `Replica`.
pub struct ReplicaConfig<S, OP, ST, NT> where
    S: Service + 'static,
    OP: StatefulOrderProtocol<S::Data, NT> + 'static,
    ST: StateTransferProtocol<S::Data, OP, NT> + 'static,
    NT: Node<ServiceMsg<S::Data, OP::Serialization, ST::Serialization>> {
    /// The application logic.
    pub service: S,
    
    pub id: NodeId,

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

    /// Check out the docs on `NodeConfig`.
    pub node: NT::Config,
}