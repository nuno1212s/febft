use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use febft_execution::executable::{Reply, Request, Service, State};
use febft_messages::messages::SystemMessage;
use crate::messages::ProtocolMessage;

pub mod consensus;
pub mod cst;
pub mod messages;
pub mod msg_log;
pub mod proposer;
pub mod sync;
pub mod observer;
pub mod follower;

pub use febft_messages::*;
pub use febft_communication::*;
use crate::consensus::{Consensus, ConsensusGuard};
use crate::cst::CollabStateTransfer;
use crate::msg_log::decided_log::DecidedLog;
use crate::msg_log::pending_decision::PendingRequestLog;
use crate::proposer::Proposer;
use crate::sync::Synchronizer;

pub type SysMsg<S: Service> = SystemMessage<S, ProtocolMessage<S::Data>>;

#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum ReplicaPhase {
    // the replica is retrieving state from
    // its peer replicas
    RetrievingState,
    // the replica has entered the
    // synchronization phase of mod-smart
    SyncPhase,
    // the replica is on the normal phase
    // of mod-smart
    NormalPhase,
}

pub struct ConsensusProtocol<S> where S: Service + 'static {

    // What phase of the algorithm is this replica currently in
    phase: ReplicaPhase,
    // this value is primarily used to switch from
    // state transfer back to a view change
    phase_stack: Option<ReplicaPhase>,
    // The timeout layer, handles timing out requests

    // Consensus state transfer State machine
    cst: CollabStateTransfer<S>,
    // Synchronizer state machine
    synchronizer: Arc<Synchronizer<S>>,
    //Consensus state machine
    consensus: Consensus<S>,
    //The guard for the consensus.
    //Set to true when there is a consensus running, false when it's ready to receive
    //A new pre-prepare message
    consensus_guard: ConsensusGuard,
    // Check if unordered requests can be proposed.
    // This can only occur when we are in the normal phase of the state machine
    unordered_rq_guard: Arc<AtomicBool>,

    // The pending request log. Handles requests received by this replica
    // Or forwarded by others that have not yet made it into a consensus instance
    pending_request_log: Arc<PendingRequestLog<S>>,
    // The log of the decided consensus messages
    // This is completely owned by the server thread and therefore does not
    // Require any synchronization
    decided_log: DecidedLog<S>,
    // The proposer of this replica
    proposer: Arc<Proposer<S>>,
}