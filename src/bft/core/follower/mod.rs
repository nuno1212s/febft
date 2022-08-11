use std::sync::Arc;
use crate::bft::communication::Node;
use crate::bft::consensus::log::MemLog;
use crate::bft::cst::CollabStateTransfer;
use crate::bft::executable::{ExecutorHandle, Reply, Request, Service, State};

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum FollowerPhase {
    //Normal phase of follower. Up to date on the current state of the quorum
    // and actively listening for new finished quorums
    NormalPhase,
    // Retrieving the current state of the quorum. Might require transferring
    // the state if we are very far behind or only require a log transmission
    RetrievingState
}

///A follower does not participate in the quorum, but he passively listens
/// to the quorum decisions and executes them locally
///
/// A follower cannot perform non unordered requests as it is not a part of the
/// quorum so it can mostly serve reads.
///
/// This does however mean that we can scale horizontally in read processing with eventual
/// consistency, as well as serve as a "backup" to the quorum
///
/// They might also be used to loosen the load on the quorum replicas when we need a
/// State transfer as they can request the last checkpoint from these replicas.
pub struct Follower<S: Service + 'static> {
    //The current phase of the follower
    phase: FollowerPhase,
    //The handle to the current state and the executor of the service, so we
    //can keep up and respond to requests
    executor: ExecutorHandle<S>,
    //
    cst: CollabStateTransfer<S>,
    //The log of messages
    log: Arc<MemLog<State<S>, Request<S>, Reply<S>>>,
    node: Arc<Node<S::Data>>

}