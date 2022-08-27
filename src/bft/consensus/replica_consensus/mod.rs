use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use chrono::Utc;
use intmap::IntMap;
use log::{debug, error, warn};

use crate::bft::{
    communication::{
        message::{
            ConsensusMessage, ConsensusMessageKind, ObserveEventKind, SerializedMessage,
            StoredMessage, StoredSerializedSystemMessage, SystemMessage, WireMessage,
        },
        serialize::DigestData,
        Node, NodeId,
    },
    core::server::{
        follower_handling::{FollowerEvent, FollowerHandle},
        observer::{MessageType, ObserverHandle},
        ViewInfo,
    },
    crypto::hash::Digest,
    executable::{Reply, Request, Service, State},
    globals::ReadOnly,
    ordering::{Orderable, SeqNo},
    sync::{AbstractSynchronizer, Synchronizer},
    threadpool,
};

use crate::bft::ordering::tbo_pop_message;

use super::{
    log::{persistent::PersistentLogModeTrait, Log},
    AbstractConsensus, Consensus, ConsensusAccessory, ConsensusGuard, ConsensusPollStatus,
    ProtoPhase,
};

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        extract_msg!({}, ConsensusPollStatus::Recv, $g, $q)
    };

    ($opt:block, $rsp:expr, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ConsensusMessage<_>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            ConsensusPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            $rsp
        }
    };
}

/// Contains the state of an active consensus instance, as well
/// as future instances.
pub struct ReplicaConsensus<S: Service> {
    missing_requests: VecDeque<Digest>,
    missing_swapbuf: Vec<usize>,
    speculative_commits: Arc<Mutex<IntMap<StoredSerializedSystemMessage<S::Data>>>>,
    consensus_guard: ConsensusGuard,
    observer_handle: ObserverHandle,
    follower_handle: Option<FollowerHandle<S>>,
}

impl<S: Service + 'static> Consensus<S> {
    pub(super) fn handle_preprepare_sucessfull(
        &mut self,
        view: ViewInfo,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        node: &Node<S::Data>,
    ) {
        let my_id = self.node_id;

        let seq = self.sequence_number();
        let view_seq = view.sequence_number();
        let quorum = view.quorum_members().clone();

        let sign_detached = node.sign_detached();
        let current_digest = self.current_digest.clone();
        let n = view.params().n();

        match &mut self.acessory {
            ConsensusAccessory::Replica(rep) => {
                let speculative_commits = Arc::clone(&rep.speculative_commits);

                // start speculatively creating COMMIT messages,
                // which involve potentially expensive signing ops
                //Speculate in another thread.
                threadpool::execute(move || {
                    // create COMMIT
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        seq,
                        view_seq,
                        ConsensusMessageKind::Commit(current_digest),
                    ));

                    // serialize raw msg
                    let mut buf = Vec::new();

                    let digest =
                        <S::Data as DigestData>::serialize_digest(&message, &mut buf).unwrap();

                    for peer_id in NodeId::targets(0..n) {
                        let buf_clone = Vec::from(&buf[..]);

                        // create header
                        let (header, _) = WireMessage::new(
                            my_id,
                            peer_id,
                            buf_clone,
                            // NOTE: nonce not too important here,
                            // since we already contain enough random
                            // data with the unique digest of the
                            // PRE-PREPARE message
                            0,
                            Some(digest),
                            Some(sign_detached.key_pair()),
                        )
                        .into_inner();

                        // store serialized header + message
                        let serialized = SerializedMessage::new(message.clone(), buf.clone());

                        let stored = StoredMessage::new(header, serialized);

                        let mut map = speculative_commits.lock().unwrap();
                        map.insert(peer_id.into(), stored);
                    }
                });

                // leader can't vote for a PREPARE
                // Since the preprepare message is already "equivalent" to the leaders prepare message
                if my_id != view.leader() {
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        seq,
                        view.sequence_number(),
                        ConsensusMessageKind::Prepare(self.current_digest.clone()),
                    ));

                    let targets = NodeId::targets(0..view.params().n());

                    node.broadcast(message, targets);
                }

                //Notify the followers
                if let Some(follower_handle) = &rep.follower_handle {
                    if let Err(err) =
                        follower_handle.send(FollowerEvent::ReceivedConsensusMsg(view, msg))
                    {
                        error!("{:?}", err);
                    }
                }

                //Notify the observers
                if let Err(err) = rep
                    .observer_handle
                    .tx()
                    .send(MessageType::Event(ObserveEventKind::Prepare(seq)))
                {
                    error!("{:?}", err);
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_preparing_no_quorum<T>(
        &mut self,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        _log: &Log<S, T>,
        _node: &Node<S::Data>,
    ) where
        T: PersistentLogModeTrait,
    {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                if let Some(follower_handle) = &rep.follower_handle {
                    if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                        curr_view,
                        preparing_msg,
                    )) {
                        error!("{:?}", err);
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_preparing_quorum<T>(
        &mut self,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        log: &Log<S, T>,
        node: &Node<S::Data>,
    ) where
        T: PersistentLogModeTrait,
    {
        let seq = self.sequence_number();
        let node_id = self.node_id;

        match &mut self.acessory {
            ConsensusAccessory::Replica(rep) => {
                let speculative_commits = rep.take_speculative_commits();

                if valid_spec_commits::<S>(&speculative_commits, node_id, seq, &curr_view) {
                    for (_, msg) in speculative_commits.iter() {
                        debug!("{:?} // Broadcasting speculative commit message {:?} (total of {} messages) to {} targets",
                     node_id, msg.message().original(), speculative_commits.len(), curr_view.params().n());
                        break;
                    }

                    node.broadcast_serialized(speculative_commits);
                } else {
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        seq,
                        curr_view.sequence_number(),
                        ConsensusMessageKind::Commit(self.current_digest.clone()),
                    ));

                    debug!(
                        "{:?} // Broadcasting commit consensus message {:?}",
                        node_id, message
                    );

                    let targets = NodeId::targets(0..curr_view.params().n());

                    node.broadcast_signed(message, targets);
                }

                log.batch_meta().lock().commit_sent_time = Utc::now();

                //Follower notifications
                if let Some(follower_handle) = &rep.follower_handle {
                    if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                        curr_view,
                        preparing_msg,
                    )) {
                        error!("{:?}", err);
                    }
                }

                //Observer notifications
                if let Err(err) = rep
                    .observer_handle
                    .tx()
                    .send(MessageType::Event(ObserveEventKind::Commit(seq)))
                {
                    error!("{:?}", err);
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_committing_no_quorum(
        &mut self,
        curr_view: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                //Notify followers of the received message
                if let Some(follower_handle) = &rep.follower_handle {
                    if let Err(err) = follower_handle
                        .send(FollowerEvent::ReceivedConsensusMsg(curr_view, commit_msg))
                    {
                        error!("{:?}", err);
                    }
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_committing_quorum(
        &mut self,
        view_info: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                //Notify follower of the received message
                if let Some(follower_handle) = &rep.follower_handle {
                    if let Err(err) = follower_handle
                        .send(FollowerEvent::ReceivedConsensusMsg(view_info, commit_msg))
                    {
                        error!("{:?}", err);
                    }
                }

                if let Err(_) =
                    rep.observer_handle
                        .tx()
                        .send(MessageType::Event(ObserveEventKind::Consensus(
                            self.sequence_number(),
                        )))
                {
                    warn!("Failed to notify observers of the consensus instance")
                }
            }
            _ => {}
        }
    }

    pub(super) fn handle_installed_seq_num(&mut self) {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                let mut guard = rep.consensus_guard.consensus_info().lock().unwrap();

                guard.0 = self.curr_seq;
            }
            _ => {}
        }
    }

    pub(super) fn handle_next_instance(&mut self) {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                {
                    let mut guard = rep.consensus_guard.consensus_info().lock().unwrap();

                    guard.0 = self.curr_seq;
                }

                rep.consensus_guard
                    .consensus_guard()
                    .store(false, Ordering::SeqCst);

                if let Err(_) = rep
                    .observer_handle
                    .tx()
                    .send(MessageType::Event(ObserveEventKind::Ready(self.curr_seq)))
                {
                    warn!("Failed to notify observers of the consensus instance")
                }
            }
            ConsensusAccessory::Follower => {}
        }
    }

    pub(super) fn handle_finalize_view_change<T>(&mut self, synchronizer: &T)
    where
        T: AbstractSynchronizer<S>,
    {
        match &self.acessory {
            ConsensusAccessory::Replica(rep) => {
                //Update the current view and seq numbers

                //Acq consensus guard since we already have the message to propose
                //So we don't want the proposer to propose anything yet
                rep.consensus_guard
                    .consensus_guard()
                    .store(true, Ordering::SeqCst);

                let mut guard = rep.consensus_guard.consensus_lock.lock().unwrap();

                guard.1 = synchronizer.view();
            }
            ConsensusAccessory::Follower => {}
        }
    }

    pub(super) fn handle_poll_preparing_requests<T>(
        &mut self,
        log: &Log<S, T>,
    ) -> ConsensusPollStatus<Request<S>>
    where
        T: PersistentLogModeTrait,
    {
        match &mut self.acessory {
            ConsensusAccessory::Replica(rep) => {
                let iterator = rep
                    .missing_requests
                    .iter()
                    .enumerate()
                    .filter(|(_index, digest)| log.has_request(digest));
                for (index, _) in iterator {
                    rep.missing_swapbuf.push(index);
                }
                for index in rep.missing_swapbuf.drain(..) {
                    rep.missing_requests.swap_remove_back(index);
                }
                if rep.missing_requests.is_empty() {
                    extract_msg!(
                        {
                            self.phase = ProtoPhase::Preparing(1);
                        },
                        ConsensusPollStatus::Recv,
                        &mut self.tbo.get_queue,
                        &mut self.tbo.prepares
                    )
                } else {
                    ConsensusPollStatus::Recv
                }
            }
            ConsensusAccessory::Follower => ConsensusPollStatus::Recv,
        }
    }
}

impl<S: Service + 'static> ReplicaConsensus<S> {
    pub(super) fn new(
        view: ViewInfo,
        next_seq: SeqNo,
        observer_handle: ObserverHandle,
        follower_handle: Option<FollowerHandle<S>>,
    ) -> Self {
        Self {
            missing_requests: VecDeque::new(),
            missing_swapbuf: Vec::new(),
            speculative_commits: Arc::new(Mutex::new(IntMap::new())),
            consensus_guard: ConsensusGuard {
                consensus_lock: Arc::new(Mutex::new((next_seq, view))),
                consensus_guard: Arc::new(AtomicBool::new(false)),
            },
            observer_handle,
            follower_handle,
        }
    }

    pub(super) fn consensus_guard(&self) -> &ConsensusGuard {
        &self.consensus_guard
    }

    fn take_speculative_commits(&self) -> IntMap<StoredSerializedSystemMessage<S::Data>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::replace(&mut *map, IntMap::new())
    }
}

#[inline]
fn valid_spec_commits<S>(
    speculative_commits: &IntMap<StoredSerializedSystemMessage<S::Data>>,
    node_id: NodeId,
    seq_no: SeqNo,
    view: &ViewInfo,
) -> bool
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + Clone + 'static,
    Reply<S>: Send + 'static,
{
    let len = speculative_commits.len();

    let n = view.params().n();
    if len != n {
        debug!(
            "{:?} // Failed to read speculative commits, {} vs {}",
            node_id, len, n
        );

        return false;
    }

    speculative_commits
        .values()
        .map(|stored| match stored.message().original() {
            SystemMessage::Consensus(c) => c,
            _ => unreachable!(),
        })
        .all(|commit| commit.sequence_number() == seq_no)
}
