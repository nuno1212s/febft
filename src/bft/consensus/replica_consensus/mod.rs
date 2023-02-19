use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering}, Mutex,
    },
};
use bytes::BytesMut;

use chrono::Utc;
use intmap::IntMap;
use log::{debug, error, warn};

use crate::bft::{
    communication::{
        message::{
            ConsensusMessage, ConsensusMessageKind, ObserveEventKind, SerializedMessage,
            StoredMessage, StoredSerializedSystemMessage, SystemMessage, WireMessage,
        },
        Node,
        NodeId, serialize::DigestData,
    },
    core::server::{
        follower_handling::{FollowerEvent, FollowerHandle},
        observer::{MessageType, ObserverHandle},
    },
    crypto::hash::Digest,
    executable::{Reply, Request, Service, State},
    globals::ReadOnly,
    ordering::{Orderable, SeqNo},
    sync::AbstractSynchronizer,
    threadpool,
};
use crate::bft::communication::serialize::Buf;
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::msg_log::persistent::PersistentLogModeTrait;
use crate::bft::ordering::tbo_pop_message;
use crate::bft::sync::view::ViewInfo;

use super::{
    AbstractConsensus,
    Consensus, ConsensusAccessory, ConsensusGuard, ConsensusPollStatus,
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
/// Consists off consensus information that is only relevant if we are a replica, not a follower
pub struct ReplicaConsensus<S: Service> {
    missing_requests: VecDeque<Digest>,
    missing_swapbuf: Vec<usize>,
    speculative_commits: Arc<Mutex<IntMap<StoredSerializedSystemMessage<S::Data>>>>,
    consensus_guard: ConsensusGuard,
    observer_handle: ObserverHandle,
    follower_handle: Option<FollowerHandle<S>>,
}

/// Preparing requests phase (This is no longer used, maybe remove it?)
pub(super) enum ReplicaPreparingPollStatus {
    Recv,
    MoveToPreparing,
}

impl<S: Service + 'static> ReplicaConsensus<S> {
    pub(super) fn handle_pre_prepare_successful(
        &mut self,
        seq: SeqNo,
        current_digest: Digest,
        view: ViewInfo,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        node: &Node<S::Data>,
    ) {
        let my_id = node.id();
        let view_seq = view.sequence_number();
        let _quorum = view.quorum_members().clone();

        let sign_detached = node.sign_detached();
        let n = view.params().n();

        let speculative_commits = Arc::clone(&self.speculative_commits);

        // start speculatively creating COMMIT messages,
        // which involve potentially expensive signing ops
        //Speculate in another thread.
        threadpool::execute(move || {
            // create COMMIT
            let message = SystemMessage::Consensus(ConsensusMessage::new(
                seq,
                view_seq,
                ConsensusMessageKind::Commit(current_digest.clone()),
            ));

            // serialize raw msg
            let mut buf = Vec::new();

            let digest =
                <S::Data as DigestData>::serialize_digest(&message,  &mut buf).unwrap();

            let buf = Buf::from(buf);
            
            for peer_id in NodeId::targets(0..n) {
                let buf_clone = buf.clone();

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
                ).into_inner();

                // store serialized header + message
                let serialized = SerializedMessage::new(message.clone(), buf.clone());

                let stored = StoredMessage::new(header, serialized);

                let mut map = speculative_commits.lock().unwrap();

                map.insert(peer_id.into(), stored);
            }
        });

        // Vote for the received batch.
        // Leaders in this protocol must vote as they need to ack all
        // other request batches we have received and that are also a part of this instance
        // Also, since we can have # Leaders > f, if the leaders didn't partake in this
        // Instance we would have situations where faults joined with leaders would cause
        // Unresponsiveness
        let message = SystemMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::Prepare(current_digest),
        ));

        let targets = NodeId::targets(0..view.params().n());

        node.broadcast_signed(message, targets);

        //Notify the followers
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) =
                follower_handle.send(FollowerEvent::ReceivedConsensusMsg(view, msg))
            {
                error!("{:?}", err);
            }
        }

        //Notify the observers
        if let Err(err) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Prepare(seq)))
        {
            error!("{:?}", err);
        }
    }

    ///Handle a prepare message when we have not yet reached a consensus
    pub(super) fn handle_preparing_no_quorum(
        &mut self,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        _node: &Node<S::Data>,
    )
    {
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                curr_view,
                preparing_msg,
            )) {
                error!("{:?}", err);
            }
        }
    }

    ///Handle a prepare message when we have already obtained a valid quorum of messages
    pub(super) fn handle_preparing_quorum(
        &mut self,
        seq: SeqNo,
        current_digest: Digest,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
        log: &DecidingLog<S>,
        node: &Node<S::Data>,
    ) {
        let node_id = node.id();

        let speculative_commits = self.take_speculative_commits();

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
                ConsensusMessageKind::Commit(current_digest.clone()),
            ));

            debug!("{:?} // Broadcasting commit consensus message {:?}",
                        node_id, message);

            let targets = NodeId::targets(0..curr_view.params().n());

            node.broadcast_signed(message, targets);
        }

        log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();

        //Follower notifications
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                curr_view,
                preparing_msg,
            )) {
                error!("{:?}", err);
            }
        }

        //Observer notifications
        if let Err(err) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Commit(seq)))
        {
            error!("{:?}", err);
        }
    }

    pub(super) fn handle_committing_no_quorum(
        &mut self,
        curr_view: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        //Notify followers of the received message
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) = follower_handle
                .send(FollowerEvent::ReceivedConsensusMsg(curr_view, commit_msg))
            {
                error!("{:?}", err);
            }
        }
    }

    pub(super) fn handle_committing_quorum(
        &mut self,
        current_seq: SeqNo,
        view_info: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        //Notify follower of the received message
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) = follower_handle
                .send(FollowerEvent::ReceivedConsensusMsg(view_info, commit_msg))
            {
                error!("{:?}", err);
            }
        }

        if let Err(_) =
            self.observer_handle
                .tx()
                .send(MessageType::Event(ObserveEventKind::Consensus(
                    current_seq,
                )))
        {
            warn!("Failed to notify observers of the consensus instance")
        }
    }

    pub(super) fn handle_installed_seq_num(&mut self, seq: SeqNo) {
        let mut guard = self.consensus_guard.consensus_info().lock().unwrap();

        guard.0 = seq;
    }

    pub(super) fn handle_next_instance(&mut self, seq_no: SeqNo) {
        {
            let mut guard = self.consensus_guard.consensus_info().lock().unwrap();

            guard.0 = seq_no;
        }

        //Mark as ready for the next batch to come in
        self.consensus_guard.unlock_consensus();

        if let Err(_) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Ready(seq_no)))
        {
            warn!("Failed to notify observers of the consensus instance")
        }
    }

    pub(super) fn handle_finalize_view_change<T>(&mut self, synchronizer: &T)
        where
            T: AbstractSynchronizer<S>,
    {
        //Update the current view and seq numbers

        //Acq consensus guard since we already have the message to propose
        //So we don't want the proposer to propose anything yet
        self.consensus_guard.lock_consensus();

        let mut guard = self.consensus_guard.consensus_info().lock().unwrap();

        guard.1 = synchronizer.view();
    }


    pub(super) fn handle_poll_preparing_requests(
        &mut self,
        log: &PendingRequestLog<S>,
    ) -> ReplicaPreparingPollStatus
    {
        let iterator = self
            .missing_requests
            .iter()
            .enumerate()
            .filter(|(_index, digest)| log.has_pending_request(digest));

        for (index, _) in iterator {
            self.missing_swapbuf.push(index);
        }

        for index in self.missing_swapbuf.drain(..) {
            self.missing_requests.swap_remove_back(index);
        }

        if self.missing_requests.is_empty() {
            ReplicaPreparingPollStatus::MoveToPreparing
        } else {
            ReplicaPreparingPollStatus::Recv
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
                consensus_information: Arc::new(Mutex::new((next_seq, view))),
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
