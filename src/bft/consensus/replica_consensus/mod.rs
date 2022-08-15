use std::{
    collections::VecDeque,
    sync::{atomic::{Ordering, AtomicBool}, Arc, Mutex},
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
        observer::{MessageType, ObserverHandle},
        ViewInfo,
    },
    crypto::hash::Digest,
    executable::{Reply, Request, Service, State},
    ordering::{Orderable, SeqNo},
    sync::AbstractSynchronizer,
    threadpool,
};

use crate::bft::ordering::tbo_pop_message;

use super::{
    log::MemLog, AbstractConsensus, Consensus, ConsensusGuard, ConsensusPollStatus, ProtoPhase,
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
}

impl<S: Service + 'static> ReplicaConsensus<S> {
    pub(super) fn new(view: ViewInfo, next_seq: SeqNo, observer_handle: ObserverHandle) -> Self {
        Self {
            missing_requests: VecDeque::new(),
            missing_swapbuf: Vec::new(),
            speculative_commits: Arc::new(Mutex::new(IntMap::new())),
            consensus_guard: ConsensusGuard {
                consensus_lock: Arc::new(Mutex::new((next_seq, view))),
                consensus_guard: Arc::new(AtomicBool::new(false)),
            },
            observer_handle,
        }
    }

    pub(super) fn consensus_guard(&self) -> &ConsensusGuard {
        &self.consensus_guard
    }

    fn take_speculative_commits(&self) -> IntMap<StoredSerializedSystemMessage<S::Data>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::replace(&mut *map, IntMap::new())
    }

    pub(super) fn handle_preprepare_sucessfull(
        &mut self,
        base_consensus: &mut Consensus<S>,
        view: &ViewInfo,
        node: &Node<S::Data>,
    ) {
        // start speculatively creating COMMIT messages,
        // which involve potentially expensive signing ops
        let my_id = base_consensus.node_id;

        let seq = base_consensus.sequence_number();
        let view_seq = view.sequence_number();

        let sign_detached = node.sign_detached();
        let current_digest = base_consensus.current_digest.clone();
        let speculative_commits = Arc::clone(&self.speculative_commits);
        let n = view.params().n();

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

            let digest = <S::Data as DigestData>::serialize_digest(&message, &mut buf).unwrap();

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
                ConsensusMessageKind::Prepare(base_consensus.current_digest.clone()),
            ));

            let targets = NodeId::targets(0..view.params().n());

            node.broadcast(message, targets);
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

    pub(super) fn handle_preparing_no_quorum(
        &mut self,
        _base_consensus: &mut Consensus<S>,
        _curr_view: &ViewInfo,
        _log: &MemLog<State<S>, Request<S>, Reply<S>>,
        _node: &Node<S::Data>,
    ) {
    }

    pub(super) fn handle_preparing_quorum(
        &mut self,
        base_consensus: &mut Consensus<S>,
        curr_view: &ViewInfo,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        node: &Node<S::Data>,
    ) {
        let node_id = base_consensus.node_id;

        let speculative_commits = self.take_speculative_commits();

        if valid_spec_commits(&speculative_commits, base_consensus, curr_view) {
            for (_, msg) in speculative_commits.iter() {
                debug!("{:?} // Broadcasting speculative commit message {:?} (total of {} messages) to {} targets",
                     node_id, msg.message().original(), speculative_commits.len(), curr_view.params().n());
                break;
            }

            node.broadcast_serialized(speculative_commits);
        } else {
            let message = SystemMessage::Consensus(ConsensusMessage::new(
                base_consensus.sequence_number(),
                curr_view.sequence_number(),
                ConsensusMessageKind::Commit(base_consensus.current_digest.clone()),
            ));

            debug!(
                "{:?} // Broadcasting commit consensus message {:?}",
                node_id, message
            );

            let targets = NodeId::targets(0..curr_view.params().n());

            node.broadcast_signed(message, targets);
        }

        log.batch_meta().lock().commit_sent_time = Utc::now();

        if let Err(err) =
            self.observer_handle
                .tx()
                .send(MessageType::Event(ObserveEventKind::Commit(
                    base_consensus.sequence_number(),
                )))
        {
            error!("{:?}", err);
        }
    }

    pub(super) fn handle_committing_no_quorum(&mut self, base_consensus: &mut Consensus<S>) {}

    pub(super) fn handle_committing_quorum(&mut self, base_consensus: &mut Consensus<S>) {
        if let Err(_) =
            self.observer_handle
                .tx()
                .send(MessageType::Event(ObserveEventKind::Consensus(
                    base_consensus.sequence_number(),
                )))
        {
            warn!("Failed to notify observers of the consensus instance")
        }
    }

    pub(super) fn handle_installed_seq_num(&mut self, base_consensus: &mut Consensus<S>) {
        let mut guard = self.consensus_guard.consensus_info().lock().unwrap();

        guard.0 = base_consensus.curr_seq;
    }

    pub(super) fn handle_next_instance(&mut self, base_consensus: &mut Consensus<S>) {
        {
            let mut guard = self.consensus_guard.consensus_info().lock().unwrap();

            guard.0 = base_consensus.curr_seq;
        }

        self.consensus_guard
            .consensus_guard()
            .store(false, Ordering::SeqCst);

        if let Err(_) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Ready(
                base_consensus.curr_seq,
            )))
        {
            warn!("Failed to notify observers of the consensus instance")
        }
    }

    pub(super) fn handle_finalize_view_change<T>(&mut self, synchronizer: &T)
    where
        T: AbstractSynchronizer<S>,
    {
        //Update the current view and seq numbers
        {
            //Acq consensus guard since we already have the message to propose
            //So we don't want the proposer to propose anything yet
            self.consensus_guard
                .consensus_guard()
                .store(true, Ordering::SeqCst);

            let mut guard = self.consensus_guard.consensus_lock.lock().unwrap();

            guard.1 = synchronizer.view();
        }
    }

    pub(super) fn handle_poll_preparing_requests(
        &mut self,
        base_consensus: &mut Consensus<S>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) -> ConsensusPollStatus<Request<S>> {
        let iterator = self
            .missing_requests
            .iter()
            .enumerate()
            .filter(|(_index, digest)| log.has_request(digest));
        for (index, _) in iterator {
            self.missing_swapbuf.push(index);
        }
        for index in self.missing_swapbuf.drain(..) {
            self.missing_requests.swap_remove_back(index);
        }
        if self.missing_requests.is_empty() {
            extract_msg!(
                {
                    base_consensus.phase = ProtoPhase::Preparing(1);
                },
                ConsensusPollStatus::Recv,
                &mut base_consensus.tbo.get_queue,
                &mut base_consensus.tbo.prepares
            )
        } else {
            ConsensusPollStatus::Recv
        }
    }
}

#[inline]
fn valid_spec_commits<S>(
    speculative_commits: &IntMap<StoredSerializedSystemMessage<S::Data>>,
    consensus: &Consensus<S>,
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
            consensus.node_id, len, n
        );

        return false;
    }

    let seq_no = consensus.sequence_number();

    speculative_commits
        .values()
        .map(|stored| match stored.message().original() {
            SystemMessage::Consensus(c) => c,
            _ => unreachable!(),
        })
        .all(|commit| commit.sequence_number() == seq_no)
}
