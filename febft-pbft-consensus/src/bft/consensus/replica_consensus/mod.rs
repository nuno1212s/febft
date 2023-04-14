use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool}, Mutex,
    },
};
use std::collections::BTreeMap;
use std::ops::Deref;

use chrono::Utc;
use intmap::IntMap;
use log::{debug, error, warn};
use febft_common::crypto::hash::Digest;

use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_common::threadpool;
use febft_communication::message::{NetworkMessageKind, SerializedMessage, StoredMessage, StoredSerializedNetworkMessage, System, WireMessage};
use febft_communication::{Node, NodePK, serialize};
use febft_communication::serialize::Buf;
use febft_execution::app::{Reply, Request, Service, State};
use febft_execution::serialize::SharedData;
use febft_messages::followers::{FollowerEvent, FollowerHandle};
use febft_messages::messages::{SystemMessage};
use febft_messages::serialize::StateTransferMessage;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, ObserveEventKind, PBFTMessage};
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::msg_log::pending_decision::PendingRequestLog;
use crate::bft::observer::{MessageType, ObserverHandle};
use crate::bft::PBFT;
use crate::bft::sync::AbstractSynchronizer;
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
pub struct ReplicaConsensus<D: SharedData + 'static, ST: StateTransferMessage> {
    missing_requests: VecDeque<Digest>,
    missing_swapbuf: Vec<usize>,
    speculative_commits: Arc<Mutex<BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>>>>,
    consensus_guard: ConsensusGuard,
    follower_handle: Option<FollowerHandle<PBFTConsensus<D>>>,
}

/// Preparing requests phase (This is no longer used, maybe remove it?)
pub(super) enum ReplicaPreparingPollStatus {
    Recv,
    MoveToPreparing,
}

impl<D: SharedData + 'static, ST: StateTransferMessage + 'static> ReplicaConsensus<D, ST> {
    pub(super) fn handle_pre_prepare_successful<NT>(
        &mut self,
        seq: SeqNo,
        current_digest: Digest,
        view: ViewInfo,
        msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
        node: &NT,
    ) where NT: Node<PBFT<D, ST>> {
        let my_id = node.id();
        let view_seq = view.sequence_number();
        let _quorum = view.quorum_members().clone();

        let sign_detached = node.pk_crypto().sign_detached();
        let n = view.params().n();

        let speculative_commits = Arc::clone(&self.speculative_commits);

        // start speculatively creating COMMIT messages,
        // which involve potentially expensive signing ops
        //Speculate in another thread.
        threadpool::execute(move || {
            // create COMMIT
            let message = NetworkMessageKind::from(
                SystemMessage::from_protocol_message(
                    PBFTMessage::Consensus(ConsensusMessage::new(
                        seq,
                        view_seq,
                        ConsensusMessageKind::Commit(current_digest.clone()),
                    ))));

            // serialize raw msg
            let mut buf = Vec::new();

            let digest = serialize::serialize_digest::<Vec<u8>,
                PBFT<D, ST>>(&message, &mut buf).unwrap();

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
        let message = PBFTMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::Prepare(current_digest),
        ));

        let targets = NodeId::targets(0..view.params().n());

        node.broadcast_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets).unwrap();

        /*TODO:
                //Notify the followers
                if let Some(follower_handle) = &self.follower_handle {

                     let follower_event = FollowerEvent::ReceivedConsensusMsg(view, msg);

                    if let Err(err) =
                        follower_handle.send(follower_event)
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
           */
    }

    ///Handle a prepare message when we have not yet reached a consensus
    pub(super) fn handle_preparing_no_quorum<NT>(
        &mut self,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
        _node: &NT,
    )
        where NT: Node<PBFT<D, ST>>
    {
        if let Some(follower_handle) = &self.follower_handle {
            /*TODO:
               if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                curr_view,
                preparing_msg,
            )) {
                error!("{:?}", err);
            }*/
        }
    }

    ///Handle a prepare message when we have already obtained a valid quorum of messages
    pub(super) fn handle_preparing_quorum<NT>(
        &mut self,
        seq: SeqNo,
        current_digest: Digest,
        curr_view: ViewInfo,
        preparing_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
        log: &DecidingLog<D>,
        node: &NT,
    ) where NT: Node<PBFT<D, ST>> {
        let node_id = node.id();

        let speculative_commits = self.take_speculative_commits();

        if valid_spec_commits::<D, ST>(&speculative_commits, node_id, seq, &curr_view) {
            for (_, msg) in speculative_commits.iter() {
                debug!("{:?} // Broadcasting speculative commit message {:?} (total of {} messages) to {} targets",
                     node_id, msg.message().original(), speculative_commits.len(), curr_view.params().n());
                break;
            }

            node.broadcast_serialized(speculative_commits);
        } else {
            let message = PBFTMessage::Consensus(ConsensusMessage::new(
                seq,
                curr_view.sequence_number(),
                ConsensusMessageKind::Commit(current_digest.clone()),
            ));

            debug!("{:?} // Broadcasting commit consensus message {:?}",
                        node_id, message);

            let targets = NodeId::targets(0..curr_view.params().n());

            node.broadcast_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
        }

        log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();

        //Follower notifications
        if let Some(follower_handle) = &self.follower_handle {
            /*
            if let Err(err) = follower_handle.send(FollowerEvent::ReceivedConsensusMsg(
                curr_view,
                preparing_msg,
            )) {
                error!("{:?}", err);
            }
             */
        }

        //Observer notifications
        /*
        if let Err(err) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Commit(seq)))
        {
            error!("{:?}", err);
        }
         */
    }

    pub(super) fn handle_committing_no_quorum(
        &mut self,
        curr_view: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
    ) {
        //Notify followers of the received message
        /*TODO:
        if let Some(follower_handle) = &self.follower_handle {
            if let Err(err) = follower_handle
                .send(FollowerEvent::ReceivedConsensusMsg(curr_view, commit_msg))
            {
                error!("{:?}", err);
            }
        }

         */
    }

    pub(super) fn handle_committing_quorum(
        &mut self,
        current_seq: SeqNo,
        view_info: ViewInfo,
        commit_msg: Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
    ) {
        //Notify follower of the received message
        /*TODO:
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

         */
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

        /*
        TODO:
        if let Err(_) = self
            .observer_handle
            .tx()
            .send(MessageType::Event(ObserveEventKind::Ready(seq_no)))
        {
            warn!("Failed to notify observers of the consensus instance")
        }

         */
    }

    pub(super) fn handle_finalize_view_change<T>(&mut self, synchronizer: &T)
        where
            T: AbstractSynchronizer<D>,
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
        log: &PendingRequestLog<D>,
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

impl<D: SharedData + 'static, ST: StateTransferMessage> ReplicaConsensus<D, ST> {
    pub(super) fn new(
        view: ViewInfo,
        next_seq: SeqNo,
        follower_handle: Option<FollowerHandle<PBFTConsensus<D>>>,
    ) -> Self {
        Self {
            missing_requests: VecDeque::new(),
            missing_swapbuf: Vec::new(),
            speculative_commits: Arc::new(Mutex::new(Default::default())),
            consensus_guard: ConsensusGuard {
                consensus_information: Arc::new(Mutex::new((next_seq, view))),
                consensus_guard: Arc::new(AtomicBool::new(false)),
            },
            follower_handle,
        }
    }

    pub(super) fn consensus_guard(&self) -> &ConsensusGuard {
        &self.consensus_guard
    }

    fn take_speculative_commits(&self) -> BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::replace(&mut *map, BTreeMap::new())
    }
}

#[inline]
fn valid_spec_commits<D, ST>(
    speculative_commits: &BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>>,
    node_id: NodeId,
    seq_no: SeqNo,
    view: &ViewInfo,
) -> bool
    where
        D: SharedData + 'static,
        ST: StateTransferMessage
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
        .map(|stored| match stored.message().original().deref_system() {
            SystemMessage::ProtocolMessage(protocol) => {
                match protocol.deref() {
                    PBFTMessage::Consensus(consensus) => consensus,
                    _ => { unreachable!() }
                }
            }
            _ => { unreachable!() }
        })
        .all(|commit| commit.sequence_number() == seq_no)
}
