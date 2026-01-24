use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use thiserror::Error;

use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::Err;
use atlas_communication::message::StoredMessage;
use atlas_core::messages::ClientRqInfo;
use atlas_core::ordering_protocol::decision::{DecisionRequestBatch, DecisionRequests};
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::ordering_protocol::ShareableMessage;
use atlas_metrics::benchmarks::BatchMeta;
use atlas_metrics::metrics::metric_duration;

use crate::bft::log::decisions::{IncompleteProof, PrepareSet, ProofMetadata, ViewDecisionPair};
use crate::bft::message::{ConsensusMessageKind, PBFTMessage};
use crate::bft::metric::PRE_PREPARE_LOG_ANALYSIS_ID;
use crate::bft::sync::view;
use crate::bft::sync::view::ViewInfo;

/// The log of messages for a given batch
pub struct MessageLog<O> {
    pre_prepare: Vec<Option<ShareableMessage<PBFTMessage<O>>>>,
    prepares: Vec<ShareableMessage<PBFTMessage<O>>>,
    commits: Vec<ShareableMessage<PBFTMessage<O>>>,
}

impl<O> MessageLog<O> {
    pub fn with_leader_count(leaders: usize, quorum: usize) -> Self {
        Self {
            pre_prepare: iter::repeat_n(None, leaders).collect(),
            prepares: Vec::with_capacity(quorum),
            commits: Vec::with_capacity(quorum),
        }
    }

    pub fn insert_pre_prepare(
        &mut self,
        index: usize,
        pre_prepare: ShareableMessage<PBFTMessage<O>>,
    ) {
        let _ = self.pre_prepare.get_mut(index).unwrap().insert(pre_prepare);
    }

    pub fn insert_prepare(&mut self, prepare: ShareableMessage<PBFTMessage<O>>) {
        self.prepares.push(prepare);
    }

    pub fn insert_commit(&mut self, commit: ShareableMessage<PBFTMessage<O>>) {
        self.commits.push(commit);
    }

    pub fn finalize(self) -> FinishedMessageLog<O> {
        FinishedMessageLog {
            pre_prepares: self
                .pre_prepare
                .into_iter()
                .map(|opt| opt.unwrap())
                .collect(),
            prepares: self.prepares,
            commits: self.commits,
        }
    }
}

pub struct FinishedMessageLog<O> {
    pub(super) pre_prepares: Vec<ShareableMessage<PBFTMessage<O>>>,
    pub(super) prepares: Vec<ShareableMessage<PBFTMessage<O>>>,
    pub(super) commits: Vec<ShareableMessage<PBFTMessage<O>>>,
}

/// Information about the completed batch, the contained requests and
/// other relevant information
pub struct CompletedBatch<O> {
    pub(super) seq: SeqNo,
    pub(super) proof_metadata: ProofMetadata,
    // The messages that are a part of this decision
    pub(super) contained_messages: FinishedMessageLog<O>,
    // The metadata for the batch
    #[allow(dead_code)]
    pub(super) batch_meta: BatchMeta,
}

impl<O> Orderable for CompletedBatch<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<O> CompletedBatch<O> {
    pub fn new(
        seq: SeqNo,
        proof_metadata: ProofMetadata,
        contained_messages: FinishedMessageLog<O>,
        batch_meta: BatchMeta,
    ) -> Self {
        Self {
            seq,
            proof_metadata,
            contained_messages,
            batch_meta,
        }
    }

    pub fn request_count(&self) -> usize {
        self.proof_metadata.contained_client_rqs()
    }
}

enum PrePrepareLogState<O> {
    InProgress(PrePrepareLog<O>),
    PrePrepareReceived(ProofMetadata),
}

pub struct WorkingDecisionLog<O> {
    node_id: NodeId,
    seq_no: SeqNo,
    pre_prepare_log: PrePrepareLogState<O>,
    duplicate_detection: DuplicateReplicaEvaluator,
    // The log of messages of the currently working decision
    message_log: MessageLog<O>,
    // Some logging information about metadata
    batch_meta: Arc<Mutex<BatchMeta>>,
}
impl<O> WorkingDecisionLog<O>
where
    O: Clone,
{
    pub fn new(node: NodeId, seq: SeqNo, view: &ViewInfo) -> Self {
        Self {
            node_id: node,
            seq_no: seq,
            pre_prepare_log: PrePrepareLogState::InProgress(PrePrepareLog::new(seq, view)),
            duplicate_detection: Default::default(),
            message_log: MessageLog::with_leader_count(view.leader_set().len(), view.quorum()),
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
        }
    }

    pub fn current_digest(&self) -> Option<Digest> {
        match &self.pre_prepare_log {
            PrePrepareLogState::InProgress(log) => log.current_digest(),
            PrePrepareLogState::PrePrepareReceived(metadata) => Some(metadata.batch_digest()),
        }
    }

    pub fn current_batch_size(&self) -> usize {
        match &self.pre_prepare_log {
            PrePrepareLogState::InProgress(log) => log.current_batch_size(),
            PrePrepareLogState::PrePrepareReceived(metadata) => metadata.contained_client_rqs(),
        }
    }

    pub fn update_current_view(&mut self, view: &ViewInfo) {
        match &mut self.pre_prepare_log {
            PrePrepareLogState::InProgress(log) => log.update_current_view(view),
            PrePrepareLogState::PrePrepareReceived(_) => {
                // No need to update anything as we have already received all pre prepares
            }
        }
    }

    pub fn process_pre_prepare(
        &mut self,
        s_message: ShareableMessage<PBFTMessage<O>>,
        digest: Digest,
        batch_rq_digests: Vec<ClientRqInfo>,
    ) -> Result<Option<(ProofMetadata, DecisionRequests<O>)>> {
        let pre_prepare_log = match &mut self.pre_prepare_log {
            PrePrepareLogState::InProgress(log) => log,
            PrePrepareLogState::PrePrepareReceived(_) => {
                // We have already received all pre prepares and returned the information
                return Ok(None);
            }
        };

        let start = Instant::now();

        let sending_leader = s_message.header().from();

        if sending_leader != self.node_id {
            // Implicit trust on self messages and skip the check
            if !pre_prepare_log.are_requests_in_leader_space(sending_leader, &batch_rq_digests)? {
                return Err!(DecidingLogError::BatchContainsRequestsNotInLeaderAddrSpace(
                    sending_leader
                ));
            }
        }

        self.duplicate_detection
            .insert_pre_prepare_received(sending_leader)?;

        let leader_index = pre_prepare_log.leader_index_for(sending_leader)?;

        self.message_log
            .insert_pre_prepare(leader_index, s_message.clone());

        let result = pre_prepare_log.process_pre_prepare(s_message, digest, batch_rq_digests)?;

        if let Some(proof_metadata) = result {
            let preprepare_log = std::mem::replace(
                &mut self.pre_prepare_log,
                PrePrepareLogState::PrePrepareReceived(proof_metadata.clone()),
            );

            let PrePrepareLogState::InProgress(log) = preprepare_log else {
                unreachable!("Pre prepare log not in progress state");
            };

            let decision_requests = log.finalize_pre_prepare(self.seq_no)?;

            metric_duration(PRE_PREPARE_LOG_ANALYSIS_ID, start.elapsed());

            Ok(Some((proof_metadata, decision_requests)))
        } else {
            metric_duration(PRE_PREPARE_LOG_ANALYSIS_ID, start.elapsed());

            Ok(None)
        }
    }

    /// Process the message received
    pub(crate) fn process_message(
        &mut self,
        s_message: ShareableMessage<PBFTMessage<O>>,
    ) -> Result<()> {
        let (header, message) = (s_message.header(), s_message.message().consensus());

        match message.kind() {
            ConsensusMessageKind::Prepare(_) => {
                self.duplicate_detection
                    .insert_prepare_received(header.from())?;
                self.message_log.insert_prepare(s_message);
            }
            ConsensusMessageKind::Commit(_) => {
                self.duplicate_detection
                    .insert_commit_received(header.from())?;
                self.message_log.insert_commit(s_message);
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    /// Getter for batch_meta
    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    /// Get the current decision
    pub fn deciding(&self, f: usize) -> IncompleteProof {
        let write_set = PrepareSet({
            let mut set = Vec::new();

            for shareable_msg in self.message_log.prepares.iter().rev() {
                let digest = match shareable_msg.message().consensus().kind() {
                    ConsensusMessageKind::Prepare(d) => *d,
                    _ => unreachable!(),
                };

                set.push(ViewDecisionPair(
                    shareable_msg.message().consensus().view(),
                    digest,
                ));
            }

            set
        });

        let quorum_prepares = 'outer: {
            let quorum = f << 1;
            let mut last_view = None;
            let mut count = 0;

            for stored in self.message_log.prepares.iter().rev() {
                match last_view {
                    None => (),
                    Some(v) if stored.message().consensus().view() == v => (),
                    _ => count = 0,
                }
                last_view = Some(stored.message().consensus().view());
                count += 1;
                if count == quorum {
                    let digest = match stored.message().consensus().kind() {
                        ConsensusMessageKind::Prepare(d) => *d,
                        _ => unreachable!(),
                    };
                    break 'outer Some(ViewDecisionPair(
                        stored.message().consensus().view(),
                        digest,
                    ));
                }
            }

            break 'outer None;
        };

        IncompleteProof::new(self.seq_no, write_set, quorum_prepares)
    }

    /// Indicate that the batch is finished processing and
    /// return the relevant information for it
    pub fn finish_processing_batch(self) -> Option<CompletedBatch<O>> {
        let new_meta = BatchMeta::new();
        let batch_meta = std::mem::replace(&mut *self.batch_meta().lock().unwrap(), new_meta);

        let proof_metadata = match self.pre_prepare_log {
            PrePrepareLogState::PrePrepareReceived(metadata) => metadata,
            _ => unreachable!("Pre prepare log not in finished state"),
        };

        Some(CompletedBatch {
            seq: self.seq_no,
            proof_metadata,
            contained_messages: self.message_log.finalize(),
            batch_meta,
        })
    }
}

struct PrePrepareLog<O> {
    seq_no: SeqNo,
    //The digest of the entire batch that is currently being processed
    // This will only be calculated when we receive all of the requests necessary
    // As this digest requires the knowledge of all of them
    batch_digest: Option<Digest>,
    // The digests of all received pre prepares
    // We store a vec of options because we want to store the digests in the correct ordering
    pre_prepare_digests: Vec<Option<Digest>>,
    // How many pre prepares have we received
    current_received_pre_prepares: usize,
    //The size of batch that is currently being processed. Increases as we receive more pre prepares
    current_batch_size: usize,
    //The client requests that are currently being processed
    //Does not have to follow the correct order, only has to contain the requests
    client_rqs: Vec<ClientRqInfo>,
    // The set of leaders that is currently in vigour for this consensus decision
    leader_set: Vec<NodeId>,
    // Which hash space should each leader be responsible for (multiple leader optimization)
    request_space_slices: BTreeMap<NodeId, (Vec<u8>, Vec<u8>)>,
    // The contained requests per each of the received pre prepares
    contained_requests: Vec<Option<Vec<StoredMessage<O>>>>,
}

impl<O> PrePrepareLog<O>
where
    O: Clone,
{
    pub fn new(seq_no: SeqNo, view: &ViewInfo) -> Self {
        let leader_count = view.leader_set().len();

        Self {
            seq_no,
            batch_digest: None,
            pre_prepare_digests: iter::repeat_n(None, leader_count).collect(),
            current_received_pre_prepares: 0,
            current_batch_size: 0,
            client_rqs: vec![],
            leader_set: view.leader_set().clone(),
            request_space_slices: view.hash_space_division().clone(),
            contained_requests: iter::repeat_n(None, leader_count).collect(),
        }
    }

    fn update_current_view(&mut self, view: &ViewInfo) {
        self.leader_set = view.leader_set().clone();
        self.request_space_slices = view.hash_space_division().clone();
    }

    fn leader_index_for(&self, leader: NodeId) -> Result<usize> {
        let leader_index = pre_prepare_index_of(&self.leader_set, &leader)?;

        if leader_index >= self.pre_prepare_digests.len() {
            return Err!(DecidingLogError::LeaderNotInLeaderSet(leader));
        }

        Ok(leader_index)
    }

    /// Getter for the current digest
    pub fn current_digest(&self) -> Option<Digest> {
        self.batch_digest
    }

    /// Get the current batch size in this consensus decision
    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size
    }

    /// Checks whether all the requests in the batch are in the
    /// address space of the given leader
    fn are_requests_in_leader_space(
        &self,
        leader: NodeId,
        request_digest: &[ClientRqInfo],
    ) -> Result<bool> {
        let slice = self
            .request_space_slices
            .get(&leader)
            .ok_or(DecidingLogError::FailedToGetLeadersRequestSpace(leader))?;

        for request_digest in request_digest {
            if !view::is_request_in_hash_space(request_digest.digest(), slice) {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn process_pre_prepare(
        &mut self,
        s_message: ShareableMessage<PBFTMessage<O>>,
        _digest: Digest,
        mut batch_rq_digests: Vec<ClientRqInfo>,
    ) -> Result<Option<ProofMetadata>> {
        let (header, message) = (s_message.header(), s_message.message().consensus());

        let sending_leader = header.from();
        let leader_index = self.leader_index_for(sending_leader)?;

        self.pre_prepare_digests[leader_index] = Some(*s_message.header().digest());
        self.contained_requests[leader_index] = Some(match message.kind() {
            ConsensusMessageKind::PrePrepare(requests) => requests.clone(),
            _ => unreachable!(),
        });

        self.current_received_pre_prepares += 1;

        self.current_batch_size += batch_rq_digests.len();

        self.client_rqs.append(&mut batch_rq_digests);

        // if we have received all the messages in the set, calculate the digest.

        if self.current_received_pre_prepares == self.leader_set.len() {
            // We have received all the required batches
            let result = self.calculate_instance_digest();

            let (digest, ordering) =
                result.ok_or(DecidingLogError::FailedToCalculateDigest(self.seq_no))?;

            self.batch_digest = Some(digest);

            Ok(Some(ProofMetadata::new(
                self.seq_no,
                digest,
                ordering,
                self.current_batch_size,
            )))
        } else {
            Ok(None)
        }
    }

    fn can_be_finalized(&self) -> bool {
        self.current_received_pre_prepares == self.leader_set.len()
    }

    /// Calculate the instance of a completed consensus pre prepare phase with
    /// all the batches received
    fn calculate_instance_digest(&self) -> Option<(Digest, Vec<Digest>)> {
        let mut ctx = Context::new();

        let mut batch_ordered_digests = Vec::with_capacity(self.pre_prepare_digests.len());

        for order_digest in &self.pre_prepare_digests {
            if let Some(digest) = *order_digest {
                ctx.update(digest.as_ref());
                batch_ordered_digests.push(digest);
            } else {
                return None;
            }
        }

        Some((ctx.finish(), batch_ordered_digests))
    }

    fn finalize_pre_prepare(self, seq_no: SeqNo) -> Result<DecisionRequests<O>> {
        if !self.can_be_finalized() {
            return Err!(DecidingLogError::PrePrepareLogCannotBeFinalizedYet);
        }

        let mut requests = Vec::with_capacity(self.current_batch_size);

        for pre_prepare_request in self.contained_requests {
            requests.append(&mut pre_prepare_request.unwrap());
        }

        let request_batch = DecisionRequestBatch::new(seq_no, requests, None);

        Ok(DecisionRequests::new(
            seq_no,
            request_batch,
            self.client_rqs.clone(),
            self.batch_digest.unwrap(),
        ))
    }
}

/// Checks to make sure replicas aren't providing more than one vote for the
/// Same consensus decision
#[derive(Default)]
pub struct DuplicateReplicaEvaluator {
    // The set of leaders that is currently in vigour for this consensus decision
    #[allow(dead_code)]
    leader_set: Vec<NodeId>,
    // The set of leaders that have already sent a pre prepare message
    received_pre_prepare_messages: BTreeSet<NodeId>,
    // The set of leaders that have already sent a prepare message
    received_prepare_messages: BTreeSet<NodeId>,
    // The set of leaders that have already sent a commit message
    received_commit_messages: BTreeSet<NodeId>,
}


impl<O> Orderable for WorkingDecisionLog<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl DuplicateReplicaEvaluator {
    fn insert_pre_prepare_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_pre_prepare_messages.insert(node_id) {
            return Err!(DecidingLogError::DuplicateVoteFromNode(node_id));
        }

        Ok(())
    }
    fn insert_prepare_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_prepare_messages.insert(node_id) {
            return Err!(DecidingLogError::DuplicateVoteFromNode(node_id));
        }

        Ok(())
    }
    fn insert_commit_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_commit_messages.insert(node_id) {
            return Err!(DecidingLogError::DuplicateVoteFromNode(node_id));
        }

        Ok(())
    }
}

pub fn pre_prepare_index_from_digest_opt(
    prepare_set: &[Option<Digest>],
    digest: &Digest,
) -> Result<usize> {
    match prepare_set
        .iter()
        .position(|pre_prepare| pre_prepare.map(|d| d == *digest).unwrap_or(false))
    {
        None => {
            Err!(DecidingLogError::PrePrepareNotPartOfSet(
                *digest,
                prepare_set.iter().cloned().flatten().collect()
            ))
        }
        Some(pos) => Ok(pos),
    }
}

pub fn pre_prepare_index_of_from_digest(
    prepare_set: &[Digest],
    pre_prepare: &Digest,
) -> Result<usize> {
    match prepare_set
        .iter()
        .position(|pre_prepare_2| *pre_prepare == *pre_prepare_2)
    {
        None => {
            Err!(DecidingLogError::PrePrepareNotPartOfSet(
                *pre_prepare,
                prepare_set.to_owned()
            ))
        }
        Some(pos) => Ok(pos),
    }
}

pub fn pre_prepare_index_of(leader_set: &[NodeId], proposer: &NodeId) -> Result<usize> {
    match leader_set.iter().position(|node| *node == *proposer) {
        None => {
            Err!(DecidingLogError::ProposerNotInLeaderSet(
                *proposer,
                leader_set.to_owned()
            ))
        }
        Some(pos) => Ok(pos),
    }
}

#[derive(Error, Debug)]
pub enum DecidingLogError {
    #[error("Proposer is not part of leader set {0:?}, leader set {1:?}")]
    ProposerNotInLeaderSet(NodeId, Vec<NodeId>),
    #[error("Pre prepare is not part of the pre prepare set. {0:?} vs {1:?}")]
    PrePrepareNotPartOfSet(Digest, Vec<Digest>),
    #[error("We have received a duplicate vote from node {0:?}")]
    DuplicateVoteFromNode(NodeId),
    #[error("Failed to calculate instance digest {0:?}")]
    FailedToCalculateDigest(SeqNo),
    #[error("Failed leader not in leader set")]
    LeaderNotInLeaderSet(NodeId),
    #[error("Batch contains requests that are not in leader's ({0:?}) address space")]
    BatchContainsRequestsNotInLeaderAddrSpace(NodeId),
    #[error("Failed to get leader's request space {0:?}")]
    FailedToGetLeadersRequestSpace(NodeId),
    #[error("Pre prepare log cannot be finalized yet")]
    PrePrepareLogCannotBeFinalizedYet,
}
