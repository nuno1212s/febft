use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_core::messages::{ClientRqInfo, StoredRequestMessage};
use atlas_core::ordering_protocol::networking::serialize::NetworkView;
use atlas_core::smr::smr_decision_log::ShareableMessage;
use atlas_metrics::benchmarks::BatchMeta;
use atlas_metrics::metrics::metric_duration;

use crate::bft::log::decisions::{IncompleteProof, PrepareSet, ProofMetadata, ViewDecisionPair};
use crate::bft::message::{ConsensusMessageKind, PBFTMessage};
use crate::bft::metric::PRE_PREPARE_LOG_ANALYSIS_ID;
use crate::bft::sync::view::ViewInfo;

/// The log of messages for a given batch
pub struct MessageLog<O> {
    pre_prepare: Vec<Option<ShareableMessage<PBFTMessage<O>>>>,
    prepares: Vec<ShareableMessage<PBFTMessage<O>>>,
    commits: Vec<ShareableMessage<PBFTMessage<O>>>,
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
    // The overall digest of the entire batch
    pub(super) digest: Digest,
    // The ordering of the pre prepare requests
    pub(super) pre_prepare_ordering: Vec<Digest>,
    // The messages that are a part of this decision
    pub(super) contained_messages: FinishedMessageLog<O>,
    // The information of the client requests that are contained in this batch
    pub(super) client_request_info: Vec<ClientRqInfo>,
    // The client requests contained in this batch
    pub(super) client_requests: Vec<StoredRequestMessage<O>>,

    // The metadata for the batch
    pub(super) batch_meta: BatchMeta,
}

pub struct WorkingDecisionLog<O> {
    node_id: NodeId,
    seq_no: SeqNo,

    duplicate_detection: DuplicateReplicaEvaluator,
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
    // Which hash space should each leader be responsible for
    request_space_slices: BTreeMap<NodeId, (Vec<u8>, Vec<u8>)>,
    // The log of messages of the currently working decision
    message_log: MessageLog<O>,
    // Some logging information about metadata
    batch_meta: Arc<Mutex<BatchMeta>>,
    // The contained requests per each of the received pre prepares
    contained_requests: Vec<Option<Vec<StoredRequestMessage<O>>>>,
}

/// Checks to make sure replicas aren't providing more than one vote for the
/// Same consensus decision
#[derive(Default)]
pub struct DuplicateReplicaEvaluator {
    // The set of leaders that is currently in vigour for this consensus decision
    leader_set: Vec<NodeId>,
    // The set of leaders that have already sent a pre prepare message
    received_pre_prepare_messages: BTreeSet<NodeId>,
    // The set of leaders that have already sent a prepare message
    received_prepare_messages: BTreeSet<NodeId>,
    // The set of leaders that have already sent a commit message
    received_commit_messages: BTreeSet<NodeId>,
}

impl<O> MessageLog<O> {
    pub fn with_leader_count(leaders: usize, quorum: usize) -> Self {
        Self {
            pre_prepare: iter::repeat(None).take(leaders).collect(),
            prepares: Vec::with_capacity(quorum),
            commits: Vec::with_capacity(quorum),
        }
    }

    pub fn insert_pre_prepare(&mut self, index: usize, pre_prepare: ShareableMessage<PBFTMessage<O>>) {
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
            pre_prepares: self.pre_prepare.into_iter().map(|opt| opt.unwrap()).collect(),
            prepares: self.prepares,
            commits: self.commits,
        }
    }
}

impl<O> WorkingDecisionLog<O> where O: Clone {
    pub fn new(node: NodeId, seq: SeqNo, view: &ViewInfo) -> Self {
        let leader_count = view.leader_set().len();
        Self {
            node_id: node,
            seq_no: seq,
            duplicate_detection: Default::default(),
            batch_digest: None,
            pre_prepare_digests: iter::repeat(None).take(leader_count).collect(),
            current_received_pre_prepares: 0,
            current_batch_size: 0,
            client_rqs: vec![],
            leader_set: view.leader_set().clone(),
            request_space_slices: view.hash_space_division().clone(),
            message_log: MessageLog::with_leader_count(view.leader_set().len(), view.quorum()),
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
            contained_requests: iter::repeat(None).take(leader_count).collect(),
        }
    }

    pub fn update_current_view(&mut self, view: &ViewInfo) {
        self.leader_set = view.leader_set().clone();
        self.request_space_slices = view.hash_space_division().clone();
    }

    pub fn process_pre_prepare(&mut self,
                               s_message: ShareableMessage<PBFTMessage<O>>,
                               digest: Digest,
                               mut batch_rq_digests: Vec<ClientRqInfo>) -> Result<Option<ProofMetadata>> {
        let (header, message) = (s_message.header(), s_message.message().consensus());

        let start = Instant::now();

        let sending_leader = header.from();

        let slice = self.request_space_slices.get(&sending_leader)
            .ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                          format!("Failed to get request space for leader {:?}. Len: {:?}. {:?}",
                                                  sending_leader,
                                                  self.request_space_slices.len(),
                                                  self.request_space_slices).as_str()))?;

        if sending_leader != self.node_id {
            // Only check batches from other leaders since we implicitly trust in ourselves
            for request in &batch_rq_digests {
                if !crate::bft::sync::view::is_request_in_hash_space(&request.digest(), slice) {
                    return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                                      "This batch contains requests that are not in the hash space of the leader."));
                }
            }
        }

        self.duplicate_detection.insert_pre_prepare_received(sending_leader)?;

        // Get the correct index for this batch
        let leader_index = pre_prepare_index_of(&self.leader_set, &sending_leader)?;

        if leader_index >= self.pre_prepare_digests.len() {
            unreachable!("Cannot insert a pre prepare message that was sent by a leader that is out of bounds")
        }

        self.pre_prepare_digests[leader_index] = Some(digest);
        self.contained_requests[leader_index] = Some(match message.kind() {
            ConsensusMessageKind::PrePrepare(requests) => {
                requests.clone()
            }
            _ => unreachable!()
        });

        self.current_received_pre_prepares += 1;

        self.current_batch_size += batch_rq_digests.len();

        self.client_rqs.append(&mut batch_rq_digests);

        self.message_log.insert_pre_prepare(leader_index, s_message);

        metric_duration(PRE_PREPARE_LOG_ANALYSIS_ID, start.elapsed());

        // if we have received all of the messages in the set, calculate the digest.
        Ok(if self.current_received_pre_prepares == self.leader_set.len() {
            // We have received all of the required batches
            let result = self.calculate_instance_digest();

            let (digest, ordering) = result
                .ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog, "Failed to calculate instance digest"))?;

            self.batch_digest = Some(digest.clone());

            Some(ProofMetadata::new(self.seq_no, digest, ordering, self.current_batch_size))
        } else {
            None
        })
    }

    /// Process the message received
    pub(crate) fn process_message(&mut self, s_message: ShareableMessage<PBFTMessage<O>>) -> Result<()> {
        let (header, message) = (s_message.header(), s_message.message().consensus());

        match message.kind() {
            ConsensusMessageKind::Prepare(_) => {
                self.duplicate_detection.insert_prepare_received(header.from())?;
                self.message_log.insert_prepare(s_message);
            }
            ConsensusMessageKind::Commit(_) => {
                self.duplicate_detection.insert_commit_received(header.from())?;
                self.message_log.insert_commit(s_message);
            }
            _ => unreachable!()
        }

        Ok(())
    }

    /// Getter for batch_meta
    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    /// Getter for the current digest
    pub fn current_digest(&self) -> Option<Digest> {
        self.batch_digest.clone()
    }

    /// Get the current batch size in this consensus decision
    pub fn current_batch_size(&self) -> usize { self.current_batch_size }

    /// Calculate the instance of a completed consensus pre prepare phase with
    /// all the batches received
    fn calculate_instance_digest(&self) -> Option<(Digest, Vec<Digest>)> {
        let mut ctx = Context::new();

        let mut batch_ordered_digests = Vec::with_capacity(self.pre_prepare_digests.len());

        for order_digest in &self.pre_prepare_digests {
            if let Some(digest) = order_digest.clone() {
                ctx.update(digest.as_ref());
                batch_ordered_digests.push(digest);
            } else {
                return None;
            }
        }

        Some((ctx.finish(), batch_ordered_digests))
    }

    /// Get the current decision
    pub fn deciding(&self, f: usize) -> IncompleteProof {
        let write_set = PrepareSet({
            let mut set = Vec::new();

            for shareable_msg in self.message_log.prepares.iter().rev() {
                let digest = match shareable_msg.message().consensus().kind() {
                    ConsensusMessageKind::Prepare(d) => d.clone(),
                    _ => unreachable!(),
                };

                set.push(ViewDecisionPair(
                    shareable_msg.message().consensus().view(),
                    digest,
                ));
            }

            set
        });

        let quorum_prepares = 'outer: loop {
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
                        ConsensusMessageKind::Prepare(d) => d.clone(),
                        _ => unreachable!(),
                    };
                    break 'outer Some(ViewDecisionPair(stored.message().consensus().view(), digest));
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

        let current_digest = self.batch_digest?;

        let pre_prepare_ordering = self.pre_prepare_digests.into_iter().map(|elem| elem.unwrap()).collect();

        let mut requests = Vec::with_capacity(self.current_batch_size);

        for pre_prepare_request in self.contained_requests {
            requests.append(&mut pre_prepare_request.unwrap());
        }

        Some(CompletedBatch {
            seq: self.seq_no,
            digest: current_digest,
            pre_prepare_ordering,
            contained_messages: self.message_log.finalize(),
            client_request_info: self.client_rqs,
            batch_meta,
            client_requests: requests,
        })
    }
}

impl<O> Orderable for CompletedBatch<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl<O> CompletedBatch<O> {
    pub fn new(seq: SeqNo, digest: Digest, pre_prepare_ordering: Vec<Digest>,
               contained_messages: FinishedMessageLog<O>, client_request_info: Vec<ClientRqInfo>,
               client_requests: Vec<StoredRequestMessage<O>>, batch_meta: BatchMeta) -> Self {
        Self {
            seq,
            digest,
            pre_prepare_ordering,
            contained_messages,
            client_request_info,
            client_requests,
            batch_meta,
        }
    }

    pub fn request_count(&self) -> usize {
        self.client_requests.len()
    }
}

impl<O> Orderable for WorkingDecisionLog<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl DuplicateReplicaEvaluator {
    fn insert_pre_prepare_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_pre_prepare_messages.insert(node_id) {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                              "We have already received a message from that leader."));
        }

        Ok(())
    }
    fn insert_prepare_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_prepare_messages.insert(node_id) {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                              "We have already received a message from that leader."));
        }

        Ok(())
    }
    fn insert_commit_received(&mut self, node_id: NodeId) -> Result<()> {
        if !self.received_commit_messages.insert(node_id) {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                              "We have already received a message from that leader."));
        }

        Ok(())
    }
}


pub fn pre_prepare_index_from_digest_opt(prepare_set: &Vec<Option<Digest>>, digest: &Digest) -> Result<usize> {
    match prepare_set.iter().position(|pre_prepare| pre_prepare.map(|d| d == *digest).unwrap_or(false)) {
        None => {
            Err(Error::simple_with_msg(ErrorKind::Consensus, "Pre prepare is not part of the pre prepare set"))
        }
        Some(pos) => {
            Ok(pos)
        }
    }
}

pub fn pre_prepare_index_of_from_digest(prepare_set: &Vec<Digest>, preprepare: &Digest) -> Result<usize> {
    match prepare_set.iter().position(|pre_prepare| *pre_prepare == *preprepare) {
        None => {
            Err(Error::simple_with_msg(ErrorKind::Consensus, "Pre prepare is not part of the pre prepare set"))
        }
        Some(pos) => {
            Ok(pos)
        }
    }
}

pub fn pre_prepare_index_of(leader_set: &Vec<NodeId>, proposer: &NodeId) -> Result<usize> {
    match leader_set.iter().position(|node| *node == *proposer) {
        None => {
            Err(Error::simple_with_msg(ErrorKind::Consensus, "Proposer is not part of the leader set"))
        }
        Some(pos) => {
            Ok(pos)
        }
    }
}
