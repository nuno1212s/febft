use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::iter::zip;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::globals::ReadOnly;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::message::StoredMessage;
use atlas_core::messages::ClientRqInfo;
use atlas_core::ordering_protocol::DecisionInformation;
use atlas_metrics::benchmarks::BatchMeta;
use atlas_metrics::metrics::metric_duration;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::metric::PRE_PREPARE_LOG_ANALYSIS_ID;
use crate::bft::msg_log::decisions::{IncompleteProof, Proof, ProofMetadata, StoredConsensusMessage, ViewDecisionPair, PrepareSet};
use crate::bft::sync::view::ViewInfo;

/// A batch that has been decided by the consensus instance and is now ready to be delivered to the
/// Executor for execution.
/// Contains all of the necessary information for when we are using the strict persistency mode
pub struct CompletedBatch<O> {
    // The sequence number of the batch
    seq_no: SeqNo,

    //The digest of the batch
    batch_digest: Digest,
    // The ordering of the pre prepares
    pre_prepare_ordering: Vec<Digest>,
    // The prepare message of the batch
    pre_prepare_messages: Vec<StoredConsensusMessage<O>>,
    // The prepare messages for this batch
    prepare_messages: Vec<StoredConsensusMessage<O>>,
    // The commit messages for this batch
    commit_messages: Vec<StoredConsensusMessage<O>>,
    // The information of the client requests that are contained in this batch
    client_requests: Vec<ClientRqInfo>,
    //The messages that must be persisted for this consensus decision to be executable
    //This should contain the pre prepare, quorum of prepares and quorum of commits
    messages_to_persist: Vec<Digest>,

    // The metadata for this batch (mostly statistics)
    batch_meta: BatchMeta,
}

pub struct DecidingLog<O> {
    node_id: NodeId,
    seq_no: SeqNo,

    // Detect duplicate requests sent by replicas
    duplicate_detection: DuplicateReplicaEvaluator,
    //The digest of the entire batch that is currently being processed
    // This will only be calculated when we receive all of the requests necessary
    // As this digest requires the knowledge of all of them
    current_digest: Option<Digest>,
    // How many pre prepares have we received
    current_received_pre_prepares: usize,
    //The size of batch that is currently being processed. Increases as we receive more pre prepares
    current_batch_size: usize,
    //The client requests that are currently being processed
    //Does not have to follow the correct order, only has to contain the requests
    client_rqs: Vec<ClientRqInfo>,

    // The message log of the current ongoing decision
    ongoing_decision: OnGoingDecision<O>,

    // The set of leaders that is currently in vigour for this consensus decision
    leader_set: Vec<NodeId>,
    // Which hash space should each leader be responsible for
    request_space_slices: BTreeMap<NodeId, (Vec<u8>, Vec<u8>)>,

    //A list of digests of all consensus related messages pertaining to this
    //Consensus instance. Used to keep track of if the persistent log has saved the messages already
    //So the requests can be executed
    current_messages_to_persist: Vec<Digest>,

    // Some logging information about metadata
    batch_meta: Arc<Mutex<BatchMeta>>,
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

/// Store the messages corresponding to a given ongoing consensus decision
pub struct OnGoingDecision<O> {
    pre_prepare_digests: Vec<Option<Digest>>,
    // This must be a vec of options since we want to keep the ordering of the pre prepare messages
    // Even when the messages have not yet been received
    pre_prepare_messages: Vec<Option<StoredConsensusMessage<O>>>,
    prepare_messages: Vec<StoredConsensusMessage<O>>,
    commit_messages: Vec<StoredConsensusMessage<O>>,
}

/// Information about a full batch
pub type FullBatch = ProofMetadata;

impl<O> DecidingLog<O> {
    pub fn new(node_id: NodeId, seq_no: SeqNo, view: &ViewInfo) -> Self {
        Self {
            node_id,
            seq_no,
            duplicate_detection: Default::default(),
            current_digest: None,
            current_received_pre_prepares: 0,
            ongoing_decision: OnGoingDecision::initialize(view.leader_set().len()),
            leader_set: view.leader_set().clone(),
            request_space_slices: view.hash_space_division().clone(),
            current_batch_size: 0,
            current_messages_to_persist: vec![],
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
            client_rqs: vec![],
        }
    }

    /// Update our log to reflect the new views
    pub fn update_current_view(&mut self, view: &ViewInfo) {
        self.leader_set = view.leader_set().clone();
        self.request_space_slices = view.hash_space_division().clone();
    }

    /// Getter for batch_meta
    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    /// Getter for the current digest
    pub fn current_digest(&self) -> Option<Digest> {
        self.current_digest.clone()
    }

    /// Get the current batch size in this consensus decision
    pub fn current_batch_size(&self) -> usize { self.current_batch_size }

    /// Inform the log that we are now processing a new batch of operations
    pub fn process_pre_prepare(&mut self,
                               request_batch: StoredConsensusMessage<O>,
                               digest: Digest,
                               mut batch_rq_digests: Vec<ClientRqInfo>) -> Result<Option<FullBatch>> {
        let start = Instant::now();

        let sending_leader = request_batch.header().from();

        let slice = self.request_space_slices.get(&sending_leader)
            .ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                          format!("Failed to get request space for leader {:?}. Len: {:?}. {:?}",
                                                  sending_leader,
                                                  self.request_space_slices.len(),
                                                  self.request_space_slices).as_str()))?;

        if request_batch.header().from() != self.node_id {
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

        self.ongoing_decision.insert_pre_prepare(leader_index, request_batch.clone());

        self.current_received_pre_prepares += 1;

        //
        self.current_batch_size += batch_rq_digests.len();

        self.client_rqs.append(&mut batch_rq_digests);

        // Register this new batch as one that must be persisted for this batch to be executed
        self.register_message_to_save(digest);

        metric_duration(PRE_PREPARE_LOG_ANALYSIS_ID, start.elapsed());

        // if we have received all of the messages in the set, calculate the digest.
        Ok(if self.current_received_pre_prepares == self.leader_set.len() {
            // We have received all of the required batches
            let result = self.calculate_instance_digest();

            let (digest, ordering) = result
                .ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog, "Failed to calculate instance digest"))?;

            self.current_digest = Some(digest.clone());

            Some(ProofMetadata::new(self.seq_no, digest, ordering))
        } else {
            None
        })
    }

    /// Calculate the instance of a completed consensus pre prepare phase with
    /// all the batches received
    fn calculate_instance_digest(&self) -> Option<(Digest, Vec<Digest>)> {
        let mut ctx = Context::new();

        let mut batch_ordered_digests = Vec::with_capacity(self.ongoing_decision.pre_prepare_digests.len());

        for order_digest in &self.ongoing_decision.pre_prepare_digests {
            if let Some(digest) = order_digest.clone() {
                ctx.update(digest.as_ref());
                batch_ordered_digests.push(digest);
            } else {
                return None;
            }
        }

        Some((ctx.finish(), batch_ordered_digests))
    }

    /// Process the message received
    pub(crate) fn process_message(&mut self, message: StoredConsensusMessage<O>) -> Result<()> {
        match message.message().consensus().kind() {
            ConsensusMessageKind::Prepare(_) => {
                self.duplicate_detection.insert_prepare_received(message.header().from())?;
            }
            ConsensusMessageKind::Commit(_) => {
                self.duplicate_detection.insert_commit_received(message.header().from())?;
            }
            _ => unreachable!()
        }

        self.ongoing_decision.insert_message(message);

        Ok(())
    }

    /// Get the current decision
    pub fn deciding(&self, f: usize) -> IncompleteProof {
        let in_exec = self.seq_no;

        self.ongoing_decision.deciding(in_exec, f)
    }

    /// Indicate that the batch is finished processing and
    /// return the relevant information for it
    pub fn finish_processing_batch(self) -> Option<CompletedBatch<O>> {
        let new_meta = BatchMeta::new();
        let batch_meta = std::mem::replace(&mut *self.batch_meta().lock().unwrap(), new_meta);

        let OnGoingDecision {
            pre_prepare_digests,
            pre_prepare_messages,
            prepare_messages,
            commit_messages
        } = self.ongoing_decision;

        let current_digest = self.current_digest?;

        let pre_prepare_ordering = pre_prepare_digests.into_iter().map(|elem| elem.unwrap()).collect();
        let pre_prepare_messages = pre_prepare_messages.into_iter().map(|elem| elem.unwrap()).collect();

        let messages_to_persist = self.current_messages_to_persist;

        Some(CompletedBatch {
            batch_digest: current_digest,
            seq_no: self.seq_no,
            pre_prepare_ordering,
            pre_prepare_messages,
            prepare_messages,
            commit_messages,
            client_requests: self.client_rqs,
            messages_to_persist,
            batch_meta,
        })
    }

    fn register_message_to_save(&mut self, message: Digest) {
        self.current_messages_to_persist.push(message);
    }
}

impl<O> OnGoingDecision<O> {
    pub fn initialize(leader_count: usize) -> Self {
        Self {
            pre_prepare_digests: iter::repeat(None).take(leader_count).collect(),
            pre_prepare_messages: iter::repeat(None).take(leader_count).collect(),
            prepare_messages: Vec::new(),
            commit_messages: Vec::new(),
        }
    }

    pub fn initialize_with_ordering(leader_count: usize, ordering: Vec<Digest>) -> Self {
        Self {
            pre_prepare_digests: ordering.into_iter().map(|d| Some(d)).collect(),
            pre_prepare_messages: iter::repeat(None).take(leader_count).collect(),
            prepare_messages: Vec::new(),
            commit_messages: Vec::new(),
        }
    }

    /// Insert a pre prepare message into this on going decision
    fn insert_pre_prepare(&mut self, index: usize, pre_prepare: StoredConsensusMessage<O>) {
        if index >= self.pre_prepare_messages.len() {
            unreachable!("Cannot insert a pre prepare message that was sent by a leader that is out of bounds")
        }

        self.pre_prepare_digests[index] = Some(pre_prepare.header().digest().clone());
        self.pre_prepare_messages[index] = Some(pre_prepare);
    }

    /// Insert a consensus message into this on going decision
    fn insert_message(&mut self, message: StoredConsensusMessage<O>) {
        match message.message().consensus().kind() {
            ConsensusMessageKind::Prepare(_) => {
                self.prepare_messages.push(message);
            }
            ConsensusMessageKind::Commit(_) => {
                self.commit_messages.push(message);
            }
            _ => {
                unreachable!("Please use insert_pre_prepare to insert a pre prepare message")
            }
        }
    }

    /// Insert a message from the stored message into this on going decision
    pub fn insert_persisted_msg(&mut self, message: StoredConsensusMessage<O>) -> Result<()> {
        match message.message().consensus().kind() {
            ConsensusMessageKind::PrePrepare(_) => {
                let index = pre_prepare_index_from_digest_opt(&self.pre_prepare_digests, message.header().digest())?;

                self.pre_prepare_messages[index] = Some(message);
            }
            _ => {
                self.insert_message(message);
            }
        }

        Ok(())
    }

    /// Get the current decision
    pub fn deciding(&self, in_exec: SeqNo, f: usize) -> IncompleteProof {

        // fetch write set
        let write_set = PrepareSet({
            let mut buf = Vec::new();

            for stored in self.prepare_messages.iter().rev() {
                match stored.message().consensus().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
                        let digest = match stored.message().consensus().kind() {
                            ConsensusMessageKind::Prepare(d) => d.clone(),
                            _ => unreachable!(),
                        };

                        buf.push(ViewDecisionPair(
                            stored.message().consensus().view(),
                            digest,
                        ));
                    }
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            buf
        });

        // fetch quorum prepares
        let quorum_prepares = 'outer: loop {
            // NOTE: check `last_decision` comment on quorum
            let quorum = f << 1;
            let mut last_view = None;
            let mut count = 0;

            for stored in self.prepare_messages.iter().rev() {
                match stored.message().consensus().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
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
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            break 'outer None;
        };

        IncompleteProof::new(in_exec, write_set, quorum_prepares)
    }
}

impl<O> Orderable for DecidingLog<O> {
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

impl<O> Orderable for CompletedBatch<O> {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

impl<O> CompletedBatch<O> {
    pub fn batch_digest(&self) -> Digest {
        self.batch_digest
    }
    pub fn pre_prepare_ordering(&self) -> &Vec<Digest> {
        &self.pre_prepare_ordering
    }
    pub fn pre_prepare_messages(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.pre_prepare_messages
    }

    pub fn messages_to_persist(&self) -> &Vec<Digest> {
        &self.messages_to_persist
    }
    pub fn batch_meta(&self) -> &BatchMeta {
        &self.batch_meta
    }

    pub fn request_count(&self) -> usize {
        self.client_requests.len()
    }

    /// Create a proof from the completed batch
    pub fn proof(&self, quorum: Option<usize>) -> Result<Proof<O>> {
        let (leader_count, order) = (self.pre_prepare_ordering.len(), &self.pre_prepare_ordering);

        if self.pre_prepare_messages.len() != leader_count {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                              format!("Failed to create a proof, pre_prepares do not match up to the leader count {} leader count {} preprepares",
                                                      leader_count, self.prepare_messages.len()).as_str()));
        }

        let mut ordered_pre_prepares = Vec::with_capacity(leader_count);

        for digest in order {
            for i in 0..self.pre_prepare_messages.len() {
                let message = &self.pre_prepare_messages[i];

                if *message.header().digest() == *digest {
                    ordered_pre_prepares.push(self.pre_prepare_messages[i].clone());

                    break;
                }
            }
        }

        if let Some(quorum) = quorum {
            if self.prepare_messages.len() < quorum {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                  "Failed to create a proof, prepares do not match up to the 2*f+1"));
            }

            if self.commit_messages.len() < quorum {
                return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                                  "Failed to create a proof, commits do not match up to the 2*f+1"));
            }
        }

        let metadata = ProofMetadata::new(self.seq_no,
                                          self.batch_digest.clone(),
                                          order.clone());

        Ok(Proof::new(metadata,
                      ordered_pre_prepares,
                      self.prepare_messages.clone(),
                      self.commit_messages.clone()))
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

pub fn make_proof_from<O>(proof_meta: ProofMetadata, mut ongoing: OnGoingDecision<O>) -> Result<Proof<O>> {
    let OnGoingDecision {
        pre_prepare_digests,
        pre_prepare_messages,
        prepare_messages,
        commit_messages
    } = ongoing;

    let pre_prepare_messages: Vec<StoredConsensusMessage<O>> = pre_prepare_messages.into_iter()
        .map(|elem| {
            elem.unwrap()
        }).collect();

    for (digest, digest2) in zip(proof_meta.pre_prepare_ordering(), &pre_prepare_digests) {
        let digest2 = digest2.as_ref().ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog, "Failed to create a proof, pre prepare messages are missing"))?;

        if digest != digest2 {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                              "Failed to create a proof, pre prepares do not match up to the ordering"));
        }
    }

    Ok(Proof::new(proof_meta, pre_prepare_messages, prepare_messages, commit_messages))
}

impl<O> From<CompletedBatch<O>> for DecisionInformation {
    fn from(value: CompletedBatch<O>) -> Self {
        DecisionInformation::new(value.batch_digest,
                                 value.messages_to_persist,
                                 value.client_requests)
    }
}