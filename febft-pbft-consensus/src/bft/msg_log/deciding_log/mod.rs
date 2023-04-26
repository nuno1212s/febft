use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::sync::{Arc, Mutex};
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_metrics::benchmarks::BatchMeta;
use crate::bft::message::ConsensusMessageKind;
use crate::bft::msg_log::decisions::{IncompleteProof, ProofMetadata, StoredConsensusMessage, ViewDecisionPair, WriteSet};
use crate::bft::sync::view::ViewInfo;

/// A batch that has been decided by the consensus instance and is now ready to be delivered to the
/// Executor for execution.
/// Contains all of the necessary information for when we are using the strict persistency mode
pub struct CompletedBatch<O> {
    //The digest of the batch
    batch_digest: Digest,

    // The sequence number of the batch
    seq_no: SeqNo,

    // The ordering of the pre prepares
    pre_prepare_ordering: Vec<Digest>,
    // The prepare message of the batch
    pre_prepare_messages: Vec<StoredConsensusMessage<O>>,
    // The prepare messages for this batch
    prepare_messages: Vec<StoredConsensusMessage<O>>,
    // The commit messages for this batch
    commit_messages: Vec<StoredConsensusMessage<O>>,

    //The amount of requests contained in this batch
    request_count: usize,

    //The messages that must be persisted for this consensus decision to be executable
    //This should contain the pre prepare, quorum of prepares and quorum of commits
    messages_to_persist: Vec<Digest>,

    // The metadata for this batch (mostly statistics)
    batch_meta: BatchMeta,
}

pub struct DecidingLog<O> {
    node_id: NodeId,
    seq_no: SeqNo,

    // The set of leaders that is currently in vigour for this consensus decision
    leader_set: Vec<NodeId>,

    //The digest of the entire batch that is currently being processed
    // This will only be calculated when we receive all of the requests necessary
    // As this digest requires the knowledge of all of them
    current_digest: Option<Digest>,
    // How many pre prepares have we received
    current_received_pre_prepares: usize,
    // The message log of the current ongoing decision
    ongoing_decision: OnGoingDecision<O>,
    // Received messages from these leaders
    received_leader_messages: BTreeSet<NodeId>,
    // Which hash space should each leader be responsible for
    request_space_slices: BTreeMap<NodeId, (Vec<u8>, Vec<u8>)>,

    //The size of batch that is currently being processed. Increases as we receive more pre prepares
    current_batch_size: usize,

    //A list of digests of all consensus related messages pertaining to this
    //Consensus instance. Used to keep track of if the persistent log has saved the messages already
    //So the requests can be executed
    current_messages_to_persist: Vec<Digest>,

    // Some logging information about metadata
    batch_meta: Arc<Mutex<BatchMeta>>,
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
            leader_set: view.leader_set().clone(),
            current_digest: None,
            current_received_pre_prepares: 0,
            ongoing_decision: OnGoingDecision::initialize(view.leader_set().len()),
            received_leader_messages: Default::default(),
            request_space_slices: view.hash_space_division().clone(),
            current_batch_size: 0,
            current_messages_to_persist: vec![],
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
        }
    }

    // Getter for batch_meta
    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    ///Inform the log that we are now processing a new batch of operations
    pub fn process_pre_prepare(&mut self,
                               request_batch: StoredConsensusMessage<O>,
                               digest: Digest,
                               mut batch_rq_digests: Vec<Digest>) -> Result<Option<FullBatch>> {
        let sending_leader = request_batch.header().from();

        let slice = self.request_space_slices.get(&sending_leader)
            .ok_or(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                          format!("Failed to get request space for leader {:?}. Len: {:?}",
                                                  sending_leader,
                                                  self.request_space_slices.len()).as_str()))?;

        if request_batch.header().from() != self.node_id {
            for request in &batch_rq_digests {
                if !crate::bft::sync::view::is_request_in_hash_space(request, slice) {
                    return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                                      "This batch contains requests that are not in the hash space of the leader."));
                }
            }
        }

        // Check if we have already received messages from this leader
        if !self.received_leader_messages.insert(sending_leader.clone()) {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecidingLog,
                                              "We have already received a message from that leader."));
        }

        // Get the correct index for this batch
        let leader_index = pre_prepare_index_of(&self.leader_set, &sending_leader)?;

        self.ongoing_decision.insert_pre_prepare(leader_index, request_batch.clone());

        self.current_received_pre_prepares += 1;

        self.current_batch_size += batch_rq_digests.len();

        // Register this new batch as one that must be persisted for this batch to be executed
        self.register_message_to_save(digest);

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

    /// Get the current decision
    pub fn deciding(&self, f: usize) -> IncompleteProof {
        let in_exec = self.seq_no;

        self.ongoing_decision.deciding(in_exec, f)
    }

    /// Indicate that the batch is finished processing and
    /// return the relevant information for it
    pub fn finish_processing_batch(self) -> Option<CompletedBatch<O>> {
        let OnGoingDecision {
            pre_prepare_digests,
            pre_prepare_messages,
            prepare_messages,
            commit_messages
        } = self.ongoing_decision;

        let current_digest = self.current_digest?;

        let pre_prepare_ordering = pre_prepare_digests.into_iter().map(|elem| elem?).collect();
        let pre_prepare_messages = pre_prepare_messages.into_iter().map(|elem| elem?).collect();

        let new_meta = BatchMeta::new();
        let batch_meta = std::mem::replace(&mut *self.batch_meta().lock().unwrap(), new_meta);

        Some(CompletedBatch {
            batch_digest: current_digest,
            seq_no: self.seq_no,
            pre_prepare_ordering,
            pre_prepare_messages,
            prepare_messages,
            commit_messages,
            request_count: self.current_batch_size,
            messages_to_persist,
            batch_meta,
        })
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
        match message.message().kind() {
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
    pub fn insert_stored_msg(&mut self, message: StoredConsensusMessage<O>) -> Result<()> {
        match message.message().kind() {
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
        let write_set = WriteSet({
            let mut buf = Vec::new();

            for stored in self.prepare_messages.iter().rev() {
                match stored.message().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
                        buf.push(ViewDecisionPair(
                            stored.message().view(),
                            stored.header().digest().clone(),
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
        let quorum_writes = 'outer: loop {
            // NOTE: check `last_decision` comment on quorum
            let quorum = f << 1;
            let mut last_view = None;
            let mut count = 0;

            for stored in self.prepare_messages.iter().rev() {
                match stored.message().sequence_number().cmp(&in_exec) {
                    Ordering::Equal => {
                        match last_view {
                            None => (),
                            Some(v) if stored.message().view() == v => (),
                            _ => count = 0,
                        }
                        last_view = Some(stored.message().view());
                        count += 1;
                        if count == quorum {
                            let digest = match stored.message().kind() {
                                ConsensusMessageKind::Prepare(d) => d.clone(),
                                _ => unreachable!(),
                            };
                            break 'outer Some(ViewDecisionPair(stored.message().view(), digest));
                        }
                    }
                    Ordering::Less => break,
                    // impossible, because we are executing `in_exec`
                    Ordering::Greater => unreachable!(),
                }
            }

            break 'outer None;
        };

        IncompleteProof::new(in_exec, write_set, quorum_writes)
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
