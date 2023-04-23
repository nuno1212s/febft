use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::iter::zip;
use std::sync::{Arc, Mutex};
use futures::StreamExt;
use febft_common::crypto::hash::{Context, Digest};

use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::StoredMessage;
use febft_execution::app::{Request, Service};
use febft_execution::serialize::SharedData;
use febft_metrics::benchmarks::BatchMeta;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::msg_log::decisions::{IncompleteProof, Proof, ProofMetadata, StoredConsensusMessage, ViewDecisionPair, WriteSet};
use crate::bft::msg_log::persistent::PersistentLog;
use crate::bft::sync::view::ViewInfo;


/// The type that composes a processed batch
/// Contains the pre-prepare message and the Vec of messages that contains all messages
/// to be persisted pertaining to this consensus instance
pub type ProcessedBatch<O> = CompletedBatch<O>;

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

/// The log for the current consensus decision
/// Stores the pre prepare that is being decided along with
/// Digests of all of the requests, digest of the entire batch and
/// the messages that should be persisted in order to consider this execution unit
/// persisted.
/// Basically some utility information about the current batch.
/// The actual consensus messages are handled by the decided log
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

/// The complete batch digest, the order of the batch messages,
/// the prepare messages,
/// messages to persist, the meta of the batch
pub type CompletedConsensus<O> = (Digest, Vec<Digest>, Vec<StoredConsensusMessage<O>>,
                                  Vec<Digest>, BatchMeta);

/// Information about a full batch
pub type FullBatch = ProofMetadata;

impl<O> Into<CompletedConsensus<O>> for CompletedBatch<O> {
    fn into(self) -> CompletedConsensus<O> {
        (self.batch_digest, self.pre_prepare_ordering, self.pre_prepare_messages,
         self.messages_to_persist, self.batch_meta)
    }
}

impl<O> DecidingLog<O> {
    pub fn new(node_id: NodeId) -> Self {
        Self
        {
            node_id,
            seq_no: SeqNo::ZERO,
            leader_set: vec![],
            current_digest: None,
            current_received_pre_prepares: 0,
            ongoing_decision: OnGoingDecision::initialize(4),
            received_leader_messages: Default::default(),
            request_space_slices: Default::default(),
            current_batch_size: 0,
            current_messages_to_persist: Vec::with_capacity(1000),
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
        }
    }

    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    /// Inform this log that we are now processing a new round within the given view
    /// and with the given sequence number
    pub fn processing_new_round(&mut self,
                                view: &ViewInfo,
                                seq: SeqNo) {
        self.leader_set = view.leader_set().clone();
        self.received_leader_messages.clear();
        self.seq_no = seq;

        self.request_space_slices.clear();

        for (id, section) in view.hash_space_division() {
            self.request_space_slices.insert(id.clone(), section.clone());
        }

        self.ongoing_decision.assert_length(self.leader_set.len());
    }

    ///Inform the log that we are now processing a new batch of operations
    pub fn process_pre_prepare(&mut self,
                               request_batch: Arc<ReadOnly<StoredMessage<ConsensusMessage<O>>>>,
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

    /// Register a message that is important to this consensus instance and
    /// therefore must be saved
    pub fn register_message_to_save(&mut self, message_digest: Digest) {
        self.current_messages_to_persist.push(message_digest)
    }

    /// Register a consensus message that is relevant to this consensus instance
    pub fn register_consensus_message(&mut self, message: StoredConsensusMessage<O>) {
        let digest = message.header().digest().clone();

        self.ongoing_decision.insert_message(message);

        self.register_message_to_save(digest);
    }

    /// Indicate that the batch is finished processing and
    /// return the relevant information for it
    pub fn finish_processing_batch(&mut self) -> Option<ProcessedBatch<O>> {
        let (pre_prepare_ordering, pre_prepare_messages,
            prepare_messages, commit_messages) = self.ongoing_decision.take_messages();

        let current_digest = self.current_digest?;

        let msg_to_persist_size = self.current_messages_to_persist.len();

        let messages_to_persist = std::mem::replace(
            &mut self.current_messages_to_persist,
            Vec::with_capacity(msg_to_persist_size),
        );

        let new_meta = BatchMeta::new();
        let batch_meta = std::mem::replace(&mut *self.batch_meta().lock().unwrap(), new_meta);

        //TODO: Do I even need this here since reset is always called
        self.received_leader_messages.clear();
        self.request_space_slices.clear();

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

    /// Get the current decision
    pub fn deciding(&self, f: usize) -> IncompleteProof {
        let in_exec = self.seq_no;

        self.ongoing_decision.deciding(in_exec, f)
    }

    /// Clear incomplete proofs from the log, which match the consensus
    /// with sequence number `in_exec`.
    ///
    /// If `value` is `Some(v)`, then a `PRE-PREPARE` message will be
    /// returned matching the digest `v`.
    pub fn clear_last_occurrences(
        &mut self,
        in_exec: SeqNo,
        value: Option<&Digest>,
    ) -> Option<StoredConsensusMessage<O>> {
        // let mut scratch = Vec::with_capacity(8);

        if self.seq_no == in_exec {
            if let Some(value) = value {
                let mut pre_prepare = None;

                for stored in self.ongoing_decision.pre_prepare_messages.iter() {
                    if let Some(stored) = stored.as_ref() {
                        if *stored.header().digest() == *value {
                            pre_prepare = Some(stored.clone());
                            break;
                        }
                    }
                }

                if let Some(pre_prepare) = pre_prepare {
                    return Some(pre_prepare);
                }
            }

            self.reset();

            None
        } else {
            unreachable!()
        }
    }


    /// Reset the batch that is currently being processed
    pub fn reset(&mut self) {
        self.leader_set.clear();
        self.ongoing_decision.reset();
        self.received_leader_messages.clear();
        self.request_space_slices.clear();
        self.current_digest = None;
        self.current_received_pre_prepares = 0;
        self.current_messages_to_persist.clear();
    }

    /// Are we currently processing a batch
    pub fn is_currently_processing(&self) -> bool {
        self.current_received_pre_prepares > 0
    }

    /// The digest of the batch that is currently being processed
    pub fn current_digest(&self) -> Option<Digest> {
        self.current_digest
    }

    /// The current request list for the batch that is being processed
    // pub fn current_requests(&self) -> &Vec<Digest> {
    //     &self.current_requests
    // }

    /// The size of the batch that is currently being processed
    pub fn current_batch_size(&self) -> Option<usize> {
        if self.is_currently_processing() {
            Some(self.current_batch_size)
        } else {
            None
        }
    }

    /// The current messages that should be persisted for the current consensus instance to be
    /// considered executable
    pub fn current_messages(&self) -> &Vec<Digest> {
        &self.current_messages_to_persist
    }

    pub fn pre_prepare_ordering(&self) -> &Vec<Option<Digest>> {
        &self.ongoing_decision.pre_prepare_digests
    }

    pub fn pre_prepare_messages(&self) -> &Vec<Option<StoredConsensusMessage<O>>> {
        &self.ongoing_decision.pre_prepare_messages
    }

    pub fn prepare_messages(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.ongoing_decision.prepare_messages
    }

    pub fn commit_messages(&self) -> &Vec<StoredConsensusMessage<O>> {
        &self.ongoing_decision.commit_messages
    }

    pub fn received_leader_messages(&self) -> &BTreeSet<NodeId> {
        &self.received_leader_messages
    }
}

impl<O> Orderable for DecidingLog<O> {
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
        self.request_count
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

    /// Assert that the length of the pre prepare messages and pre prepare digests match the amount
    /// of leaders in the system
    fn assert_length(&mut self, leader_count: usize) {
        if self.pre_prepare_digests.len() < leader_count {
            self.pre_prepare_digests.extend(iter::repeat(None).take(leader_count - self.pre_prepare_digests.len()));
        } else if self.pre_prepare_digests.len() > leader_count {
            self.pre_prepare_digests.truncate(leader_count);
        }

        if self.pre_prepare_messages.len() < leader_count {
            self.pre_prepare_messages.extend(iter::repeat(None).take(leader_count - self.pre_prepare_messages.len()));
        } else if self.pre_prepare_messages.len() > leader_count {
            self.pre_prepare_messages.truncate(leader_count);
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

    fn reset(&mut self) {
        self.pre_prepare_digests.clear();
        self.pre_prepare_messages.clear();
        self.prepare_messages.clear();
        self.commit_messages.clear();
    }

    /// Take the messages that are currently in this on going decision
    fn take_messages(&mut self) -> (Vec<Digest>, Vec<StoredConsensusMessage<O>>,
                                    Vec<StoredConsensusMessage<O>>,
                                    Vec<StoredConsensusMessage<O>>) {
        let pre_prepare_len = self.pre_prepare_digests.len();
        let prepare_len = self.prepare_messages.len();
        let commit_len = self.commit_messages.len();

        let digest = std::mem::replace(&mut self.pre_prepare_digests,
                                       iter::repeat(None).take(pre_prepare_len).collect())
            .into_iter().map(|opt| opt.unwrap()).collect();

        let pre_prepare_messages = std::mem::replace(&mut self.pre_prepare_messages,
                                                     iter::repeat(None).take(pre_prepare_len).collect())
            .into_iter().map(|opt| opt.unwrap()).collect();

        let prepare_messages = std::mem::replace(&mut self.prepare_messages,
                                                 Vec::with_capacity(prepare_len));

        let commit_messages = std::mem::replace(&mut self.commit_messages,
                                                Vec::with_capacity(commit_len));

        (digest, pre_prepare_messages, prepare_messages, commit_messages)
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

pub fn make_proof_from<O>(proof_meta: ProofMetadata, mut ongoing: OnGoingDecision<O>) -> Result<Proof<O>> {
    let (digests, pre_prepares,
        prepares, commit) = ongoing.take_messages();

    for (digest, digest2) in zip(proof_meta.pre_prepare_ordering(), &digests) {
        if digest != digest2 {
            return Err(Error::simple_with_msg(ErrorKind::MsgLogDecisions,
                                              "Failed to create a proof, pre prepares do not match up to the ordering"));
        }
    }

    Ok(Proof::new(proof_meta, pre_prepares, prepares, commit))
}