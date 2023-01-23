use std::sync::{Arc, Mutex};
use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::message::{ConsensusMessage, StoredMessage};
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::{Request, Service};
use crate::bft::globals::ReadOnly;
use crate::bft::error::*;
use crate::bft::msg_log::persistent::PersistentLog;

/// The log for the current consensus decision
/// Stores the pre prepare that is being decided along with
/// Digests of all of the requests, digest of the entire batch and
/// the messages that should be persisted in order to consider this execution unit
/// persisted.
/// Basically some utility information about the current batch.
/// The actual consensus messages are handled by the decided log
pub struct DecidingLog<S> where S: Service {
    //Stores the request batch that is currently being processed by the system
    pre_prepare_message: Option<Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>>,

    //The digest of the entire batch that is currently being processed
    current_digest: Option<Digest>,
    //A vector that contains the digest of all requests contained in the batch that is currently being processed
    current_requests: Vec<Digest>,
    //The size of batch that is currently being processed
    current_batch_size: usize,

    //A list of digests of all consensus related messages pertaining to this
    //Consensus instance. Used to keep track of if the persistent log has saved the messages already
    //So the requests can be executed
    current_messages_to_persist: Vec<Digest>,

    batch_meta: Arc<Mutex<BatchMeta>>
}

///The type that composes a processed batch
/// Contains the pre-prepare message and the Vec of messages that contains all messages
/// to be persisted pertaining to this consensus instance
pub type ProcessedBatch<S> = CompletedBatch<S>;

/// A batch that has been decided by the consensus instance and is now ready to be delivered to the
/// Executor for execution.
/// Contains all of the necessary information for when we are using the strict persistency mode
pub struct CompletedBatch<S> where S: Service {
    // The prepare message of the batch
    pre_prepare_message: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    //The digest of the batch
    batch_digest: Digest,
    //The digests of all the requests in the batch
    request_digests: Vec<Digest>,
    //The messages that must be persisted for this consensus decision to be executable
    //This should contain the pre prepare, quorum of prepares and quorum of commits
    messages_to_persist: Vec<Digest>,

    batch_meta: BatchMeta
}

impl<S> Into<(Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
              Digest, Vec<Digest>, Vec<Digest>, BatchMeta)> for CompletedBatch<S> where S: Service {

    fn into(self) -> (Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
                      Digest, Vec<Digest>, Vec<Digest>, BatchMeta) {
        (self.pre_prepare_message, self.batch_digest, self.request_digests, self.messages_to_persist, self.batch_meta)
    }
}

impl<S> DecidingLog<S> where S: Service {

    pub fn new() -> Self {
        Self {
            pre_prepare_message: None,
            current_digest: None,
            current_requests: Vec::with_capacity(1000),
            current_batch_size: 1000,
            current_messages_to_persist: Vec::with_capacity(1000),
            batch_meta: Arc::new(Mutex::new(BatchMeta::new())),
        }
    }

    pub fn batch_meta(&self) -> &Arc<Mutex<BatchMeta>> {
        &self.batch_meta
    }

    ///Inform the log that we are now processing a new
    pub fn processing_new_batch(&mut self,
                                request_batch: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
                                digest: Digest,
                                mut batch_rq_digests: Vec<Digest>) {

        // Register this new batch as one that must be processed
        self.current_messages_to_persist.push(request_batch.header().digest().clone());

        let _prev = self.pre_prepare_message.insert(request_batch);

        self.current_requests.append(&mut batch_rq_digests);

        self.current_batch_size = batch_rq_digests.len();

        let _prev_dig = self.current_digest.insert(digest);
    }

    /// Register a message that is important to this consensus instance
    pub fn register_consensus_message(&mut self, message_digest: Digest) {
        self.current_messages_to_persist.push(message_digest)
    }

    /// Indicate that the batch is finished processing and
    /// return the relevant information for it
    pub fn finish_processing_batch(&mut self) -> Option<ProcessedBatch<S>> {
        let currently_processing = self.pre_prepare_message.take();

        if let Some(currently_processing) = currently_processing {
            let current_requests = std::mem::replace(
                &mut self.current_requests,
                Vec::with_capacity(self.current_batch_size),
            );


            self.current_batch_size = 0;

            let batch_digest = self.current_digest.take().unwrap();

            let prev_messages_to_persist = self.current_messages_to_persist.len();

            let messages_to_persist = std::mem::replace(
                &mut self.current_messages_to_persist,
                Vec::with_capacity(prev_messages_to_persist),
            );

            let new_meta = BatchMeta::new();
            let meta = std::mem::replace(&mut *self.batch_meta().lock().unwrap(), new_meta);

            Some(CompletedBatch {
                pre_prepare_message: currently_processing,
                batch_digest,
                request_digests: current_requests,
                messages_to_persist,
                batch_meta: meta
            })
        } else {
            None
        }
    }

    /// Reset the batch that is currently being processed
    pub fn reset(&mut self) {
        self.current_requests.clear();
        self.current_messages_to_persist.clear();
        self.current_digest.take();
        self.current_batch_size = 0;
        self.pre_prepare_message.take();
    }

    pub fn is_currently_processing(&self) -> bool {
        self.pre_prepare_message.is_some()
    }

    /// Get a reference to the pre prepare message of the batch that we are currently processing
    pub fn pre_prepare_message(&self) -> &Option<Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>> {
        &self.pre_prepare_message
    }

    /// The digest of the batch that is currently being processed
    pub fn current_digest(&self) -> Option<Digest> {
        self.current_digest
    }

    /// The current request list for the batch that is being processed
    pub fn current_requests(&self) -> &Vec<Digest> {
        &self.current_requests
    }

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

}