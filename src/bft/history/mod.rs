//! Message history log and tools to make it persistent.

use std::marker::PhantomData;

use crate::bft::error::*;
use crate::bft::consensus::SeqNo;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::UpdateBatch;
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    RequestMessage,
    ConsensusMessage,
    ConsensusMessageKind,
};
use crate::bft::collections::{
    self,
    HashMap,
    OrderedMap,
};

/// Checkpoint period.
///
/// Every `PERIOD` messages, the message log is cleared,
/// and a new log checkpoint is initiated.
pub const PERIOD: u32 = 1000;

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log became full. We are waiting for the execution layer
    /// to provide the current serialized application state, so we can
    /// complete the log's garbage collection and eventually its
    /// checkpoint.
    BeginCheckpoint,
}

enum CheckpointState {
    // no checkpoint has been performed yet
    None,
    // we are calling this a partial checkpoint because we are
    // waiting for the application state from the execution layer
    Partial {
        // sequence number of the last executed request
        seq: SeqNo,
    },
    PartialWithEarlier {
        // sequence number of the last executed request
        seq: SeqNo,
        // save the earlier checkpoint, in case corruption takes place
        earlier: Checkpoint,
    },
    // application state received, the checkpoint state is finalized
    Complete(Checkpoint),
}

struct Checkpoint {
    // sequence number of the last executed request
    seq: SeqNo,
    // serialized application state
    appstate: Vec<u8>,
}

struct StoredConsensus {
    header: Header,
    message: ConsensusMessage,
}

struct StoredRequest<O> {
    header: Header,
    message: RequestMessage<O>
}

/// Represents a log of messages received by the BFT system.
pub struct Log<O, P> {
    batch_size: usize,
    pre_prepares: Vec<StoredConsensus>,
    prepares: Vec<StoredConsensus>,
    commits: Vec<StoredConsensus>,
    // TODO: view change stuff
    requests: OrderedMap<Digest, StoredRequest<O>>,
    deciding: HashMap<Digest, StoredRequest<O>>,
    checkpoint: CheckpointState,
    _marker: PhantomData<P>,
}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<O, P> Log<O, P> {
    /// Creates a new message log.
    ///
    /// The value `batch_size` represents the maximum number of
    /// client requests to queue before executing a consensus instance.
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            pre_prepares: Vec::new(),
            prepares: Vec::new(),
            commits: Vec::new(),
            deciding: collections::hash_map(),
            requests: collections::ordered_map(),
            checkpoint: CheckpointState::None,
            _marker: PhantomData,
        }
    }

/*
    /// Replaces the current `Log` with an empty one, and returns
    /// the replaced instance.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Log::new())
    }
*/

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&mut self, header: Header, message: SystemMessage<O, P>) {
        match message {
            SystemMessage::Request(message) => {
                let digest = header.digest().clone();
                let stored = StoredRequest { header, message };
                self.requests.insert(digest, stored);
            },
            SystemMessage::Consensus(message) => {
                let stored = StoredConsensus { header, message };
                match stored.message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => self.pre_prepares.push(stored),
                    ConsensusMessageKind::Prepare => self.prepares.push(stored),
                    ConsensusMessageKind::Commit => self.commits.push(stored),
                }
            },
            // rest are not handled by the log
            _ => (),
        }
    }

    /// Retrieves the next batch of requests available for proposing, if any.
    pub fn next_batch(&mut self) -> Option<Vec<Digest>> {
        let (digest, stored) = self.requests.pop_front()?;
        self.deciding.insert(digest, stored);
        if self.deciding.len() >= self.batch_size {
            Some(self.deciding
                .keys()
                .take(self.batch_size)
                .collect())
        } else {
            None
        }
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    pub fn has_request(&self, digest: &Digest) -> bool {
        match () {
            _ if self.deciding.contains_key(digest) => true,
            _ if self.requests.contains_key(digest) => true,
            _ => false,
        }
    }

    /// Finalize a batch of client requests, retrieving the payload associated with their
    /// given digests `digests`.
    ///
    /// The log may be cleared resulting from this operation. Check the enum variant of
    /// `Info`, to perform a local checkpoint when appropriate.
    pub fn finalize_batch(&mut self, digests: &[Digest]) -> Option<(Info, UpdateBatch<O>)> {
        let mut batch = UpdateBatch::new();
        for digest in digests {
            let (header, message) = self.deciding
                .remove(digest)
                .or_else(|| self.requests.remove(digest))
                .map(StoredRequest::into_inner)?;
            batch.add(header.from(), digest.clone(), message.into_inner());
        }

        // retrive the sequence number stored within the PRE-PREPARE message
        // pertaining to the current request being executed
        let last_seq_no = if self.pre_prepares.len() > 0 {
            let stored_pre_prepare = &self.pre_prepares[self.pre_prepares.len()-1].message;
            stored_pre_prepare.sequence_number()
        } else {
            // the log was cleared concurrently, retrieve
            // the seq number stored before the log was cleared
            self.curr_seq
        };
        let last_seq_no_u32 = u32::from(last_seq_no);

        let info = if last_seq_no_u32 > 0 && last_seq_no_u32 % PERIOD == 0 {
            self.begin_checkpoint(last_seq_no)
        } else {
            Info::Nil
        };

        Some((info, header, message))
    }

    fn begin_checkpoint(&mut self, seq: SeqNo) -> Info {
        let earlier = std::mem::replace(&mut self.checkpoint, CheckpointState::None);
        self.checkpoint = match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => CheckpointState::PartialWithEarlier { seq, earlier },
            // TODO: maybe return Result with the error
            _ => panic!("Invalid checkpoint state detected!"),
        };
        Info::BeginCheckpoint
    }

    /// End the state of an ongoing checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, appstate: Vec<u8>) -> Result<()> {
        match self.checkpoint {
            CheckpointState::None => Err("No checkpoint has been initiated yet").wrapped(ErrorKind::History),
            CheckpointState::Complete(_) => Err("Checkpoint already finalized").wrapped(ErrorKind::History),
            CheckpointState::Partial { ref seq } | CheckpointState::PartialWithEarlier { ref seq, .. } => {
                let seq = *seq;
                self.checkpoint = CheckpointState::Complete(Checkpoint {
                    seq,
                    appstate,
                });
                //
                // NOTE: workaround bug where when we clear the log,
                // we remove the PRE-PREPARE of an on-going request
                //
                // FIXME: the log should not be cleared until a request is over
                //
                match self.pre_prepares.pop() {
                    Some(last_pre_prepare) => {
                        self.pre_prepares.clear();

                        // store the id of the last received pre-prepare,
                        // which corresponds to the request currently being
                        // processed
                        self.curr_seq = last_pre_prepare.sequence_number();
                    },
                    None => {
                        // no stored PRE-PREPARE messages, NOOP
                    },
                }
                self.prepares.clear();
                self.commits.clear();
                Ok(())
            },
        }
    }
}

impl<O> StoredRequest<O> {
    fn into_inner(self) -> (Header, RequestMessage<O>) {
        (self.header, self.message)
    }
}
