//! A module to manage the `febft` message log.

use crate::bft::error::*;
use crate::bft::ordering::SeqNo;
use crate::bft::cst::ExecutionState;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::UpdateBatch;
use crate::bft::communication::serialize::SharedData;
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

enum CheckpointState<D> {
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
        earlier: Checkpoint<D>,
    },
    // application state received, the checkpoint state is finalized
    Complete(Checkpoint<D>),
}

struct Checkpoint<D> {
    // sequence number of the last executed request
    seq: SeqNo,
    // application state
    appstate: D::State,
}

struct StoredConsensus {
    header: Header,
    message: ConsensusMessage,
}

struct StoredRequest<D: SharedData> {
    header: Header,
    message: RequestMessage<D>
}

/// Represents a log of messages received by the BFT system.
pub struct Log<D: SharedData> {
    curr_seq: SeqNo,
    batch_size: usize,
    pre_prepares: Vec<StoredConsensus>,
    prepares: Vec<StoredConsensus>,
    commits: Vec<StoredConsensus>,
    // TODO: view change stuff
    requests: OrderedMap<Digest, StoredRequest<D>>,
    deciding: HashMap<Digest, StoredRequest<D>>,
    decided: Vec<D::Request>,
    checkpoint: CheckpointState<D>,
}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<D: SharedData> Log<D> {
    /// Creates a new message log.
    ///
    /// The value `batch_size` represents the maximum number of
    /// client requests to queue before executing a consensus instance.
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            curr_seq: SeqNo::from(0),
            pre_prepares: Vec::new(),
            prepares: Vec::new(),
            commits: Vec::new(),
            deciding: collections::hash_map_capacity(batch_size),
            // TODO: use config value instead of const
            decided: Vec::with_capacity(PERIOD as usize),
            requests: collections::ordered_map(),
            checkpoint: CheckpointState::None,
        }
    }

    pub fn snapshot(&self) -> ExecutionState<D> {
        unimplemented!()
    }

/*
    /// Replaces the current `Log` with an empty one, and returns
    /// the replaced instance.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Log::new())
    }
*/

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&mut self, header: Header, message: SystemMessage<D>) {
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
        // TODO: we may include another condition here to decide on a
        // smaller batch size, so that client request latency is lower
        if self.deciding.len() >= self.batch_size {
            Some(self.deciding
                .keys()
                .copied()
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
    pub fn finalize_batch(&mut self, digests: &[Digest]) -> Result<(Info, UpdateBatch<D>)>
    where
        D::Request: Clone,
    {
        let mut batch = UpdateBatch::new();
        for digest in digests {
            let (header, message) = self.deciding
                .remove(digest)
                .or_else(|| self.requests.remove(digest))
                .map(StoredRequest::into_inner)
                .ok_or(Error::simple(ErrorKind::Log))?;
            batch.add(header.from(), digest.clone(), message.into_inner());
        }

        // TODO: optimize batch cloning, as this can take
        // several ms if the batch size is large, and each
        // request also large
        for update in batch.as_ref() {
            self.decided.push(update.operation().clone());
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
            self.begin_checkpoint(last_seq_no)?
        } else {
            Info::Nil
        };

        Ok((info, batch))
    }

    fn begin_checkpoint(&mut self, seq: SeqNo) -> Result<Info> {
        let earlier = std::mem::replace(&mut self.checkpoint, CheckpointState::None);
        self.checkpoint = match earlier {
            CheckpointState::None => CheckpointState::Partial { seq },
            CheckpointState::Complete(earlier) => CheckpointState::PartialWithEarlier { seq, earlier },
            _ => return Err("Invalid checkpoint state detected").wrapped(ErrorKind::Log),
        };
        Ok(Info::BeginCheckpoint)
    }

    /// Check if the log has finished waiting for the execution layer
    /// to deliver the application state.
    pub fn has_complete_checkpoint(&self) -> bool {
        matches!(self.checkpoint, CheckpointState::Complete(_))
    }

    /// End the state of an on-going checkpoint.
    ///
    /// This method should only be called when `finalize_request()` reports
    /// `Info::BeginCheckpoint`, and the requested application state is received
    /// on the core server task's master channel.
    pub fn finalize_checkpoint(&mut self, appstate: Vec<u8>) -> Result<()> {
        match self.checkpoint {
            CheckpointState::None => Err("No checkpoint has been initiated yet").wrapped(ErrorKind::Log),
            CheckpointState::Complete(_) => Err("Checkpoint already finalized").wrapped(ErrorKind::Log),
            CheckpointState::Partial { ref seq } | CheckpointState::PartialWithEarlier { ref seq, .. } => {
                let seq = *seq;
                self.checkpoint = CheckpointState::Complete(Checkpoint {
                    seq,
                    appstate,
                });
                self.decided.clear();
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
                        self.curr_seq = last_pre_prepare.message.sequence_number();
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

impl<D> StoredRequest<D> {
    fn into_inner(self) -> (Header, RequestMessage<D>) {
        (self.header, self.message)
    }
}
