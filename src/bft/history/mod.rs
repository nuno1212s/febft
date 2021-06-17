//! Message history log and tools to make it persistent.

use std::marker::PhantomData;

use crate::bft::error::*;
use crate::bft::consensus::SeqNo;
use crate::bft::crypto::hash::Digest;
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
    pub fn new() -> Self {
        Self {
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
    pub fn next_batch(&mut self, batch_size: usize) -> Option<Vec<Digest>> {
        //if self.deciding.len() == batch_size {
        //    return None;
        //}
        let (digest, stored) = self.requests.pop_front()?;
        self.deciding.insert(digest, stored);
        if self.deciding.len() == batch_size {
            Some(self.deciding.keys().collect())
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
    pub fn finalize_batch(&mut self, digests: &[Digest]) -> Option<(Info, Header, RequestMessage<O>)> {
        // TODO: finish this

        let (header, message) = self.deciding
            .remove(digest)
            .or_else(|| self.requests.remove(digest))
            .map(StoredRequest::into_inner)?;

        // assert the digest `digest` matches the last one in the log
        //
        // no need to worry about index out of bounds stuff, because
        // the statement above guarantees we have received the corresponding
        // PRE-PREPARE for the request with digest `digest`, namely it should be
        // the last one in the `Vec`
        let stored_pre_prepare = &self.pre_prepares[self.pre_prepares.len()-1].message;

        match stored_pre_prepare.kind() {
            ConsensusMessageKind::PrePrepare(ref other) if other == digest => {
                // nothing to do! we are set
            },
            // NOTE: this should be an impossible scenario, but either way
            // it gets catched by the `unreachable!()` expression at
            // `febft::bft::core::server`, generating a runtime panic
            _ => return None,
        }

        // retrive the sequence number of the stored PRE-PREPARE message,
        // meanwhile dropping its reference in case we need to clear the log
        let last_seq_no = {
            let seq_no = stored_pre_prepare.sequence_number();
            drop(stored_pre_prepare);
            seq_no
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
                // NOTE: workaround bug where we clear the log,
                // deleting the digest of an on-going request; the log
                // entries are synchronized between all nodes, thanks
                // to the consensus layer
                //
                // FIXME: find a better solution for this
                //
                match self.pre_prepares.pop() {
                    Some(last_pre_prepare) => {
                        // store the last received pre-prepare, which
                        // corresponds to the request currently being
                        // processed
                        self.pre_prepares.clear();
                        self.pre_prepares.push(last_pre_prepare);
                    },
                    None => {
                        self.pre_prepares.clear();
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
