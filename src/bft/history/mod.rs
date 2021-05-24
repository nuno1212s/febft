//! Message history log and tools to make it persistent.

use std::marker::PhantomData;

use crate::bft::consensus::SeqNo;
use crate::bft::crypto::hash::Digest;
use crate::bft::communication::NodeId;
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    RequestMessage,
    ConsensusMessage,
    CheckpointMessage,
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
pub const PERIOD: u32 = 1000 + 1;

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log became full and thus has been garbage collected.
    /// The sequence number of the last executed request is
    /// retrieved, to broadcast a new `CheckpointMessage`.
    Gc(SeqNo),
}

struct StoredCheckpoint {
    header: Header,
    message: CheckpointMessage,
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
    checkpoints: HashMap<NodeId, StoredCheckpoint>,
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
            checkpoints: collections::hash_map(),
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
            SystemMessage::Checkpoint(message) => {
                let stored = StoredCheckpoint { header, message };
                // override last checkpoint
                self.checkpoints.insert(stored.header.from(), stored);
            },
            // rest are not handled by the log
            _ => (),
        }
    }

    /// Retrieves the next request available for proposing, if any.
    pub fn next_request(&mut self) -> Option<Digest> {
        let (digest, stored) = self.requests.pop_front()?;
        self.deciding.insert(digest, stored);
        Some(digest)
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    pub fn has_request(&self, digest: &Digest) -> bool {
        match () {
            _ if self.deciding.contains_key(digest) => true,
            _ if self.requests.contains_key(digest) => true,
            _ => false,
        }
    }

    /// Finalize a client request, retrieving the payload associated with its given
    /// digest `digest`.
    ///
    /// The log may be cleared resulting from this operation. Check the enum variant of
    /// `Info`, to broadcast a `CheckpointMessage` when adequate.
    pub fn finalize_request(&mut self, digest: &Digest) -> Option<(Info, Header, RequestMessage<O>)> {
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

        // add one so we don't start a checkpoint if the sequence number
        // is zero (e.g. during a system bootstrap)
        let last_seq_no_u32 = u32::from(last_seq_no) + 1;

        let info = if last_seq_no_u32 % PERIOD == 0 {
            // clear the log
            self.pre_prepares.clear();
            self.prepares.clear();
            self.commits.clear();

            Info::Gc(last_seq_no)
        } else {
            Info::Nil
        };

        Some((info, header, message))
    }
}

impl<O> StoredRequest<O> {
    fn into_inner(self) -> (Header, RequestMessage<O>) {
        (self.header, self.message)
    }
}
