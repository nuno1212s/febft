//! Message history log and tools to make it persistent.

use std::marker::PhantomData;
use std::collections::VecDeque;

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

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log is full. We are ready to perform a
    /// garbage collection operation.
    Full,
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
    pre_prepares: VecDeque<StoredConsensus>,
    prepares: VecDeque<StoredConsensus>,
    commits: VecDeque<StoredConsensus>,
    // TODO: view change stuff
    requests: OrderedMap<Digest, StoredRequest<O>>,
    deciding: HashMap<Digest, StoredRequest<O>>,
    _marker: PhantomData<P>,
}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<O, P> Log<O, P> {
    /// Creates a new message log.
    pub fn new() -> Self {
        Self {
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
            deciding: collections::hash_map(),
            requests: collections::ordered_map(),
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
    pub fn insert(&mut self, header: Header, message: SystemMessage<O, P>) -> Info {
        match message {
            SystemMessage::Request(message) => {
                let digest = header.digest().clone();
                let stored = StoredRequest { header, message };
                self.requests.insert(digest, stored);
            },
            SystemMessage::Consensus(message) => {
                let stored = StoredConsensus { header, message };
                match stored.message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => self.pre_prepares.push_back(stored),
                    ConsensusMessageKind::Prepare => self.prepares.push_back(stored),
                    ConsensusMessageKind::Commit => self.commits.push_back(stored),
                }
            },
            // rest are not handled by the log
            _ => (),
        }
        Info::Nil
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

    /// Retrieves the payload associated with the given request `digest`,
    /// when it is available in this `Log`.
    pub fn request_payload(&mut self, digest: &Digest) -> Option<(Header, RequestMessage<O>)> {
        self.deciding
            .remove(digest)
            .or_else(|| self.requests.remove(digest))
            .map(StoredRequest::into_inner)
    }
}

impl<O> StoredRequest<O> {
    fn into_inner(self) -> (Header, RequestMessage<O>) {
        (self.header, self.message)
    }
}
