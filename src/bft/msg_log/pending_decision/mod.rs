use std::sync::{Arc, Mutex};
use intmap::IntMap;
use crate::bft::collections::ConcurrentHashMap;
use crate::bft::communication::message::{Header, RequestMessage, StoredMessage};
use crate::bft::communication::NodeId;
use crate::bft::crypto::hash::Digest;
use crate::bft::executable::Request;
use crate::bft::executable::Service;
use crate::bft::globals::ReadOnly;
use crate::bft::msg_log::operation_key;
use crate::bft::ordering::{Orderable, SeqNo};

/// The log for requests that have been received but not yet decided by the system
pub struct PendingRequestLog<S> where S: Service {
    //This item will be accessed from both the client request thread and the
    //Replica request thread
    //This stores client requests that have not yet been put into a batch
    pending_requests: ConcurrentHashMap<Digest, Arc<ReadOnly<StoredMessage<RequestMessage<Request<S>>>>>>,

    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    latest_op: Mutex<IntMap<SeqNo>>,
}


impl<S> PendingRequestLog<S> where S: Service {

    pub(super) fn new() -> Self {
        Self {
            pending_requests: Default::default(),
            latest_op: Mutex::new(IntMap::new())
        }
    }

    /// Insert a client message into the log
    pub fn insert(&self, header: Header, message: RequestMessage<Request<S>>) {
        let key = operation_key::<Request<S>>(&header, &message);

        let seq_no = {
            let latest_op_guard = self.latest_op.lock().unwrap();

            latest_op_guard.get(key).copied().unwrap_or(SeqNo::ZERO)
        };

        // avoid executing earlier requests twice
        if message.sequence_number() < seq_no {
            return;
        }

        let digest = header.unique_digest();
        let stored = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

        self.pending_requests.insert(digest, stored);
    }

    /// Insert a batch of requests
    pub fn insert_batch(&self, messages: Vec<(Header, RequestMessage<Request<S>>)>) {
        for (header, message) in messages {
            self.insert(header, message);
        }
    }

    pub fn has_received_more_recent(&self,header:& Header, message: &RequestMessage<Request<S>>) -> bool {

        let key = operation_key::<Request<S>>(header, message);

        let seq_no = {
            let latest_op_guard = self.latest_op.lock().unwrap();

            latest_op_guard.get(key).copied().unwrap_or(SeqNo::ZERO)
        };

        seq_no >= message.sequence_number()
    }

    /// Retrieves a batch of requests to be proposed during a view change.
    pub fn view_change_propose(&self) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        let mut pending_messages = Vec::with_capacity(self.pending_requests.len());

        self.pending_requests.iter().for_each(|multi_ref| {
            pending_messages.push((**multi_ref.value()).clone());
        });

        pending_messages
    }

    /// Checks if this `Log` has a particular request with the given `digest`.
    ///
    /// If this request is only contained within a batch, this will not return correctly
    pub fn has_pending_request(&self, digest: &Digest) -> bool {
        self.pending_requests.contains_key(digest)
    }

    /// Clone the requests corresponding to the provided list of hash digests.
    pub fn clone_pending_requests(
        &self,
        digests: &[Digest],
    ) -> Vec<StoredMessage<RequestMessage<Request<S>>>> {
        digests
            .iter()
            .flat_map(|d| self.pending_requests.get(d))
            .map(|rq_ref| (**rq_ref.value()).clone())
            .collect()
    }

    /// Delete requests from the pending request pool
    pub fn delete_pending_requests(&self, messages: &[Digest]) {

        for digest in messages {
            self.pending_requests.remove(digest);
        }
    }
}
