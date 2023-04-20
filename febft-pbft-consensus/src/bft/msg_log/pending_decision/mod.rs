use std::sync::{Arc, Mutex};
use intmap::IntMap;
use febft_common::collections::ConcurrentHashMap;
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_execution::app::{Request, Service};
use febft_execution::serialize::SharedData;
use febft_messages::messages::RequestMessage;
use crate::bft::msg_log::operation_key;

/// The log for requests that have been received but not yet decided by the system
pub struct PendingRequestLog<D> where D: SharedData {
    //This item will be accessed from both the client request thread and the
    //Replica request thread
    //This stores client requests that have not yet been put into a batch
    pending_requests: ConcurrentHashMap<Digest, Arc<ReadOnly<StoredMessage<RequestMessage<D::Request>>>>>,

    //This item will also be accessed from both the client request thread and the
    //replica request thread. However the client request thread will always only read
    //And the replica request thread writes and reads from it
    // Since it will almost always be accessed by the proposer, we use a mutex
    latest_op: Mutex<IntMap<SeqNo>>,

    /// Requests that have been forwarded from other replicas
    forwarded_requests: Mutex<Vec<StoredMessage<RequestMessage<D::Request>>>>,
}


impl<D> PendingRequestLog<D> where D: SharedData {
    pub(super) fn new() -> Self {
        Self {
            pending_requests: Default::default(),
            latest_op: Default::default(),
            forwarded_requests: Default::default(),
        }
    }

    /// Retrieves a batch of requests to be proposed during a view change.
    pub fn view_change_propose(&self) -> Vec<StoredMessage<RequestMessage<D::Request>>> {
        let mut pending_messages = Vec::with_capacity(self.pending_requests.len());

        self.pending_requests.iter().for_each(|multi_ref| {
            pending_messages.push((**multi_ref.value()).clone());
        });

        pending_messages
    }

    /// Register a batch of requests that have been sent to us by other replicas in STOP rqs
    pub fn register_stopped_requests(&self, rqs: &Vec<StoredMessage<RequestMessage<D::Request>>>) {

        rqs.iter().for_each(|rq| {

            let header = rq.header();

            self.pending_requests.insert(header.unique_digest(), Arc::new(ReadOnly::new(rq.clone())));
        });

    }

    /// Insert a forwarded request vec into the log
    pub fn insert_forwarded(&self, mut messages: Vec<StoredMessage<RequestMessage<D::Request>>>) {
        self.forwarded_requests.lock().unwrap().append(&mut messages);
    }

    /// Delete forwarded requests from the forwarded request pool
    pub fn take_forwarded_requests(&self, mut replacement: Option<Vec<StoredMessage<RequestMessage<D::Request>>>>)
                                   -> Option<Vec<StoredMessage<RequestMessage<D::Request>>>> {
        let original = {
            let mut guard = self.forwarded_requests.lock().unwrap();

            if guard.is_empty() {
                return None;
            }

            let replacement = replacement.take().unwrap_or_else(||
                Vec::with_capacity(guard.len()));

            std::mem::replace(&mut *guard, replacement)
        };

        Some(original)
    }

    /// Insert a block of requests into the log
    pub fn insert_latest_ops(&self, messages: &Vec<StoredMessage<RequestMessage<D::Request>>>) {
        let mut guard = self.latest_op.lock().unwrap();

        for message in messages {
            let key = operation_key::<D::Request>(message.header(), message.message());

            guard.insert(key, message.message().sequence_number());
        }
    }

    /// Filter the provided messages to only include messages that are more recent than the
    /// latest message we have received from that client
    pub fn filter_and_update_more_recent(&self, messages: Vec<StoredMessage<RequestMessage<D::Request>>>) -> Vec<StoredMessage<RequestMessage<D::Request>>> {
        let mut mutex_guard = self.latest_op.lock().unwrap();

        messages.into_iter().filter(|msg| {
            let key = operation_key::<D::Request>(msg.header(), msg.message());

            let result = if let Some(seq_no) = mutex_guard.get(key) {
                *seq_no < msg.message().sequence_number()
            } else {
                true
            };

            if result {
                mutex_guard.insert(key, msg.message().sequence_number());
            }

            result
        }).collect()
    }

    /// Have we received a more recent message from this client?
    pub fn has_received_more_recent(&self, header: &Header, message: &RequestMessage<D::Request>) -> bool {
        let key = operation_key::<D::Request>(header, message);

        let seq_no = {
            if let Some(seq_no) = self.latest_op.lock().unwrap().get(key) {
                seq_no.clone()
            } else {
                SeqNo::ZERO
            }
        };

        seq_no >= message.sequence_number()
    }

    /// Insert a client message into the log
    pub fn insert(&self, header: Header, message: RequestMessage<D::Request>) {
        if self.has_received_more_recent(&header, &message) {
            return;
        }

        let digest = header.unique_digest();
        let stored = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

        self.pending_requests.insert(digest, stored);
    }

    /// Insert a batch of requests
    pub fn insert_batch(&self, messages: Vec<(Header, RequestMessage<D::Request>)>) {
        for (header, message) in messages {
            self.insert(header, message);
        }
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
    ) -> Vec<StoredMessage<RequestMessage<D::Request>>> {
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
