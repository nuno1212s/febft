use atlas_common::ordering::Orderable;
use atlas_common::serialization_helper::SerMsg;
use atlas_core::messages::{ClientRqInfo, SessionBased};
use std::marker::PhantomData;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};

pub struct FollowerSynchronizer<RQ: SerMsg> {
    _phantom: PhantomData<fn() -> RQ>,
}

impl<RQ: SerMsg + SessionBased + 'static> Default for FollowerSynchronizer<RQ> {
    fn default() -> Self {
        Self::new()
    }
}

impl<RQ: SerMsg + SessionBased + 'static> FollowerSynchronizer<RQ> {
    pub fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }

    ///Watch a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn watch_request_batch(&self, pre_prepare: &ConsensusMessage<RQ>) -> Vec<ClientRqInfo> {
        let requests = match pre_prepare.kind() {
            ConsensusMessageKind::PrePrepare(req) => req,
            _ => {
                panic!()
            }
        };

        let mut digests = Vec::with_capacity(requests.len());

        //TODO: Cancel ongoing timeouts of requests that are in the batch

        for x in requests {
            let header = x.header();
            let digest = header.unique_digest();

            let seq_no = x.message().sequence_number();
            let session = x.message().session_number();

            //let request_digest = header.digest().clone();
            let client_rq_info = ClientRqInfo::new(digest, header.from(), seq_no, session);

            digests.push(client_rq_info);
        }

        //If we try to send just the array of the digests contained in the batch (instead of the actual
        //requests)

        //It's possible that, if the latency of the client to a given replica A is smaller than the
        //Latency to leader replica B + time taken to process request in B + Latency between A and B,
        //This replica does not know of the request and yet it is valid.
        //This means that that client would not be able to process requests from that replica, which could
        //break some of the quorum properties (replica A would always be faulty for that client even if it is
        //not, so we could only tolerate f-1 faults for clients that are in that situation)
        //log.insert_batched(preprepare);

        digests
    }
}
