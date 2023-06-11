use std::{marker::PhantomData, sync::Arc};
use atlas_common::crypto::hash::Digest;
use atlas_common::ordering::Orderable;
use atlas_communication::message::StoredMessage;
use atlas_core::messages::ClientRqInfo;
use atlas_execution::app::{Request, Service};
use atlas_execution::serialize::SharedData;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::msg_log::decisions::StoredConsensusMessage;

pub struct FollowerSynchronizer<D: SharedData> {
    _phantom: PhantomData<D>
}

impl<D: SharedData + 'static> FollowerSynchronizer<D> {

    pub fn new() -> Self {
        Self { _phantom: Default::default() }
    }

    ///Watch a batch of requests received from a Pre prepare message sent by the leader
    /// In reality we won't watch, more like the contrary, since the requests were already
    /// proposed, they won't timeout
    pub fn watch_request_batch(
        &self,
        pre_prepare: &StoredConsensusMessage<D::Request>,
    ) -> Vec<ClientRqInfo> {

        let requests = match pre_prepare.message().consensus().kind() {
            ConsensusMessageKind::PrePrepare(req) => {req},
            _ => {panic!()}
        };

        let mut digests = Vec::with_capacity(requests.len());

        //TODO: Cancel ongoing timeouts of requests that are in the batch

        for x in requests {
            let header = x.header();
            let digest = header.unique_digest();

            let seq_no = x.message().sequence_number();
            let session = x.message().session_id();

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