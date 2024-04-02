use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use chrono::Utc;

use tracing::debug;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::serialization_helper::SerType;
use atlas_common::threadpool;
use atlas_communication::lookup_table::MessageModule;
use atlas_communication::message::{
    Header, SerializedMessage, StoredMessage, StoredSerializedMessage, WireMessage,
};
use atlas_communication::reconfiguration::NetworkInformationProvider;
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;

use crate::bft::consensus::accessory::AccessoryConsensus;
use crate::bft::log::deciding::WorkingDecisionLog;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::sync::view::ViewInfo;
use crate::bft::{SysMsg, PBFT};

pub struct ReplicaAccessory<RQ>
where
    RQ: SerType,
{
    speculative_commits: Arc<Mutex<BTreeMap<NodeId, StoredSerializedMessage<SysMsg<RQ>>>>>,
}

impl<RQ> AccessoryConsensus<RQ> for ReplicaAccessory<RQ>
where
    RQ: SerType + 'static,
{
    fn handle_partial_pre_prepare<NT>(
        &mut self,
        _deciding_log: &WorkingDecisionLog<RQ>,
        _view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        _node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
    }

    fn handle_pre_prepare_phase_completed<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        node: &Arc<NT>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>> + 'static,
    {
        let my_id = node.id();
        let view_seq = view.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();

        let key_pair = node.network_info_provider().get_key_pair().clone();
        let n = view.params().n();

        let seq = deciding_log.sequence_number();

        let speculative_commits = Arc::clone(&self.speculative_commits);

        let node_clone = node.clone();

        threadpool::execute(move || {
            let message = PBFTMessage::Consensus(ConsensusMessage::new(
                seq,
                view_seq,
                ConsensusMessageKind::Commit(current_digest),
            ));

            let (message, digest) = node_clone.serialize_digest_message(message).unwrap();

            let (message, buf) = message.into_inner();

            for peer_id in NodeId::targets(0..n) {
                let buf_clone = buf.clone();

                // create header
                let (header, _, _) = WireMessage::new(
                    my_id,
                    peer_id,
                    MessageModule::Protocol,
                    buf_clone,
                    // NOTE: nonce not too important here,
                    // since we already contain enough random
                    // data with the unique digest of the
                    // PRE-PREPARE message
                    0,
                    Some(digest),
                    Some(&*key_pair),
                )
                .into_inner();

                // store serialized header + message
                let serialized = SerializedMessage::new(message.clone(), buf.clone());

                let stored = StoredMessage::new(header, serialized);

                let mut map = speculative_commits.lock().unwrap();

                map.insert(peer_id, stored);
            }
        });

        let targets = view.quorum_members().clone();

        debug!(
            "{:?} // Broadcasting prepare messages to quorum {:?}, {:?}",
            my_id, seq, targets
        );

        // Vote for the received batch.
        // Leaders in this protocol must vote as they need to ack all
        // other request batches we have received and that are also a part of this instance
        // Also, since we can have # Leaders > f, if the leaders didn't partake in this
        // Instance we would have situations where faults joined with leaders would cause
        // Unresponsiveness
        let message = PBFTMessage::Consensus(ConsensusMessage::new(
            seq,
            view.sequence_number(),
            ConsensusMessageKind::Prepare(current_digest),
        ));

        let _ = node.broadcast_signed(message, targets.into_iter());
    }

    fn handle_preparing_no_quorum<NT>(
        &mut self,
        _deciding_log: &WorkingDecisionLog<RQ>,
        _view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        _node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
    }

    fn handle_preparing_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
        let node_id = node.id();

        let seq = deciding_log.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();
        let speculative_commits = self.take_speculative_commits();

        if valid_spec_commits::<RQ>(&speculative_commits, node_id, seq, view) {
            speculative_commits
                .iter()
                .take(1)
                .for_each(|(_id, _stored)| {
                    debug!("{:?} // Broadcasting speculative commit message (total of {} messages) to {} targets",
                     node_id, speculative_commits.len(), view.params().n());
                });

            let _ = node.broadcast_serialized(speculative_commits);
        } else {
            let message = PBFTMessage::Consensus(ConsensusMessage::new(
                seq,
                view.sequence_number(),
                ConsensusMessageKind::Commit(current_digest),
            ));

            debug!(
                "{:?} // Broadcasting commit consensus message {:?}",
                node_id, message
            );

            let targets = view.quorum_members().clone();

            let _ = node.broadcast_signed(message, targets.into_iter());
        }

        debug!(
            "{:?} // Broadcasted commit consensus message {:?}",
            node_id,
            deciding_log.sequence_number()
        );

        deciding_log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();
    }

    fn handle_committing_no_quorum<NT>(
        &mut self,
        _deciding_log: &WorkingDecisionLog<RQ>,
        _view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        _node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
    }

    fn handle_committing_quorum<NT>(
        &mut self,
        _deciding_log: &WorkingDecisionLog<RQ>,
        _view: &ViewInfo,
        _header: &Header,
        _msg: &ConsensusMessage<RQ>,
        _node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFT<RQ>>,
    {
    }
}

impl<RQ> Default for ReplicaAccessory<RQ>
where
    RQ: SerType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<RQ> ReplicaAccessory<RQ>
where
    RQ: SerType,
{
    pub fn new() -> Self {
        Self {
            speculative_commits: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn take_speculative_commits(&self) -> BTreeMap<NodeId, StoredSerializedMessage<SysMsg<RQ>>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::take(&mut *map)
    }
}

#[inline]
fn valid_spec_commits<RQ>(
    speculative_commits: &BTreeMap<NodeId, StoredSerializedMessage<SysMsg<RQ>>>,
    node_id: NodeId,
    seq_no: SeqNo,
    view: &ViewInfo,
) -> bool
where
    RQ: SerType,
{
    let len = speculative_commits.len();

    let n = view.params().n();
    if len != n {
        debug!(
            "{:?} // Failed to read speculative commits, {} vs {}",
            node_id, len, n
        );

        return false;
    }

    speculative_commits
        .values()
        .map(|msg| msg.message())
        .all(|commit| commit.original().sequence_number() == seq_no)
}
