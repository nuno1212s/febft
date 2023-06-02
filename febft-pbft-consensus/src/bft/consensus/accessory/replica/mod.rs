use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use chrono::Utc;
use log::debug;
use atlas_common::crypto::hash::Digest;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::threadpool;
use atlas_communication::message::{NetworkMessageKind, SerializedMessage, StoredMessage, StoredSerializedNetworkMessage, WireMessage};
use atlas_communication::{Node, NodePK, serialize};
use atlas_communication::serialize::Buf;
use atlas_execution::serialize::SharedData;
use atlas_core::messages::SystemMessage;
use atlas_core::serialize::StateTransferMessage;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::msg_log::decisions::StoredConsensusMessage;
use crate::bft::PBFT;
use crate::bft::sync::view::ViewInfo;
use crate::bft::consensus::accessory::AccessoryConsensus;

pub struct ReplicaAccessory<D: SharedData + 'static, ST: StateTransferMessage + 'static> {
    speculative_commits: Arc<Mutex<BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>>>>,
}

impl<D, ST> AccessoryConsensus<D, ST> for ReplicaAccessory<D, ST>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static {

    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: Node<PBFT<D, ST>> {}

    fn handle_pre_prepare_phase_completed<NT>(&mut self,
                                              deciding_log: &DecidingLog<D::Request>,
                                              view: &ViewInfo,
                                              _msg: StoredConsensusMessage<D::Request>,
                                              node: &NT) where NT: Node<PBFT<D, ST>> {
        let my_id = node.id();
        let view_seq = view.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();

        let sign_detached = node.pk_crypto().sign_detached();
        let n = view.params().n();

        let seq = deciding_log.sequence_number();

        let speculative_commits = Arc::clone(&self.speculative_commits);

        threadpool::execute(move || {
            let message = NetworkMessageKind::from(
                SystemMessage::from_protocol_message(
                    PBFTMessage::Consensus(ConsensusMessage::new(
                        seq,
                        view_seq,
                        ConsensusMessageKind::Commit(current_digest.clone()),
                    ))));


            // serialize raw msg
            let mut buf = Vec::new();

            let digest = serialize::serialize_digest::<Vec<u8>,
                PBFT<D, ST>>(&message, &mut buf).unwrap();

            let buf = Buf::from(buf);

            for peer_id in NodeId::targets(0..n) {
                let buf_clone = buf.clone();

                // create header
                let (header, _) = WireMessage::new(
                    my_id,
                    peer_id,
                    buf_clone,
                    // NOTE: nonce not too important here,
                    // since we already contain enough random
                    // data with the unique digest of the
                    // PRE-PREPARE message
                    0,
                    Some(digest),
                    Some(sign_detached.key_pair()),
                ).into_inner();

                // store serialized header + message
                let serialized = SerializedMessage::new(message.clone(), buf.clone());

                let stored = StoredMessage::new(header, serialized);

                let mut map = speculative_commits.lock().unwrap();

                map.insert(peer_id.into(), stored);
            }
        });

        debug!("{:?} // Broadcasting prepare messages to quorum {:?}",
               my_id, seq);

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

        let targets = NodeId::targets(0..view.params().n());

        node.broadcast_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
    }

    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: Node<PBFT<D, ST>> {}

    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                   view: &ViewInfo,
                                   msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: Node<PBFT<D, ST>> {

        let node_id = node.id();

        let seq = deciding_log.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();
        let speculative_commits = self.take_speculative_commits();

        if valid_spec_commits::<D, ST>(&speculative_commits, node_id, seq, view) {
            for (_, msg) in speculative_commits.iter() {
                debug!("{:?} // Broadcasting speculative commit message {:?} (total of {} messages) to {} targets",
                     node_id, msg.message().original(), speculative_commits.len(), view.params().n());
                break;
            }

            node.broadcast_serialized(speculative_commits);
        } else {
            let message = PBFTMessage::Consensus(ConsensusMessage::new(
                seq,
                view.sequence_number(),
                ConsensusMessageKind::Commit(current_digest.clone()),
            ));

            debug!("{:?} // Broadcasting commit consensus message {:?}",
                        node_id, message);

            let targets = NodeId::targets(0..view.params().n());

            node.broadcast_signed(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
        }

        debug!("{:?} // Broadcasted commit consensus message {:?}",
                        node_id, deciding_log.sequence_number());

        deciding_log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();
    }

    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                       view: &ViewInfo,
                                       msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: Node<PBFT<D, ST>> {}

    fn handle_committing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                    view: &ViewInfo,
                                    msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: Node<PBFT<D, ST>> {}

}

impl<D, ST> ReplicaAccessory<D, ST>
    where D: SharedData + 'static,
          ST: StateTransferMessage + 'static {
    pub fn new() -> Self {
        Self {
            speculative_commits: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn take_speculative_commits(&self) -> BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::replace(&mut *map, BTreeMap::new())
    }

}


#[inline]
fn valid_spec_commits<D, ST>(
    speculative_commits: &BTreeMap<NodeId, StoredSerializedNetworkMessage<PBFT<D, ST>>>,
    node_id: NodeId,
    seq_no: SeqNo,
    view: &ViewInfo,
) -> bool
    where
        D: SharedData + 'static,
        ST: StateTransferMessage
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
        .map(|stored| match stored.message().original().deref_system() {
            SystemMessage::ProtocolMessage(protocol) => {
                match protocol.deref() {
                    PBFTMessage::Consensus(consensus) => consensus,
                    _ => { unreachable!() }
                }
            }
            _ => { unreachable!() }
        })
        .all(|commit| commit.sequence_number() == seq_no)
}