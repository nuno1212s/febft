use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use log::debug;

use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_common::threadpool;
use atlas_communication::message::{NetworkMessageKind, SerializedMessage, StoredMessage, StoredSerializedProtocolMessage, WireMessage};
use atlas_communication::protocol_node::ProtocolNetworkNode;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_core::messages::SystemMessage;
use atlas_core::serialize::{LogTransferMessage, ReconfigurationProtocolMessage, ServiceMsg, StateTransferMessage};
use atlas_execution::serialize::ApplicationData;

use crate::bft::consensus::accessory::AccessoryConsensus;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::msg_log::decisions::StoredConsensusMessage;
use crate::bft::{PBFT, SysMsg};
use crate::bft::sync::view::ViewInfo;

pub struct ReplicaAccessory<D, ST, LP, RP>
    where D: ApplicationData + 'static,
          ST: StateTransferMessage + 'static,
          LP: LogTransferMessage + 'static,
          RP: ReconfigurationProtocolMessage + 'static {
    speculative_commits: Arc<Mutex<BTreeMap<NodeId, StoredSerializedProtocolMessage<SysMsg<D, ST, LP, RP>>>>>,
}

impl<D, ST, LP, RP> AccessoryConsensus<D, ST, LP, RP> for ReplicaAccessory<D, ST, LP, RP>
    where D: ApplicationData + 'static,
          ST: StateTransferMessage + 'static,
          LP: LogTransferMessage + 'static,
          RP: ReconfigurationProtocolMessage + 'static {
    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> {}

    fn handle_pre_prepare_phase_completed<NT>(&mut self,
                                              deciding_log: &DecidingLog<D::Request>,
                                              view: &ViewInfo,
                                              _msg: StoredConsensusMessage<D::Request>,
                                              node: &Arc<NT>) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> + 'static {
        let my_id = node.id();
        let view_seq = view.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();

        let key_pair = node.network_info_provider().get_key_pair().clone();
        let n = view.params().n();

        let seq = deciding_log.sequence_number();

        let speculative_commits = Arc::clone(&self.speculative_commits);

        let node_clone = node.clone();

        threadpool::execute(move || {
            let message = SystemMessage::from_protocol_message(
                PBFTMessage::Consensus(ConsensusMessage::new(
                    seq,
                    view_seq,
                    ConsensusMessageKind::Commit(current_digest.clone()),
                )));

            let (message, digest) = node_clone.serialize_digest_message(message).unwrap();

            let (message, buf) = message.into_inner();

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
                    Some(&*key_pair),
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

        let targets = view.quorum_members().clone();

        node.broadcast_signed(SystemMessage::from_protocol_message(message), targets.into_iter());
    }

    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> {}

    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                   view: &ViewInfo,
                                   msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> {
        let node_id = node.id();

        let seq = deciding_log.sequence_number();
        let current_digest = deciding_log.current_digest().unwrap();
        let speculative_commits = self.take_speculative_commits();

        if valid_spec_commits::<D, ST, LP, RP>(&speculative_commits, node_id, seq, view) {
            for (_, msg) in speculative_commits.iter() {
                debug!("{:?} // Broadcasting speculative commit message (total of {} messages) to {} targets",
                     node_id, speculative_commits.len(), view.params().n());
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

            let targets = view.quorum_members().clone();

            node.broadcast_signed(SystemMessage::from_protocol_message(message), targets.into_iter());
        }

        debug!("{:?} // Broadcasted commit consensus message {:?}",
                        node_id, deciding_log.sequence_number());

        deciding_log.batch_meta().lock().unwrap().commit_sent_time = Utc::now();
    }

    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                       view: &ViewInfo,
                                       msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> {}

    fn handle_committing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                    view: &ViewInfo,
                                    msg: StoredConsensusMessage<D::Request>, node: &NT) where NT: ProtocolNetworkNode<PBFT<D, ST, LP, RP>> {}
}

impl<D, ST, LP, RP> ReplicaAccessory<D, ST, LP, RP>
    where D: ApplicationData + 'static,
          ST: StateTransferMessage + 'static,
          LP: LogTransferMessage + 'static,
          RP: ReconfigurationProtocolMessage + 'static {
    pub fn new() -> Self {
        Self {
            speculative_commits: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn take_speculative_commits(&self) -> BTreeMap<NodeId, StoredSerializedProtocolMessage<SysMsg<D, ST, LP, RP>>> {
        let mut map = self.speculative_commits.lock().unwrap();
        std::mem::replace(&mut *map, BTreeMap::new())
    }
}


#[inline]
fn valid_spec_commits<D, ST, LP, RP>(
    speculative_commits: &BTreeMap<NodeId, StoredSerializedProtocolMessage<SysMsg<D, ST, LP, RP>>>,
    node_id: NodeId,
    seq_no: SeqNo,
    view: &ViewInfo,
) -> bool
    where
        D: ApplicationData + 'static,
        ST: StateTransferMessage + 'static,
        LP: LogTransferMessage + 'static,
        RP: ReconfigurationProtocolMessage + 'static
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
        .map(|stored| match stored.message().original() {
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