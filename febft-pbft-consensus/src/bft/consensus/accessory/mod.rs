use std::sync::Arc;
use atlas_communication::message::Header;

use atlas_communication::protocol_node::ProtocolNetworkNode;
use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;
use atlas_smr_application::serialize::ApplicationData;

use crate::bft::consensus::accessory::replica::ReplicaAccessory;
use crate::bft::log::deciding::WorkingDecisionLog;
use crate::bft::message::ConsensusMessage;
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::sync::view::ViewInfo;

pub mod replica;

pub enum ConsensusDecisionAccessory<D>
    where D: ApplicationData + 'static, {
    Follower,
    Replica(ReplicaAccessory<D>),
}

pub trait AccessoryConsensus<D> where D: ApplicationData + 'static, {
    /// Handle the reception of a pre-prepare message without having completed the pre prepare phase
    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                      view: &ViewInfo,
                                      header: &Header, msg: &ConsensusMessage<D::Request>,
                                      node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;

    /// Handle the prepare phase having been completed
    fn handle_pre_prepare_phase_completed<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                              view: &ViewInfo,
                                              header: &Header, msg: &ConsensusMessage<D::Request>,
                                              node: &Arc<NT>) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;

    /// Handle a prepare message processed during the preparing phase without having
    /// reached a quorum
    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                      view: &ViewInfo,
                                      header: &Header, msg: &ConsensusMessage<D::Request>,
                                      node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;

    /// Handle a prepare message processed during the prepare phase when a quorum
    /// has been achieved
    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                   view: &ViewInfo,
                                   header: &Header, msg: &ConsensusMessage<D::Request>,
                                   node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;

    /// Handle a commit message processed during the preparing phase without having
    /// reached a quorum
    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                       view: &ViewInfo,
                                       header: &Header, msg: &ConsensusMessage<D::Request>,
                                       node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;

    /// Handle a commit message processed during the prepare phase when a quorum has been reached
    fn handle_committing_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                    view: &ViewInfo,
                                    header: &Header, msg: &ConsensusMessage<D::Request>,
                                    node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static;
}

impl<D> AccessoryConsensus<D> for ConsensusDecisionAccessory<D>
    where D: ApplicationData + 'static {
    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                      view: &ViewInfo,
                                      header: &Header, msg: &ConsensusMessage<D::Request>,
                                      node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_partial_pre_prepare(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_pre_prepare_phase_completed<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                              view: &ViewInfo,
                                              header: &Header, msg: &ConsensusMessage<D::Request>,
                                              node: &Arc<NT>) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_pre_prepare_phase_completed(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                      view: &ViewInfo,
                                      header: &Header, msg: &ConsensusMessage<D::Request>,
                                      node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_no_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                   view: &ViewInfo,
                                   header: &Header, msg: &ConsensusMessage<D::Request>,
                                   node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                       view: &ViewInfo,
                                       header: &Header, msg: &ConsensusMessage<D::Request>,
                                       node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_no_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_committing_quorum<NT>(&mut self, deciding_log: &WorkingDecisionLog<D::Request>,
                                    view: &ViewInfo,
                                    header: &Header, msg: &ConsensusMessage<D::Request>,
                                    node: &NT) where NT: OrderProtocolSendNode<D, PBFTConsensus<D>> + 'static {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_quorum(deciding_log, view, header, msg, node);
            }
        }
    }
}