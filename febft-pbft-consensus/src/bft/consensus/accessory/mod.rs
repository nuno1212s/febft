use atlas_common::crypto::hash::Digest;
use atlas_common::ordering::SeqNo;
use atlas_communication::Node;
use atlas_execution::serialize::ApplicationData;
use atlas_core::serialize::{LogTransferMessage, StateTransferMessage};
use crate::bft::consensus::accessory::replica::ReplicaAccessory;
use crate::bft::message::ConsensusMessage;
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::msg_log::decisions::StoredConsensusMessage;
use crate::bft::PBFT;
use crate::bft::sync::view::ViewInfo;

pub mod replica;

pub enum ConsensusDecisionAccessory<D, ST, LP>
    where D: ApplicationData + 'static, ST: StateTransferMessage + 'static, LP: LogTransferMessage + 'static {
    Follower,
    Replica(ReplicaAccessory<D, ST, LP>),
}

pub trait AccessoryConsensus<D, ST, LP> where D: ApplicationData + 'static,
                                              ST: StateTransferMessage + 'static,
                                              LP: LogTransferMessage + 'static {
    /// Handle the reception of a pre-prepare message without having completed the pre prepare phase
    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: Node<PBFT<D, ST, LP>>;

    /// Handle the prepare phase having been completed
    fn handle_pre_prepare_phase_completed<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                              view: &ViewInfo,
                                              msg: StoredConsensusMessage<D::Request>,
                                              node: &NT) where NT: Node<PBFT<D, ST, LP>>;

    /// Handle a prepare message processed during the preparing phase without having
    /// reached a quorum
    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: Node<PBFT<D, ST, LP>>;

    /// Handle a prepare message processed during the prepare phase when a quorum
    /// has been achieved
    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                   view: &ViewInfo,
                                   msg: StoredConsensusMessage<D::Request>,
                                   node: &NT) where NT: Node<PBFT<D, ST, LP>>;

    /// Handle a commit message processed during the preparing phase without having
    /// reached a quorum
    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                       view: &ViewInfo,
                                       msg: StoredConsensusMessage<D::Request>,
                                       node: &NT) where NT: Node<PBFT<D, ST, LP>>;

    /// Handle a commit message processed during the prepare phase when a quorum has been reached
    fn handle_committing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                    view: &ViewInfo,
                                    msg: StoredConsensusMessage<D::Request>,
                                    node: &NT) where NT: Node<PBFT<D, ST, LP>>;
}

impl<D, ST, LP> AccessoryConsensus<D, ST, LP> for ConsensusDecisionAccessory<D, ST, LP>
    where D: ApplicationData + 'static,
          ST: StateTransferMessage + 'static,
          LP: LogTransferMessage + 'static {
    fn handle_partial_pre_prepare<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_partial_pre_prepare(deciding_log, view, msg, node);
            }
        }
    }

    fn handle_pre_prepare_phase_completed<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                              view: &ViewInfo,
                                              msg: StoredConsensusMessage<D::Request>,
                                              node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_pre_prepare_phase_completed(deciding_log, view, msg, node);
            }
        }
    }

    fn handle_preparing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                      view: &ViewInfo,
                                      msg: StoredConsensusMessage<D::Request>,
                                      node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_no_quorum(deciding_log, view, msg, node);
            }
        }
    }

    fn handle_preparing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                   view: &ViewInfo,
                                   msg: StoredConsensusMessage<D::Request>,
                                   node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_quorum(deciding_log, view, msg, node);
            }
        }
    }

    fn handle_committing_no_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                       view: &ViewInfo,
                                       msg: StoredConsensusMessage<D::Request>,
                                       node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_no_quorum(deciding_log, view, msg, node);
            }
        }
    }

    fn handle_committing_quorum<NT>(&mut self, deciding_log: &DecidingLog<D::Request>,
                                    view: &ViewInfo,
                                    msg: StoredConsensusMessage<D::Request>,
                                    node: &NT) where NT: Node<PBFT<D, ST, LP>> {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_quorum(deciding_log, view, msg, node);
            }
        }
    }
}