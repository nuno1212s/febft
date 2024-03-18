use atlas_common::serialization_helper::SerType;
use atlas_communication::message::Header;
use std::sync::Arc;

use atlas_core::ordering_protocol::networking::OrderProtocolSendNode;

use crate::bft::consensus::accessory::replica::ReplicaAccessory;
use crate::bft::log::deciding::WorkingDecisionLog;
use crate::bft::message::serialize::PBFTConsensus;
use crate::bft::message::ConsensusMessage;
use crate::bft::sync::view::ViewInfo;

pub mod replica;

pub enum ConsensusDecisionAccessory<RQ>
where
    RQ: SerType,
{
    Follower,
    Replica(ReplicaAccessory<RQ>),
}

pub trait AccessoryConsensus<RQ>
where
    RQ: SerType + 'static,
{
    /// Handle the reception of a pre-prepare message without having completed the pre prepare phase
    fn handle_partial_pre_prepare<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;

    /// Handle the prepare phase having been completed
    fn handle_pre_prepare_phase_completed<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &Arc<NT>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;

    /// Handle a prepare message processed during the preparing phase without having
    /// reached a quorum
    fn handle_preparing_no_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;

    /// Handle a prepare message processed during the prepare phase when a quorum
    /// has been achieved
    fn handle_preparing_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;

    /// Handle a commit message processed during the preparing phase without having
    /// reached a quorum
    fn handle_committing_no_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;

    /// Handle a commit message processed during the prepare phase when a quorum has been reached
    fn handle_committing_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static;
}

impl<RQ> AccessoryConsensus<RQ> for ConsensusDecisionAccessory<RQ>
where
    RQ: SerType + 'static,
{
    fn handle_partial_pre_prepare<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_partial_pre_prepare(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_pre_prepare_phase_completed<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &Arc<NT>,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_pre_prepare_phase_completed(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_preparing_no_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_no_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_preparing_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_preparing_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_committing_no_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_no_quorum(deciding_log, view, header, msg, node);
            }
        }
    }

    fn handle_committing_quorum<NT>(
        &mut self,
        deciding_log: &WorkingDecisionLog<RQ>,
        view: &ViewInfo,
        header: &Header,
        msg: &ConsensusMessage<RQ>,
        node: &NT,
    ) where
        NT: OrderProtocolSendNode<RQ, PBFTConsensus<RQ>> + 'static,
    {
        match self {
            ConsensusDecisionAccessory::Follower => {}
            ConsensusDecisionAccessory::Replica(rep) => {
                rep.handle_committing_quorum(deciding_log, view, header, msg, node);
            }
        }
    }
}
