use crate::bft::communication::NodeId;
use crate::bft::core::SystemParams;
use crate::bft::ordering::{Orderable, SeqNo};
use crate::bft::error::*;
use serde::{Serialize, Deserialize};
use crate::bft::executable::Service;
use crate::bft::sync::Synchronizer;

/// This struct contains information related with an
/// active `febft` view.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct ViewInfo {
    // The seq no of the view
    seq: SeqNo,
    // The ids of the replicas that are currently a part of the quorum
    quorum: Vec<NodeId>,
    // The set of leaders
    leader_set: Vec<NodeId>,
    // The parameters of the view
    params: SystemParams,
}

impl Orderable for ViewInfo {
    /// Returns the sequence number of the current view.
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}

impl ViewInfo {
    /// Creates a new instance of `ViewInfo`.
    /// This is meant for when we are working with simple
    /// implementations
    pub fn new(seq: SeqNo, n: usize, f: usize) -> Result<Self> {
        //TODO: Make the quorum participants modifiable
        let params = SystemParams::new(n, f)?;

        let quorum_members: Vec<NodeId> = NodeId::targets_u32(0..n as u32).collect();

        let destined_leader = quorum_members[usize::from(seq) % n];

        Ok(ViewInfo {
            seq,
            quorum: quorum_members,
            leader_set: vec![destined_leader],
            params,
        })
    }

    /// Initialize a view with a given leader set
    pub fn with_leader_set(seq: SeqNo, n: usize, f: usize,
                           quorum_participants: Vec<NodeId>,
                           leader_set: Vec<NodeId>) -> Result<Self> {
        let params = SystemParams::new(n, f)?;

        for x in &leader_set {
            if !quorum_participants.contains(x) {
                return Err(Error::simple_with_msg(ErrorKind::CoreServer,
                                                  "Leader is not in the quorum participants."));
            }
        }

        Ok(ViewInfo {
            seq,
            quorum: quorum_participants,
            leader_set,
            params,
        })
    }

    /// Returns a copy of this node's `SystemParams`.
    pub fn params(&self) -> &SystemParams {
        &self.params
    }

    /// Returns a new view with the sequence number after
    /// the current view's number.
    pub fn next_view(&self) -> ViewInfo {
        self.peek(self.seq.next())
    }

    /// Returns a new view with the specified sequence number.
    pub fn peek(&self, seq: SeqNo) -> ViewInfo {
        let mut view = self.clone();
        view.seq = seq;
        view
    }

    /// Returns the primary of the current view.
    pub fn leader(&self) -> NodeId {
        self.quorum[usize::from(self.seq) % self.params.n()]
    }

    pub fn primary(&self) -> NodeId { self.leader() }

    /// The set of leaders for this view.
    pub fn leader_set(&self) -> &Vec<NodeId> {
        &self.leader_set
    }

    /// The amount of leaders currently active
    pub fn leader_count(&self) -> usize {
        self.leader_set.len()
    }

    /// The quorum members for this view
    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum
    }
}

/// Validate a given view with our own information
fn validate_view<S>(synchronizer: &Synchronizer<S>, view_info: &ViewInfo) -> Result<()> where S: Service {

    // We are currently deploying a round robin algorithm, so it's pretty simple to verify whether a given
    // view is correct

    if view_info.leader_count() > view_info.params().n() / 2 {
        return Err(Error::simple_with_msg(ErrorKind::SyncView, "View can not have more leaders than n / 2"));
    }

    //TODO: Check if we agree with the amount of leaders chosen by the system

    if !view_info.leader_set().contains(&view_info.primary()) {
        return Err(Error::simple_with_msg(ErrorKind::SyncView, "Leader set must contain the primary of the view"));
    }

    validate_leader(view_info)?;

    Ok(())
}

/// Validate a leader set to make sure primary is correctly choosing leaders
fn validate_leader(view_info: &ViewInfo) -> Result<()> {
    let leader_count = view_info.leader_count();

    let mut calculated_leaders = view_info.leader_set().clone();

    for _ in 0..leader_count {
        let calculated_leader = view_info.leader_set()[usize::from(view_info.sequence_number()) % view_info.params().n()].clone();

        if !calculated_leaders.contains(&calculated_leader) {
            return Err(Error::simple_with_msg(ErrorKind::SyncView, "Failed, leader set does not match local calculation"));
        }
    }

    Ok(())
}