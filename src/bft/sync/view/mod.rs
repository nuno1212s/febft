use crate::bft::communication::NodeId;
use crate::bft::core::SystemParams;
use crate::bft::ordering::{Orderable, SeqNo};
use serde::{Serialize, Deserialize};
use crate::bft::error::*;

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

        let quorum_members : Vec<NodeId> = NodeId::targets_u32(0..n as u32).collect();

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
            params
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

    /// The set of leaders for this view.
    pub fn leader_set(&self) -> &Vec<NodeId> {
        &self.leader_set
    }

    /// The quorum members for this view
    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum
    }
}
