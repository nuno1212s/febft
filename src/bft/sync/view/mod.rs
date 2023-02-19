use std::collections::BTreeMap;
use std::iter;
use std::ops::{Add, Sub};
use num_bigint::BigUint;
use num_bigint::ToBigUint;
use num_traits::identities::Zero;
use crate::bft::communication::NodeId;
use crate::bft::core::SystemParams;
use crate::bft::ordering::{Orderable, SeqNo};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use crate::bft::crypto::hash::Digest;
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
    //TODO: Do we need this? Higher cost of cloning
    leader_hash_space_division: BTreeMap<NodeId, (Vec<u8>, Vec<u8>)>,
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

        let leader_set = vec![destined_leader];

        let division = calculate_hash_space_division(&leader_set);

        Ok(ViewInfo {
            seq,
            quorum: quorum_members,
            leader_set,
            leader_hash_space_division: division,
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

        let division = calculate_hash_space_division(&leader_set);

        Ok(ViewInfo {
            seq,
            quorum: quorum_participants,
            leader_set,
            leader_hash_space_division: division,
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

    /// The set of leaders for this view.
    pub fn leader_set(&self) -> &Vec<NodeId> {
        &self.leader_set
    }

    /// The quorum members for this view
    pub fn quorum_members(&self) -> &Vec<NodeId> {
        &self.quorum
    }

    // Get the division of hash spaces for this view
    pub fn hash_space_division(&self) -> &BTreeMap<NodeId, (Vec<u8>, Vec<u8>)> {
        &self.leader_hash_space_division
    }

}

/// Get the division of hash spaces for a given leader_set
/// Divides the hash space for client requests across the various leaders.
/// Each leader should get a similar slice of the pie.
fn calculate_hash_space_division(leader_set: &Vec<NodeId>) -> BTreeMap<NodeId, (Vec<u8>, Vec<u8>)> {
    let digest_len_bits = Digest::LENGTH * 8;

    let slices = divide_hash_space(digest_len_bits, leader_set.len());

    let mut slice_for_leaders = BTreeMap::new();

    slices.into_iter().enumerate().for_each(|(leader_id, slice)| {

        let leader = leader_set[leader_id].clone();

        slice_for_leaders.insert(leader, slice);
    });

    slice_for_leaders
}

/// Check if a given requests is within a given hash space
pub fn is_request_in_hash_space(rq: &Digest, hash_space: &(Vec<u8>, Vec<u8>)) -> bool {

    let start = &hash_space.0;
    let end = &hash_space.1;

    let start_bi = BigUint::from_bytes_be(start);
    let end_bi = BigUint::from_bytes_be(end);

    let rq_digest = BigUint::from_bytes_be(rq.as_ref());

    start_bi <= rq_digest && rq_digest <= end_bi
}

/// Division of the hash space
/// The intervals returned here should be interpreted as [`[a, b], [c, d], ..`]
fn divide_hash_space(size: usize, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    // all the numbers that we are going to be working with will then be used as powers in 2^x

    // How large is each slice
    let slice_size_bits = size / count;

    // How many bytes does it take to represent a digest in this hash space
    let size_bytes = size / u8::BITS as usize;

    let mut start = BigUint::zero();

    let last_hash : Vec<u8> = iter::repeat(0xFF).take(size_bytes).collect();

    //Byte order does not matter, it's all 1s
    let end = BigUint::from_bytes_be(&last_hash[..]);

    //This is safe since the actual number of bits is very low (at most like the value 256, so a u32 can
    // Handle that very easily for the foreseeable future 2^33 hashes?)
    let slice_size_bits_u32 = slice_size_bits as u32;

    // We want to have an interval [a,b], so b cannot be the same as the next groups a or we would get issues
    // Instead, we just assign the last one the rest of the space
    let increment = 2.to_biguint().unwrap().pow(slice_size_bits_u32).sub(1.to_biguint().unwrap());

    // The final slices for each member
    let mut slices = Vec::with_capacity(count);

    // Get the slices
    for i in 1..=count {

        let slice_start = start.to_bytes_be();

        start = start.add(&increment);

        let slice_end = if i == count {
            // Assign the last slice the rest of the space
            end.to_bytes_be()
        } else {
            start.to_bytes_be()
        };

        // Move the start to the start of the next interval
        start = start.add(1.to_biguint().unwrap());

        slices.push((slice_start, slice_end))
    }

    slices
}
