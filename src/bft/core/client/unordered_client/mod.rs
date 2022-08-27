use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::bft::communication::message::{ReplyMessage, RequestMessage, SystemMessage};
use crate::bft::communication::{Node, NodeConnector};
use crate::bft::{
    communication::{serialize::SharedData, NodeId},
    ordering::SeqNo,
};

use super::{
    get_ready, get_ready_callback, get_request_key, Callback, Client, ClientData, ClientRequestFut,
    ClientType, ReplicaVotes,
};

pub enum UnorderedClientMode {
    ///BFT Client mode
    /// In this mode, clients ask at least f+1 followers the same request and will require us to
    BFT,

    BestEffort,
}

///Data relative to the follower mode of the client
pub(super) struct FollowerData {
    //The mode which the client is currently using
    unordered_request_mode: UnorderedClientMode,
    //The followers that we are currently connected to
    connected_followers: Mutex<BTreeSet<NodeId>>,
    //The followers we are currently attempting to connect to
    connecting_followers: Mutex<BTreeSet<NodeId>>,
}

impl FollowerData {
    pub fn empty(mode: UnorderedClientMode) -> Self {
        FollowerData {
            unordered_request_mode: mode,
            connected_followers: Mutex::new(Default::default()),
            connecting_followers: Mutex::new(Default::default()),
        }
    }
}
struct Unordered;

impl<D> ClientType<D> for Unordered
where
    D: SharedData + 'static,
{
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: <D as SharedData>::Request,
    ) -> SystemMessage<<D as SharedData>::State, <D as SharedData>::Request, <D as SharedData>::Reply>
    {
        SystemMessage::UnOrderedRequest(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item = NodeId>;

    fn init_targets(client: &Client<D>) -> (Self::Iter, usize) {
        //TODO: Atm we are using all followers, we should choose a small number of them and
        // Send it to those. (Maybe the ones that are closes? TBD)
        let connected_followers: Vec<NodeId> = client
            .data
            .follower_data
            .connected_followers
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();

        let count = connected_followers.len();

        (connected_followers.into_iter(), count)
    }

    fn needed_responses(client: &Client<D>) -> usize {
        let f = client.params.f();

        match client.data.follower_data.unordered_request_mode {
            UnorderedClientMode::BFT => f + 1,
            UnorderedClientMode::BestEffort => 1,
        }
    }
}
