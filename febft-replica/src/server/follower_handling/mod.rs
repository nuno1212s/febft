use std::ops::Deref;
use std::sync::Arc;
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_communication::message::{NetworkMessageKind, StoredMessage, System};
use febft_communication::{Node};
use febft_execution::app::{Request, Service};
use febft_execution::serialize::SharedData;
use febft_messages::followers::{FollowerChannelMsg, FollowerEvent, FollowerHandle};
use febft_messages::messages::{Protocol, SystemMessage};
use febft_messages::serialize::{OrderingProtocolMessage, ServiceMsg, StateTransferMessage, NetworkView};

/// Store information of the current followers of the quorum
/// This information will be used to calculate which replicas have to send the
/// Information to what followers
///
/// This routing is only relevant to the Preprepare requests, all other requests
/// Can be broadcast from each replica as they are very small and therefore
/// don't have any effects on performance
struct FollowersFollowing<OP: OrderingProtocolMessage, NT> {
    own_id: NodeId,
    followers: Vec<NodeId>,
    send_node: Arc<NT>,
    rx: ChannelSyncRx<FollowerChannelMsg<OP>>,
}

impl<OP, NT> FollowersFollowing<OP, NT> where
    OP: OrderingProtocolMessage + 'static,
    NT: Send + Sync + 'static {
    /// Starts the follower handling thread and returns a cloneable handle that
    /// can be used to deliver messages to it.
    pub fn init_follower_handling<D, ST>(id: NodeId, node: &Arc<NT>) -> FollowerHandle<OP>
        where D: SharedData + 'static,
              ST: StateTransferMessage + 'static,
              NT: Node<ServiceMsg<D, OP, ST>>  {
        let (tx, rx) = channel::new_bounded_sync(1024);

        let follower_handling = Self {
            own_id: id,
            followers: Vec::new(),
            send_node: Arc::clone(node),
            rx,
        };

        Self::start_thread::<D, ST>(follower_handling);

        FollowerHandle::new(tx)
    }

    fn start_thread<D, ST>(self) where D: SharedData + 'static,
                                       ST: StateTransferMessage + 'static,
                                       NT: Node<ServiceMsg<D, OP, ST>> {
        std::thread::Builder::new()
            .name(format!(
                "Follower Handling Thread for node {:?}",
                self.own_id
            ))
            .spawn(move || {
                self.run::<D, ST>();
            })
            .expect("Failed to launch follower handling thread!");
    }

    fn run<D, ST>(mut self)
        where D: SharedData + 'static,
              ST: StateTransferMessage + 'static,
              NT: Node<ServiceMsg<D, OP, ST>> {
        loop {
            let message = self.rx.recv().unwrap();

            match message {
                FollowerEvent::ReceivedConsensusMsg(view, consensus_msg) => {
                    todo!()
                }
                FollowerEvent::ReceivedViewChangeMsg(view_change_msg) => {
                    self.handle_sync_msg::<D, ST>(view_change_msg)
                }
            }
        }
    }

    /// Calculate which followers we have to send the messages to
    /// according to the disposition of the quorum and followers
    ///
    /// (This is only needed for the preprepare message, all others use
    /// multicast)
    fn targets(&self, view: &OP::ViewInfo) -> Vec<NodeId> {
        //How many replicas are not the leader?
        let available_replicas = view.n() - 1;

        //How many followers do we have to provide for
        let followers = self.followers.len();

        //We only need one pre prepare in reality, since it is signed by the current leader
        //And can't be forged, but since we want to prevent message dropping attacks,
        //We need to use f+1 replicas
        let replicas_per_follower = view.f() + 1;

        //We do not want to have spaces between each id so we don't get inconsistencies
        //In how we arrange the replicas
        //In this layout, we will always get 0, 1, 2 as IDs, independently of what the leader
        //is
        let temp_id = if self.own_id > view.primary() {
            NodeId::from(self.own_id.id() - 1)
        } else {
            self.own_id
        };

        if followers >= available_replicas {
            //How many followers do we have to forward the message to
            //Taking all of this into account
            let followers_for_replica = (replicas_per_follower * followers) / available_replicas;

            let first_follower = temp_id.id() % (self.followers.len() as u32);

            let last_follower = first_follower + followers_for_replica as u32;

            let mut targetted_followers =
                Vec::with_capacity((last_follower - first_follower) as usize);

            for i in first_follower..=last_follower {
                targetted_followers.push(self.followers[i as usize]);
            }

            targetted_followers
        } else {
            //TODO: How to handle layouts when there are more replicas than followers?
            todo!()
        }
    }

    /// Handle when we have received a preprepare message
    fn handle_preprepare_msg_rcvd<D, ST>(
        &mut self,
        view: &OP::ViewInfo,
        message: Arc<ReadOnly<StoredMessage<Protocol<OP::ProtocolMessage>>>>,
    ) where D: SharedData + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> {
        if view.primary() == self.own_id {
            //Leaders don't send pre_prepares to followers in order to save bandwidth
            //as they already have to send the to all of the replicas
            return;
        }

        //Clone the messages here in this thread so we don't slow down the consensus thread at all
        let header = message.header().clone();

        let pre_prepare = message.message().clone();

        let message = SystemMessage::from_fwd_protocol_message(StoredMessage::new(header, pre_prepare));

        let targets = self.targets(view);

        self.send_node.broadcast(NetworkMessageKind::from(message), targets.into_iter()).unwrap();
    }

    /// Handle us having sent a prepare message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_prepare_msg<D, ST>(
        &mut self,
        prepare: Arc<ReadOnly<StoredMessage<Protocol<OP::ProtocolMessage>>>>,
    ) where D: SharedData + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> {
        if prepare.header().from() != self.own_id {
            //We only broadcast our own prepare messages, not other peoples
            return;
        }

        let header = prepare.header().clone();

        //Clone the messages here in this thread so we don't slow down the consensus thread at all
        let prepare = prepare.message().clone();

        let message = SystemMessage::from_fwd_protocol_message(StoredMessage::new(header, prepare));

        self.send_node
            .broadcast(NetworkMessageKind::from(message), self.followers.iter().copied()).unwrap();
    }

    /// Handle us having sent a commit message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_commit_msg<D, ST>(
        &mut self,
        commit: Arc<ReadOnly<StoredMessage<Protocol<OP::ProtocolMessage>>>>,
    ) where D: SharedData + 'static,
            ST: StateTransferMessage + 'static,
            NT: Node<ServiceMsg<D, OP, ST>> {
        if commit.header().from() != self.own_id {
            //Like with prepares, we only broadcast our own commit messages
            return;
        }

        let header = commit.header().clone();
        let commit = commit.message().clone();

        let message = SystemMessage::from_fwd_protocol_message(StoredMessage::new(header, commit));

        self.send_node
            .broadcast(NetworkMessageKind::from(message), self.followers.iter().copied()).unwrap();
    }

    ///
    fn handle_sync_msg<D, ST>(&mut self, msg: Arc<ReadOnly<StoredMessage<Protocol<OP::ProtocolMessage>>>>)
        where D: SharedData + 'static,
              ST: StateTransferMessage + 'static,
              NT: Node<ServiceMsg<D, OP, ST>> {
        let header = msg.header().clone();
        let message = msg.message().clone();

        self.send_node
            .broadcast(NetworkMessageKind::from(SystemMessage::from_fwd_protocol_message(StoredMessage::new(header, message))), self.followers.iter().copied()).unwrap();
    }
}