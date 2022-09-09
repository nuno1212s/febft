use std::ops::Deref;
use std::sync::Arc;

use crate::bft::communication::channel::{self, ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, FwdConsensusMessage, StoredMessage,
    SystemMessage, ViewChangeMessage, ViewChangeMessageKind,
};
use crate::bft::communication::{Node, NodeId, SendNode};
use crate::bft::core::server::ViewInfo;
use crate::bft::executable::{Request, Service};
use crate::bft::globals::ReadOnly;

/// The message type of the channel
pub type ChannelMsg<S> = FollowerEvent<S>;

pub enum FollowerEvent<S: Service> {
    ReceivedConsensusMsg(
        ViewInfo,
        Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ),
    ReceivedViewChangeMsg(Arc<ReadOnly<StoredMessage<ViewChangeMessage<Request<S>>>>>),
}

/// Store information of the current followers of the quorum
/// This information will be used to calculate which replicas have to send the
/// Information to what followers
///
/// This routing is only relevant to the Preprepare requests, all other requests
/// Can be broadcast from each replica as they are very small and therefore
/// don't have any effects on performance
struct FollowersFollowing<S: Service + 'static> {
    own_id: NodeId,
    followers: Vec<NodeId>,
    send_node: SendNode<S::Data>,
    rx: ChannelSyncRx<ChannelMsg<S>>,
}

/// A handle to the follower handling thread
///
/// Allows us to pass the thread notifications on what is happening so it
/// can handle the events properly
#[derive(Clone)]
pub struct FollowerHandle<S: Service> {
    tx: ChannelSyncTx<ChannelMsg<S>>,
}

impl<S: Service + 'static> FollowersFollowing<S> {
    /// Starts the follower handling thread and returns a cloneable handle that
    /// can be used to deliver messages to it.
    pub fn init_follower_handling(id: NodeId, node: &Arc<Node<S::Data>>) -> FollowerHandle<S> {
        let (tx, rx) = channel::new_bounded_sync(1024);

        let follower_handling = Self {
            own_id: id,
            followers: Vec::new(),
            send_node: node.send_node(),
            rx,
        };

        Self::start_thread(follower_handling);

        FollowerHandle { tx }
    }

    fn start_thread(self) {
        std::thread::Builder::new()
            .name(format!(
                "Follower Handling Thread for node {:?}",
                self.own_id
            ))
            .spawn(move || {
                self.run();
            })
            .expect("Failed to launch follower handling thread!");
    }

    fn run(mut self) {
        loop {
            let message = self.rx.recv().unwrap();

            match message {
                FollowerEvent::ReceivedConsensusMsg(view, consensus_msg) => {
                    match consensus_msg.message().kind() {
                        ConsensusMessageKind::PrePrepare(_) => {
                            self.handle_preprepare_msg_rcvd(&view, consensus_msg)
                        }
                        ConsensusMessageKind::Prepare(_) => self.handle_prepare_msg(consensus_msg),
                        ConsensusMessageKind::Commit(_) => self.handle_commit_msg(consensus_msg),
                    }
                }
                FollowerEvent::ReceivedViewChangeMsg(view_change_msg) => {

                    self.handle_sync_msg(view_change_msg)

                }
            }
        }
    }

    /// Calculate which followers we have to send the messages to
    /// according to the disposition of the quorum and followers
    ///
    /// (This is only needed for the preprepare message, all others use
    /// multicast)
    fn targets(&self, view: &ViewInfo) -> Vec<NodeId> {
        //How many replicas are not the leader?
        let available_replicas = view.params().n() - 1;

        //How many followers do we have to provide for
        let followers = self.followers.len();

        //We only need one pre prepare in reality, since it is signed by the current leader
        //And can't be forged, but since we want to prevent message dropping attacks,
        //We need to use f+1 replicas
        let replicas_per_follower = view.params().f() + 1;

        //We do not want to have spaces between each id so we don't get inconsistencies
        //In how we arrange the replicas
        //In this layout, we will always get 0, 1, 2 as IDs, independently of what the leader
        //is
        let temp_id = if self.own_id > view.leader() {
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
    fn handle_preprepare_msg_rcvd(
        &mut self,
        view: &ViewInfo,
        message: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        if view.leader() == self.own_id {
            //Leaders don't send pre_prepares to followers in order to save bandwidth
            //as they already have to send the to all of the replicas
            return;
        }

        //Clone the messages here in this thread so we don't slow down the consensus thread at all
        let header = message.header().clone();

        let pre_prepare = message.message().clone();

        let message = SystemMessage::FwdConsensus(FwdConsensusMessage::new(header, pre_prepare));

        let targets = self.targets(view);

        self.send_node.broadcast(message, targets.into_iter());
    }

    /// Handle us having sent a prepare message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_prepare_msg(
        &mut self,
        prepare: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        if prepare.header().from() != self.own_id {
            //We only broadcast our own prepare messages, not other peoples
            return;
        }

        //Clone the messages here in this thread so we don't slow down the consensus thread at all
        let prepare = prepare.message().clone();

        let message = SystemMessage::Consensus(prepare);

        self.send_node
            .broadcast(message, self.followers.iter().copied());
    }

    /// Handle us having sent a commit message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_commit_msg(
        &mut self,
        commit: Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ) {
        if commit.header().from() != self.own_id {
            //Like with prepares, we only broadcast our own commit messages
            return;
        }

        let commit = commit.message().clone();

        let message = SystemMessage::Consensus(commit);

        self.send_node
            .broadcast(message, self.followers.iter().copied());
    }

    ///
    fn handle_sync_msg(&mut self, msg: Arc<ReadOnly<StoredMessage<ViewChangeMessage<Request<S>>>>>) {

        let message = msg.message();

        match message.kind() {
            ViewChangeMessageKind::Stop(_) => {}
            ViewChangeMessageKind::StopData(_) => {
                //Followers don't need these messages (only the leader of the quorum needs them)

                return;
            }
            ViewChangeMessageKind::Sync(_) => {
                //Sync msgs are weird
                //They are sort of like pre prepare messages so it would be nice for us to
                //send them like we do preprepare msgs
            }
        }

        let message = SystemMessage::ViewChange(message.clone());

        self.send_node
            .broadcast(message, self.followers.iter().copied());
    }

}

impl<S: Service> Deref for FollowerHandle<S> {
    type Target = ChannelSyncTx<ChannelMsg<S>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
