use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::{ConsensusMessage, FwdConsensusMessage, Header, Message, SystemMessage};
use crate::bft::communication::{NodeId, SendNode};
use crate::bft::core::server::ViewInfo;
use crate::bft::executable::{Request, Service};

///The message type of the channel
pub type ChannelMsg<S: Service> = ConsensusMessage<Request<S>>;

///Store information of the current followers of the quorum
/// This information will be used to calculate which replicas have to send the
/// Information to what followers
///
/// This routing is only relevant to the Preprepare requests, all other requests
/// Can be broadcast from each replica as they are very small and therefore
/// don't have any effects on performance
struct FollowersFollowing<S: Service> {
    own_id: NodeId,
    followers: Vec<NodeId>,
    send_node: SendNode<S::Data>,
    rx: ChannelSyncRx<ChannelMsg<S>>
}

struct FollowerHandle<S: Service> {
    tx: ChannelSyncTx<ChannelMsg<S>>,
}

impl<S: Service> FollowersFollowing<S> {

    ///Handle when we have received a preprepare message
    fn handle_preprepare_msg_rcvd(&mut self, view: ViewInfo, header: Header, pre_prepare: ConsensusMessage<Request<S>>) {
        if view.leader() == self.own_id {
            //Leaders don't send pre_prepares to followers in order to save bandwidth
            return;
        }

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

            let first_follower = temp_id.id() % self.followers.len();

            let last_follower = first_follower + followers_for_replica;

            let mut targetted_followers = Vec::with_capacity((last_follower - first_follower) as usize);

            for i in first_follower..=last_follower {
                targetted_followers.push(self.followers[i as usize]);
            }

            //TODO:
            // Forward the message
            let message = SystemMessage::FwdConsensus(FwdConsensusMessage::new(header, pre_prepare));

            self.send_node.broadcast(message, targetted_followers.iter());
        } else {
            //TODO: How to handle layouts when there are more replicas than followers?
        }
    }

    ///Handle us having sent a prepare message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_prepare_msg(&mut self, prepare: ConsensusMessage<Request<S>>) {
        let message = SystemMessage::Consensus(prepare);

        self.send_node.broadcast(message, self.followers.iter());
    }

    ///Handle us having sent a commit message (notice how pre prepare are handled on reception
    /// and prepare/commit are handled on sending, this is because we don't want the leader
    /// to have to send the pre prepare to all followers but since these messages are very small,
    /// it's fine for all replicas to broadcast it to followers)
    fn handle_commit_msg(&mut self, commit: ConsensusMessage<Request<S>>) {
        let message = SystemMessage::Consensus(commit);

        self.send_node.broadcast(message, self.followers.iter());
    }
}