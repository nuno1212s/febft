use std::ops::Deref;
use std::sync::Arc;
use febft_common::channel::ChannelSyncTx;
use febft_common::globals::ReadOnly;
use febft_communication::message::StoredMessage;
use febft_execution::app::{Request, Service};
use febft_execution::serialize::SharedData;
use crate::bft::message::{ConsensusMessage, ViewChangeMessage};
use crate::bft::sync::view::ViewInfo;

/// The message type of the channel
pub type FollowerChannelMsg<D> = FollowerEvent<D>;

pub enum FollowerEvent<D: SharedData> {
    ReceivedConsensusMsg(
        ViewInfo,
        Arc<ReadOnly<StoredMessage<ConsensusMessage<D::Request>>>>,
    ),
    ReceivedViewChangeMsg(Arc<ReadOnly<StoredMessage<ViewChangeMessage<D::Request>>>>),
}

/// A handle to the follower handling thread
///
/// Allows us to pass the thread notifications on what is happening so it
/// can handle the events properly
#[derive(Clone)]
pub struct FollowerHandle<D: SharedData> {
    tx: ChannelSyncTx<FollowerChannelMsg<D>>,
}

impl<D: SharedData> FollowerHandle<D> {
    pub fn new(tx: ChannelSyncTx<FollowerChannelMsg<D>>) -> Self {
        FollowerHandle { tx }
    }
}

impl<D: SharedData> Deref for FollowerHandle<D> {
    type Target = ChannelSyncTx<FollowerChannelMsg<D>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
