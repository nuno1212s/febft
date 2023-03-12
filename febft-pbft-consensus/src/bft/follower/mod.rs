use std::ops::Deref;
use std::sync::Arc;
use febft_common::channel::ChannelSyncTx;
use febft_common::globals::ReadOnly;
use febft_communication::message::StoredMessage;
use febft_execution::app::{Request, Service};
use crate::bft::message::{ConsensusMessage, ViewChangeMessage};
use crate::bft::sync::view::ViewInfo;

/// The message type of the channel
pub type FollowerChannelMsg<S> = FollowerEvent<S>;

pub enum FollowerEvent<S: Service> {
    ReceivedConsensusMsg(
        ViewInfo,
        Arc<ReadOnly<StoredMessage<ConsensusMessage<Request<S>>>>>,
    ),
    ReceivedViewChangeMsg(Arc<ReadOnly<StoredMessage<ViewChangeMessage<Request<S>>>>>),
}

/// A handle to the follower handling thread
///
/// Allows us to pass the thread notifications on what is happening so it
/// can handle the events properly
#[derive(Clone)]
pub struct FollowerHandle<S: Service> {
    tx: ChannelSyncTx<FollowerChannelMsg<S>>,
}

impl<S: Service> FollowerHandle<S> {
    pub fn new(tx: ChannelSyncTx<FollowerChannelMsg<S>>) -> Self {
        FollowerHandle { tx }
    }
}

impl<S: Service> Deref for FollowerHandle<S> {
    type Target = ChannelSyncTx<FollowerChannelMsg<S>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}
