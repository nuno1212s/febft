use std::ops::Deref;
use std::sync::Arc;
use febft_common::channel::ChannelSyncTx;
use febft_common::globals::ReadOnly;
use febft_communication::message::StoredMessage;
use febft_execution::executable::{Request, Service};
use crate::messages::{ConsensusMessage, ViewChangeMessage};
use crate::sync::view::ViewInfo;

/// The message type of the channel
pub type ChannelMsg<S> = FollowerEvent<S>;

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
    tx: ChannelSyncTx<ChannelMsg<S>>,
}

impl<S: Service> FollowerHandle<S> {
    pub fn new(tx: ChannelSyncTx<ChannelMsg<S>>) -> Self {
        Self { tx }
    }
}

impl<S: Service> Deref for FollowerHandle<S> {
    type Target = ChannelSyncTx<ChannelMsg<S>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}