use std::ops::Deref;
use std::sync::Arc;
use febft_pbft_consensus::bft::PBFT;
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};
use febft_common::node_id::NodeId;
use febft_communication::{Node};
use febft_communication::message::{NetworkMessageKind, System};
use febft_execution::app::{BatchReplies, Reply, Service};
use febft_messages::messages::{ReplyMessage, SystemMessage};

type RepliesType<S> = BatchReplies<S>;

///Dedicated thread to reply to clients
/// This is currently not being used (we are currently using the thread pool)
pub struct Replier<S, NT> where S: Service + 'static, NT: Node<PBFT<S::Data>> + 'static {
    node_id: NodeId,
    channel: ChannelSyncRx<RepliesType<Reply<S>>>,
    send_node: Arc<NT>,
}

pub struct ReplyHandle<S> where S: Service {
    inner: ChannelSyncTx<RepliesType<Reply<S>>>,
}

const REPLY_CHANNEL_SIZE: usize = 1024;

impl<S> ReplyHandle<S> where S: Service {
    pub fn new(replier: ChannelSyncTx<RepliesType<Reply<S>>>) -> Self {
        Self {
            inner: replier
        }
    }
}

impl<S> Deref for ReplyHandle<S> where S: Service {
    type Target = ChannelSyncTx<RepliesType<Reply<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S> Clone for ReplyHandle<S> where S: Service {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<S, NT> Replier<S, NT> where S: Service + 'static, NT: Node<PBFT<S::Data>> + 'static {
    pub fn new(node_id: NodeId, send_node: Arc<NT>) -> ReplyHandle<S> {
        let (ch_tx, ch_rx) = channel::new_bounded_sync(REPLY_CHANNEL_SIZE);

        let reply_task = Self {
            node_id,
            channel: ch_rx,
            send_node,
        };

        let handle = ReplyHandle::new(ch_tx);

        //reply_task.start();

        handle
    }

    pub fn start(mut self) {
        std::thread::Builder::new().name(format!("{:?} // Reply thread", self.node_id))
            .spawn(move || {
                loop {
                    let reply_batch = self.channel.recv().unwrap();

                    let mut batch = reply_batch.into_inner();

                    batch.sort_unstable_by_key(|update_reply| update_reply.to());

                    // keep track of the last message and node id
                    // we iterated over
                    let mut curr_send = None;

                    for update_reply in batch {
                        let (peer_id, session_id, operation_id, payload) = update_reply.into_inner();

                        // NOTE: the technique used here to peek the next reply is a
                        // hack... when we port this fix over to the production
                        // branch, perhaps we can come up with a better approach,
                        // but for now this will do
                        if let Some((message, last_peer_id)) = curr_send.take() {
                            let flush = peer_id != last_peer_id;
                            self.send_node.send(message, last_peer_id, flush);
                        }

                        // store previous reply message and peer id,
                        // for the next iteration
                        //TODO: Choose ordered or unordered reply
                        let message = NetworkMessageKind::from(
                            SystemMessage::OrderedReply(ReplyMessage::new(
                                session_id,
                                operation_id,
                                payload,
                            )));

                        curr_send = Some((message, peer_id));
                    }

                    // deliver last reply
                    if let Some((message, last_peer_id)) = curr_send {
                        self.send_node.send(message, last_peer_id, true);
                    } else {
                        // slightly optimize code path;
                        // the previous if branch will always execute
                        // (there is always at least one request in the batch)
                        unreachable!();
                    }
                }
            }).expect("Failed to launch thread for client replier!");
    }
}