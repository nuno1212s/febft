use std::ops::Deref;
use std::sync::Arc;
use crate::bft::communication::{channel, NodeId, SendNode};
use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};
use crate::bft::communication::message::{ReplyMessage, SystemMessage};
use crate::bft::consensus::log::Log;
use crate::bft::executable::{Reply, Request, Service, State, UpdateBatchReplies};

type RepliesType<S> = UpdateBatchReplies<S>;

pub struct Replier<S> where S: Service + 'static {
    node_id: NodeId,
    channel:  ChannelSyncRx<RepliesType<Reply<S>>>,
    send_node: SendNode<S::Data>,
    log: Arc<Log<State<S>, Request<S>, Reply<S>>>
}

pub struct ReplyHandle<S> where S: Service {
    inner: ChannelSyncTx<RepliesType<Reply<S>>>
}

const REPLY_CHANNEL_SIZE : usize = 1024;

impl<S> ReplyHandle<S> where S:Service {

    pub fn new(replier: ChannelSyncTx<RepliesType<Reply<S>>>) -> Self {
        Self {
            inner: replier
        }
    }

}

impl<S> Deref for ReplyHandle<S> where S:Service {
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

impl<S> Replier<S> where S: Service + 'static{

    pub fn new(node_id: NodeId, send_node: SendNode<S::Data>,
               log: Arc<Log<State<S>, Request<S>, Reply<S>>>) -> ReplyHandle<S> {
        let (ch_tx, ch_rx) = channel::new_bounded_sync(REPLY_CHANNEL_SIZE);

        let reply_task = Self {
            node_id,
            channel: ch_rx,
            send_node,
            log
        };

        let handle = ReplyHandle::new(ch_tx);

        reply_task.start();

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
                            self.send_node.send(message, last_peer_id, flush, Arc::clone(self.log.batch_meta()));
                        }

                        // store previous reply message and peer id,
                        // for the next iteration
                        let message = SystemMessage::Reply(ReplyMessage::new(
                            session_id,
                            operation_id,
                            payload,
                        ));

                        curr_send = Some((message, peer_id));
                    }

                    // deliver last reply
                    if let Some((message, last_peer_id)) = curr_send {
                        self.send_node.send(message, last_peer_id, true, Arc::clone(self.log.batch_meta()));
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