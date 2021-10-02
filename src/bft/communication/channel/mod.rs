//! FIFO channels used to send messages between async tasks.

#[cfg(feature = "channel_futures_mpsc")]
mod futures_mpsc;

#[cfg(feature = "channel_flume_mpmc")]
mod flume_mpmc;

#[cfg(feature = "channel_async_channel_mpmc")]
mod async_channel_mpmc;

use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};
use std::time::Duration;

use futures::select;
use futures_timer::Delay;
use futures::future::{
    FusedFuture,
    FutureExt,
};

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::message::{
    Message,
    SystemMessage,
    RequestMessage,
    ReplyMessage,
    StoredMessage,
    ConsensusMessage,
};

/// General purpose channel's sending half.
pub struct ChannelTx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelTx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelTx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelTx<T>,
}

/// General purpose channel's receiving half.
pub struct ChannelRx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelRx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelRx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelRx<T>,
}

/// Future for a general purpose channel's receiving operation.
pub struct ChannelRxFut<'a, T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelRxFut<'a, T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelRxFut<'a, T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelRxFut<'a, T>,
}

impl<T> Clone for ChannelTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

/// Creates a new general purpose channel that can queue up to
/// `bound` messages from different async senders.
#[inline]
pub fn new_bounded<T>(bound: usize) -> (ChannelTx<T>, ChannelRx<T>) {
    let (tx, rx) = {
        #[cfg(feature = "channel_futures_mpsc")]
        { futures_mpsc::new_bounded(bound) }

        #[cfg(feature = "channel_flume_mpmc")]
        { flume_mpmc::new_bounded(bound) }

        #[cfg(feature = "channel_async_channel_mpmc")]
        { async_channel_mpmc::new_bounded(bound) }
    };
    let tx = ChannelTx { inner: tx };
    let rx = ChannelRx { inner: rx };
    (tx, rx)
}

impl<T> ChannelTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<()> {
        self.inner.send(message).await
    }
}

impl<T> ChannelRx<T> {
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T> {
        let inner = self.inner.recv();
        ChannelRxFut { inner }
    }
}

impl<'a, T> Future for ChannelRxFut<'a, T> {
    type Output = Result<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<'a, T> FusedFuture for ChannelRxFut<'a, T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Represents the sending half of a `Message` channel.
///
/// The handle can be cloned as many times as needed for cheap.
pub struct MessageChannelTx<S, O, P> {
    other: ChannelTx<Message<S, O, P>>,
    requests: ChannelTx<StoredMessage<RequestMessage<O>>>,
    replies: ChannelTx<StoredMessage<ReplyMessage<P>>>,
    consensus: ChannelTx<StoredMessage<ConsensusMessage<O>>>,
}

/// Represents the receiving half of a `Message` channel.
pub struct MessageChannelRx<S, O, P> {
    other: ChannelRx<Message<S, O, P>>,
    requests: ChannelRx<Vec<StoredMessage<RequestMessage<O>>>>,
    replies: ChannelRx<StoredMessage<ReplyMessage<P>>>,
    consensus: ChannelRx<StoredMessage<ConsensusMessage<O>>>,
}

pub struct RequestBatcher<O> {
    current_batch: Vec<StoredMessage<RequestMessage<O>>>,
    receiver: ChannelRx<StoredMessage<RequestMessage<O>>>,
    batcher: ChannelTx<Vec<StoredMessage<RequestMessage<O>>>>,
}

impl<O: Send + 'static> RequestBatcher<O> {
    pub fn spawn(mut self, batch_size: usize) {
        rt::spawn(async move {
            // TODO: make this customizable
            const BATCH_POLL: Duration = Duration::from_millis(10);

            loop {
                select! {
                    _ = Delay::new(BATCH_POLL).fuse() => {
                        if self.current_batch.len() > 0 {
                            eprintln!("Sent batch on poll");
                            self.send_batch().await;
                        }
                    },
                    result = self.receiver.recv() => {
                        let request = match result {
                            Ok(r) => r,
                            Err(_) => return,
                        };
                        self.current_batch.push(request);
                        if self.current_batch.len() == batch_size {
                            eprintln!("Sent batch on full");
                            self.send_batch().await;
                        }
                    },
                }
            }
        });
    }

    async fn send_batch(&mut self) {
        let batch = std::mem::replace(&mut self.current_batch, Vec::new());
        eprintln!("Batch length = {}", batch.len());
        self.batcher.send(batch).await.unwrap_or(());
    }
}

/// Creates a new channel that can queue up to `bound` messages
/// from different async senders.
pub fn new_message_channel<S, O, P>(
    bound: usize,
) -> (MessageChannelTx<S, O, P>, MessageChannelRx<S, O, P>, RequestBatcher<O>) {
    let (c_tx, c_rx) = new_bounded(bound);
    let (rr_tx, rr_rx) = new_bounded(bound);
    let (o_tx, o_rx) = new_bounded(bound);

    let (req_tx, req_rx) = new_bounded(bound);
    let (reqbatch_tx, reqbatch_rx) = new_bounded(bound);

    let batcher = RequestBatcher {
        receiver: req_rx,
        batcher: reqbatch_tx,
        current_batch: Vec::new(),
    };
    let tx = MessageChannelTx {
        consensus: c_tx,
        requests: req_tx,
        replies: rr_tx,
        other: o_tx,
    };
    let rx = MessageChannelRx {
        consensus: c_rx,
        requests: reqbatch_rx,
        replies: rr_rx,
        other: o_rx,
    };
    (tx, rx, batcher)
}

impl<S, O, P> Clone for MessageChannelTx<S, O, P> {
    fn clone(&self) -> Self {
        Self {
            consensus: self.consensus.clone(),
            requests: self.requests.clone(),
            replies: self.replies.clone(),
            other: self.other.clone(),
        }
    }
}

impl<S, O, P> MessageChannelTx<S, O, P> {
    pub async fn send(&mut self, message: Message<S, O, P>) -> Result<()> {
        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::Request(message) => {
                        let m = StoredMessage::new(header, message);
                        self.requests.send(m).await
                    },
                    SystemMessage::Reply(message) => {
                        let m = StoredMessage::new(header, message);
                        self.replies.send(m).await
                    },
                    SystemMessage::Consensus(message) => {
                        let m = StoredMessage::new(header, message);
                        self.consensus.send(m).await
                    },
                    message @ SystemMessage::Cst(_) => {
                        self.other.send(Message::System(header, message)).await
                    },
                    message @ SystemMessage::ViewChange(_) => {
                        self.other.send(Message::System(header, message)).await
                    },
                    message @ SystemMessage::ForwardedRequests(_) => {
                        self.other.send(Message::System(header, message)).await
                    },
                }
            },
            _ => {
                self.other.send(message).await
            },
        }
    }
}

impl<S, O, P> MessageChannelRx<S, O, P> {
    pub async fn recv(&mut self) -> Result<Message<S, O, P>> {
        let message = select! {
            result = self.consensus.recv() => {
                let (h, c) = result?.into_inner();
                Message::System(h, SystemMessage::Consensus(c))
            },
            result = self.requests.recv() => {
                let batch = result?;
                Message::RequestBatch(batch)
            },
            result = self.replies.recv() => {
                let (h, r) = result?.into_inner();
                Message::System(h, SystemMessage::Reply(r))
            },
            result = self.other.recv() => {
                let message = result?;
                message
            },
        };
        Ok(message)
    }
}
