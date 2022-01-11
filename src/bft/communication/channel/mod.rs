//! FIFO channels used to send messages between async tasks.

#[cfg(feature = "channel_futures_mpsc")]
mod futures_mpsc;

#[cfg(feature = "channel_flume_mpmc")]
mod flume_mpmc;

#[cfg(feature = "channel_async_channel_mpmc")]
mod async_channel_mpmc;

use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use std::task::{Poll, Context};
use std::collections::LinkedList;

use chrono::offset::Utc;
use futures::select;
use event_listener::Event;
use futures::future::FusedFuture;
use parking_lot::Mutex;

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
pub enum MessageChannelTx<S, O, P> {
    Client {
        other: ChannelTx<Message<S, O, P>>,
        replies: ChannelTx<StoredMessage<ReplyMessage<P>>>,
    },
    Server {
        other: ChannelTx<Message<S, O, P>>,
        requests: Arc<RequestBatcherShared<O>>,
        consensus: ChannelTx<StoredMessage<ConsensusMessage<O>>>,
    },
}

/// Represents the receiving half of a `Message` channel.
pub enum MessageChannelRx<S, O, P> {
    Client {
        other: ChannelRx<Message<S, O, P>>,
        replies: ChannelRx<StoredMessage<ReplyMessage<P>>>,
    },
    Server {
        other: ChannelRx<Message<S, O, P>>,
        requests: ChannelRx<Vec<StoredMessage<RequestMessage<O>>>>,
        consensus: ChannelRx<StoredMessage<ConsensusMessage<O>>>,
    },
}

pub struct RequestBatcherShared<O> {
    event: Event,
    batch: Mutex<LinkedList<StoredMessage<RequestMessage<O>>>>,
}

pub struct RequestBatcher<O> {
    requests: Arc<RequestBatcherShared<O>>,
    to_core_server_task: ChannelTx<Vec<StoredMessage<RequestMessage<O>>>>,
}

impl<O: Send + 'static> RequestBatcher<O> {
    pub fn spawn(mut self, max_batch_size: usize) {
        // handle events to prepare new batch
        rt::spawn(async move {
            loop {
                let batch = 'new_batch: loop {
                    // check if batch is ready...
                    {
                        let mut current_batch = 'spin_lock: loop {
                            match self.requests.batch.try_lock() {
                                Some(batch) => break 'spin_lock batch,
                                None => (),
                            }
                            rt::yield_now().await;
                        };

                        if current_batch.len() > 0 {
                            let cap = std::cmp::min(current_batch.len(), max_batch_size);
                            let mut batch = Vec::with_capacity(cap);

                            for _i in 0..cap {
                                let request = match current_batch.pop_front() {
                                    Some(r) => r,
                                    _ => unreachable!(),
                                };
                                batch.push(request);
                            }

                            break 'new_batch batch;
                        }
                    }

                    // listen for new requests
                    self.requests.event.listen().await;
                };

                let _ = self
                    .to_core_server_task
                    .send(batch)
                    .await;
            }
        });
    }
}

/// Creates a new channel that can queue up to `bound` messages
/// from different async senders.
pub fn new_message_channel<S, O, P>(
    client: bool,
    bound: usize,
) -> (MessageChannelTx<S, O, P>, MessageChannelRx<S, O, P>, Option<RequestBatcher<O>>) {
    if client {
        let (rr_tx, rr_rx) = new_bounded(bound);
        let (o_tx, o_rx) = new_bounded(bound);

        let tx = MessageChannelTx::Client { replies: rr_tx, other: o_tx };
        let rx = MessageChannelRx::Client { replies: rr_rx, other: o_rx };

        (tx, rx, None)
    } else {
        let (c_tx, c_rx) = new_bounded(bound);
        let (o_tx, o_rx) = new_bounded(bound);

        let (reqbatch_tx, reqbatch_rx) = new_bounded(bound);
        let shared = Arc::new(RequestBatcherShared {
            event: Event::new(),
            batch: Mutex::new(LinkedList::new()),
        });

        let batcher = Some(RequestBatcher {
            requests: Arc::clone(&shared),
            to_core_server_task: reqbatch_tx,
        });
        let tx = MessageChannelTx::Server {
            consensus: c_tx,
            requests: shared,
            other: o_tx,
        };
        let rx = MessageChannelRx::Server {
            consensus: c_rx,
            requests: reqbatch_rx,
            other: o_rx,
        };
        (tx, rx, batcher)
    }
}

impl<S, O, P> Clone for MessageChannelTx<S, O, P> {
    fn clone(&self) -> Self {
        match self {
            MessageChannelTx::Server { consensus, requests, other } => {
                MessageChannelTx::Server {
                    requests: Arc::clone(requests),
                    consensus: consensus.clone(),
                    other: other.clone(),
                }
            },
            MessageChannelTx::Client { replies, other } => {
                MessageChannelTx::Client {
                    other: other.clone(),
                    replies: replies.clone(),
                }
            },
        }
    }
}

impl<S, O, P> MessageChannelTx<S, O, P> {
    pub async fn send(&mut self, message: Message<S, O, P>) -> Result<()> {
        match self {
            MessageChannelTx::Server { consensus, requests, other } => {
                match message {
                    Message::System(header, message) => {
                        match message {
                            SystemMessage::Request(message) => {
                                let m = StoredMessage::new(header, message);

                                'spin_lock: loop {
                                    if let Some(mut current_batch) = requests.batch.try_lock() {
                                        current_batch.push_back(m);
                                        break 'spin_lock;
                                    }
                                    rt::yield_now().await;
                                }

                                requests.event.notify(1);
                                Ok(())
                            },
                            SystemMessage::Consensus(message) => {
                                let m = StoredMessage::new(header, message);
                                consensus.send(m).await
                            },
                            message @ SystemMessage::Cst(_) => {
                                other.send(Message::System(header, message)).await
                            },
                            message @ SystemMessage::ViewChange(_) => {
                                other.send(Message::System(header, message)).await
                            },
                            message @ SystemMessage::ForwardedRequests(_) => {
                                other.send(Message::System(header, message)).await
                            },
                            // drop other msgs
                            _ => Ok(()),
                        }
                    },
                    _ => other.send(message).await,
                }
            },
            MessageChannelTx::Client { replies, other } => {
                match message {
                    Message::System(header, message) => {
                        match message {
                            SystemMessage::Reply(message) => {
                                let m = StoredMessage::new(header, message);
                                replies.send(m).await
                            },
                            // drop other msgs
                            _ => Ok(()),
                        }
                    },
                    _ => other.send(message).await,
                }
            },
        }
    }
}

impl<S, O, P> MessageChannelRx<S, O, P> {
    pub async fn recv(&mut self) -> Result<Message<S, O, P>> {
        match self {
            MessageChannelRx::Server { consensus, requests, other } => {
                let message = select! {
                    result = consensus.recv() => {
                        let (h, c) = result?.into_inner();
                        Message::System(h, SystemMessage::Consensus(c))
                    },
                    result = requests.recv() => {
                        let batch = result?;
                        Message::RequestBatch(Utc::now(), batch)
                    },
                    result = other.recv() => {
                        let message = result?;
                        message
                    },
                };
                Ok(message)
            },
            MessageChannelRx::Client { replies, other } => {
                let message = select! {
                    result = replies.recv() => {
                        let (h, r) = result?.into_inner();
                        Message::System(h, SystemMessage::Reply(r))
                    },
                    result = other.recv() => {
                        let message = result?;
                        message
                    },
                };
                Ok(message)
            },
        }
    }
}
