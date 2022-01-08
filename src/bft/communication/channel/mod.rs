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
use std::thread::sleep;
use std::time::Duration;

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
        requests: ChannelTx<StoredMessage<RequestMessage<O>>>,
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

struct BatcherData<O> {
    batch: Vec<StoredMessage<RequestMessage<O>>>,
}

struct RequestBatcherShared<O> {
    event: Event,
    current: Mutex<BatcherData<O>>,
}

pub struct RequestBatcher<O> {
    shared: Arc<RequestBatcherShared<O>>,
    receiver: ChannelRx<StoredMessage<RequestMessage<O>>>,
    batcher: ChannelTx<Vec<StoredMessage<RequestMessage<O>>>>,
}

enum Batch<O> {
    Now(Vec<StoredMessage<RequestMessage<O>>>),
    Notify,
}

struct ExponentialBackoff {
    current_backoff: u64,
    multiplier: u64
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        Self {
            current_backoff: 1,
            multiplier: 2
        }
    }

    pub fn new_with_exponent(multiplier: u64) -> Self {
        Self {
            current_backoff: 1,
            multiplier
        }
    }

    pub fn sleep(&mut self) {
        sleep(Duration::from_millis(self.current_backoff));

        self.current_backoff *= self.multiplier;
    }
}

impl<O: Send + 'static> RequestBatcher<O> {
    pub fn spawn(mut self, max_batch_size: usize) {
        let mut batcher = self.batcher.clone();
        let shared = Arc::clone(&self.shared);

        // handle events to prepare new batch
        rt::spawn(async move {
            loop {
                let mut backoff = ExponentialBackoff::new();

                let batch = loop {
                    // check if batch is ready...
                    {
                        let opt_current = shared.current.try_lock();

                        match opt_current {
                            Some(mut current) => {
                                //PRINT HERE TO SEE THE BATCH SIZE
                                if current.batch.len() > 0 {
                                    //println!("Current batch length {}", current.batch.len() as u32);

                                    break std::mem::take(&mut current.batch);
                                }
                            }
                            None => {}
                        }
                    }

                    backoff.sleep();

                    // listen for batch changes
                    shared.event.listen().await;
                };

                let _ = batcher.send(batch).await;
            }
        });

        // handle reception of requests
        rt::spawn(async move {
            #[allow(unused_assignments)]
                let mut batch_size = 0;

            loop {
                let request = match self.receiver.recv().await {
                    Ok(r) => r,
                    Err(_) => return,
                };

                let batch = {
                    let mut current = self.shared
                        .current
                        .lock();

                    current.batch.push(request);
                    batch_size = current.batch.len();

                    let batch = if batch_size == max_batch_size {
                        Batch::Now(std::mem::take(&mut current.batch))
                    } else {
                        Batch::Notify
                    };

                    batch
                };

                match batch {
                    Batch::Now(batch) => self.batcher.send(batch).await.unwrap(),
                    Batch::Notify => self.shared.event.notify_additional(1),
                }
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

        let (req_tx, req_rx) = new_bounded(bound);
        let (reqbatch_tx, reqbatch_rx) = new_bounded(bound);

        let batcher = Some(RequestBatcher {
            receiver: req_rx,
            batcher: reqbatch_tx,
            shared: Arc::new(RequestBatcherShared {
                event: Event::new(),
                current: Mutex::new(BatcherData {
                    batch: Vec::new(),
                }),
            }),
        });
        let tx = MessageChannelTx::Server {
            consensus: c_tx,
            requests: req_tx,
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
                    consensus: consensus.clone(),
                    requests: requests.clone(),
                    other: other.clone(),
                }
            }
            MessageChannelTx::Client { replies, other } => {
                MessageChannelTx::Client {
                    other: other.clone(),
                    replies: replies.clone(),
                }
            }
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
                                requests.send(m).await
                            }
                            SystemMessage::Consensus(message) => {
                                let m = StoredMessage::new(header, message);
                                consensus.send(m).await
                            }
                            message @ SystemMessage::Cst(_) => {
                                other.send(Message::System(header, message)).await
                            }
                            message @ SystemMessage::ViewChange(_) => {
                                other.send(Message::System(header, message)).await
                            }
                            message @ SystemMessage::ForwardedRequests(_) => {
                                other.send(Message::System(header, message)).await
                            }
                            // drop other msgs
                            _ => Ok(()),
                        }
                    }
                    _ => other.send(message).await,
                }
            }
            MessageChannelTx::Client { replies, other } => {
                match message {
                    Message::System(header, message) => {
                        match message {
                            SystemMessage::Reply(message) => {
                                let m = StoredMessage::new(header, message);
                                replies.send(m).await
                            }
                            // drop other msgs
                            _ => Ok(()),
                        }
                    }
                    _ => other.send(message).await,
                }
            }
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
            }
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
            }
        }
    }
}
