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
use std::time::{Instant, Duration};

use futures::select;
use futures_timer::Delay;
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

struct BatcherData<O> {
    batch: Vec<StoredMessage<RequestMessage<O>>>,
}

struct BatcherTimer {
    current_load: f32,
    current_request_count: usize,
    now: Instant,
    batch_until: Instant,
    batch_fill_start: Instant,
    last_load_time: Instant,
    batch_load_size: [Option<usize>; 5],
}

impl BatcherTimer {
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

    fn new() -> Self {
        let now = Instant::now();
        Self {
            now,
            current_load: 0.0,
            last_load_time: now,
            batch_fill_start: now,
            current_request_count: 0,
            batch_until: now + Self::DEFAULT_TIMEOUT,
            batch_load_size: [None, None, None, None, None],
        }
    }

    fn update(&mut self, max_batch_size: usize) {
        self.current_request_count += 1;
        self.now = Instant::now();

        if self.now.duration_since(self.last_load_time) >= Duration::from_secs(1) {
            let reqs = if self.current_request_count > max_batch_size {
                max_batch_size as f32
            } else {
                self.current_request_count as f32
            };

            self.current_load = reqs / (max_batch_size as f32);
            self.current_request_count = 0;
            self.last_load_time = self.now;
        }
    }

    fn wait(&self) -> bool {
        if self.now < self.batch_until {
            eprintln!("Waiting: {:?}", self.batch_until - self.now);
            true
        } else {
            false
        }
    }

    fn batch_filled<O: Send + 'static>(
        &mut self,
        shared: &Arc<RequestBatcherShared<O>>,
        batch_size: usize,
        max_batch_size: usize,
    ) {
        let elapsed = self.now
            .duration_since(self.batch_fill_start)
            .as_secs_f32();

        let curr_batch = batch_size as f32;
        let last_batch = self.get_batch(max_batch_size) as f32;

        let next_timeout = (curr_batch / last_batch) * elapsed;
        let next_timeout = Duration::from_secs_f32(next_timeout);

        self.batch_until = self.now + next_timeout;
        self.batch_fill_start = self.now;
        self.set_batch(batch_size);

        let shared = Arc::clone(shared);

        // spuriously wake up event listeners
        rt::spawn(async move {
            loop {
                Delay::new(next_timeout).await;
                shared.event.notify(1);
            }
        });
    }

    fn set_batch(&mut self, batch_size: usize) {
        self.batch_load_size[self.load_setting()] = Some(batch_size);
        eprintln!("Batch history: {:?}", self.batch_load_size);
    }

    fn get_batch(&self, max_batch_size: usize) -> usize {
        self.batch_load_size[self.load_setting()]
            .unwrap_or_else(|| (self.current_load * (max_batch_size as f32)) as usize)
    }

    fn load_setting(&self) -> usize {
        let load = self.current_load;
        match () {
            _ if load < 0.1 => 0,
            _ if load < 0.3 => 1,
            _ if load < 0.6 => 2,
            _ if load < 0.8 => 3,
            _  => 4,
        }
    }
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
    Wait,
}

impl<O: Send + 'static> RequestBatcher<O> {
    pub fn spawn(mut self, batch_size: usize) {
        let mut batcher = self.batcher.clone();
        let shared = Arc::clone(&self.shared);

        // handle events to prepare new batch
        rt::spawn(async move {
            loop {
                let batch = loop {
                    // check if batch is ready...
                    {
                        let mut current = shared.current.lock();
                        if current.batch.len() > 0 {
                            eprintln!("Batch: {}", current.batch.len());
                            break std::mem::take(&mut current.batch);
                        }
                    }

                    // listen for batch changes
                    shared.event.listen().await;
                };

                let _ = batcher.send(batch).await;
            }
        });

        // handle reception of requests
        rt::spawn(async move {
            let mut timer = BatcherTimer::new();

            loop {
                let result = self.receiver.recv().await;
                let request = match result {
                    Ok(r) => r,
                    Err(_) => return,
                };

                let batch = {
                    // update the calc of the load of the system
                    timer.update(batch_size);

                    eprintln!("LOCK");
                    let mut current = self.shared
                        .current
                        .lock();

                    current.batch.push(request);

                    let batch = if current.batch.len() == batch_size {
                        eprintln!("Batch: {} | Load: {:.3}", current.batch.len(), timer.current_load);
                        timer.batch_filled(&self.shared, batch_size, batch_size);
                        Batch::Now(std::mem::take(&mut current.batch))
                    } else if timer.wait() {
                        eprintln!("WAIT");
                        Batch::Wait
                    } else {
                        eprintln!("Batch: {} | Load: {:.3}", current.batch.len(), timer.current_load);
                        timer.batch_filled(&self.shared, current.batch.len(), batch_size);
                        Batch::Notify
                    };

                    batch
                };
                eprintln!("UNLOCK");

                match batch {
                    Batch::Now(batch) => self.batcher.send(batch).await.unwrap_or(()),
                    Batch::Notify => self.shared.event.notify(1),
                    Batch::Wait => (),
                }
            }
        });
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
        shared: Arc::new(RequestBatcherShared {
            event: Event::new(),
            current: Mutex::new(BatcherData {
                batch: Vec::new(),
            }),
        }),
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
