//! FIFO channels used to send messages between async tasks.

use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};

use futures::select;
use futures::channel::mpsc;
use futures::stream::{
    Stream,
    FusedStream,
};
use futures::future::{
    poll_fn,
    FusedFuture,
};

use crate::bft::error::*;
use crate::bft::communication::message::{
    Header,
    Message,
    SystemMessage,
    RequestMessage,
    ConsensusMessage,
};

/// General purpose channel's sending half.
pub struct ChannelTx<T> {
    inner: mpsc::Sender<T>,
}

/// General purpose channel's receiving half.
pub struct ChannelRx<T> {
    inner: mpsc::Receiver<T>,
}

/// Future for a general purpose channel's receiving operation.
pub struct ChannelRxFut<'a, T> {
    inner: &'a mut mpsc::Receiver<T>,
}

impl<T> Clone for ChannelTx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

/// Creates a new general purpose channel that can queue up to
/// `bound` messages from different async senders.
pub fn new_bounded<T>(bound: usize) -> (ChannelTx<T>, ChannelRx<T>) {
    let (tx, rx) = mpsc::channel(bound);
    let tx = ChannelTx { inner: tx };
    let rx = ChannelRx { inner: rx };
    (tx, rx)
}

impl<T> ChannelTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<()> {
        self.ready().await?;
        self.inner
            .try_send(message)
            .simple(ErrorKind::CommunicationChannel)
    }

    #[inline]
    async fn ready(&mut self) -> Result<()> {
        poll_fn(|cx| match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) if e.is_full() => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(Error::simple(ErrorKind::CommunicationChannel))),
            Poll::Pending => Poll::Pending,
        }).await
    }
}

impl<T> ChannelRx<T> {
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T> {
        let inner = &mut self.inner;
        ChannelRxFut { inner }
    }
}

impl<'a, T> Future for ChannelRxFut<'a, T> {
    type Output = Result<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.ok_or(Error::simple(ErrorKind::CommunicationChannel)))
    }
}

impl<'a, T> FusedFuture for ChannelRxFut<'a, T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

/// Represents the sending half of a `Message` channel.
///
/// The handle can be cloned as many times as needed for cheap.
pub struct MessageChannelTx<O, R> {
    other: ChannelTx<Message<O, R>>,
    requests: ChannelTx<(Header, RequestMessage<O, R>)>,
    consensus: ChannelTx<(Header, ConsensusMessage)>,
}

/// Represents the receiving half of a `Message` channel.
pub struct MessageChannelRx<O, R> {
    other: ChannelRx<Message<O, R>>,
    requests: ChannelRx<(Header, RequestMessage<O, R>)>,
    consensus: ChannelRx<(Header, ConsensusMessage)>,
}

/// Creates a new channel that can queue up to `bound` messages
/// from different async senders.
pub fn new_message_channel<O, R>(bound: usize) -> (MessageChannelTx<O, R>, MessageChannelRx<O, R>) {
    let (c_tx, c_rx) = new_bounded(bound);
    let (r_tx, r_rx) = new_bounded(bound);
    let (o_tx, o_rx) = new_bounded(bound);
    let tx = MessageChannelTx {
        consensus: c_tx,
        requests: r_tx,
        other: o_tx,
    };
    let rx = MessageChannelRx {
        consensus: c_rx,
        requests: r_rx,
        other: o_rx,
    };
    (tx, rx)
}

impl<O, R> Clone for MessageChannelTx<O, R> {
    fn clone(&self) -> Self {
        Self {
            consensus: self.consensus.clone(),
            requests: self.requests.clone(),
            other: self.other.clone(),
        }
    }
}

impl<O, R> MessageChannelTx<O, R> {
    pub async fn send(&mut self, message: Message<O, R>) -> Result<()> {
        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::Request(message) => {
                        self.requests.send((header, message)).await
                    },
                    SystemMessage::Consensus(message) => {
                        self.consensus.send((header, message)).await
                    },
                }
            },
            _ => {
                self.other.send(message).await
            },
        }
    }
}

impl<O, R> MessageChannelRx<O, R> {
    pub async fn recv(&mut self) -> Result<Message<O, R>> {
        let message = select! {
            result = self.consensus.recv() => {
                let (h, c) = result?;
                Message::System(h, SystemMessage::Consensus(c))
            },
            result = self.requests.recv() => {
                let (h, r) = result?;
                Message::System(h, SystemMessage::Request(r))
            },
            result = self.other.recv() => {
                let message = result?;
                message
            },
        };
        Ok(message)
    }
}
