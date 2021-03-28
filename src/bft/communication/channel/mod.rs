//! FIFO channels used to send messages between async tasks.

use std::task::Poll;

use futures::select;
use futures::channel::mpsc;
use futures::stream::StreamExt;
use futures::future::poll_fn;

use crate::bft::error::*;
use crate::bft::communication::message::{
    Header,
    Message,
    SystemMessage,
    RequestMessage,
    ConsensusMessage,
};

/// Represents the sending half of a `Message` channel.
///
/// The handle can be cloned as many times as needed for cheap.
#[derive(Clone)]
pub struct MessageChannelTx<O> {
    other: mpsc::Sender<Message<O>>,
    requests: mpsc::Sender<(Header, RequestMessage<O>)>,
    consensus: mpsc::Sender<(Header, ConsensusMessage)>,
}

/// Represents the receiving half of a `Message` channel.
pub struct MessageChannelRx<O> {
    other: mpsc::Receiver<Message<O>>,
    requests: mpsc::Receiver<(Header, RequestMessage<O>)>,
    consensus: mpsc::Receiver<(Header, ConsensusMessage)>,
}

/// Creates a new channel that can queue up to `bound` messages
/// from different async senders.
pub fn new<O>(bound: usize) -> (MessageChannelTx<O>, MessageChannelRx<O>) {
    let (c_tx, c_rx) = mpsc::channel(bound);
    let (r_tx, r_rx) = mpsc::channel(bound);
    let (o_tx, o_rx) = mpsc::channel(bound);
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

impl<O> MessageChannelTx<O> {
    pub async fn send(&mut self, message: Message<O>) -> Result<()> {
        match message {
            Message::System(header, message) => {
                match message {
                    SystemMessage::Request(message) => {
                        poll_fn(|cx| match self.requests.poll_ready(cx) {
                            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
                            Poll::Ready(Err(e)) if e.is_full() => Poll::Pending,
                            Poll::Ready(e) => Poll::Ready(e.simple(ErrorKind::CommunicationChannel)),
                            Poll::Pending => Poll::Pending,
                        }).await?;
                        self.requests
                            .try_send((header, message))
                            .simple(ErrorKind::CommunicationChannel)
                    },
                    SystemMessage::Consensus(message) => {
                        poll_fn(|cx| match self.consensus.poll_ready(cx) {
                            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
                            Poll::Ready(Err(e)) if e.is_full() => Poll::Pending,
                            Poll::Ready(e) => Poll::Ready(e.simple(ErrorKind::CommunicationChannel)),
                            Poll::Pending => Poll::Pending,
                        }).await?;
                        self.consensus
                            .try_send((header, message))
                            .simple(ErrorKind::CommunicationChannel)
                    },
                }
            },
            _ => {
                poll_fn(|cx| match self.other.poll_ready(cx) {
                    Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
                    Poll::Ready(Err(e)) if e.is_full() => Poll::Pending,
                    Poll::Ready(e) => Poll::Ready(e.simple(ErrorKind::CommunicationChannel)),
                    Poll::Pending => Poll::Pending,
                }).await?;
                self.other
                    .try_send(message)
                    .simple(ErrorKind::CommunicationChannel)
            },
        }
    }
}

impl<O> MessageChannelRx<O> {
    pub async fn recv(&mut self) -> Result<Message<O>> {
        let message = select! {
            opt = self.consensus.next() => {
                let (h, c) = opt
                    .ok_or(Error::simple(ErrorKind::CommunicationChannel))?;
                Message::System(h, SystemMessage::Consensus(c))
            },
            opt = self.requests.next() => {
                let (h, r) = opt
                    .ok_or(Error::simple(ErrorKind::CommunicationChannel))?;
                Message::System(h, SystemMessage::Request(r))
            },
            opt = self.other.next() => {
                let message = opt
                    .ok_or(Error::simple(ErrorKind::CommunicationChannel))?;
                message
            },
        };
        Ok(message)
    }
}
