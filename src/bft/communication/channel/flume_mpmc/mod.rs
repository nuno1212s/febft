use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};

use futures::future::FusedFuture;

use crate::bft::error::*;

pub struct ChannelTx<T> {
    inner: ::flume::Sender<T>,
}

pub struct ChannelRx<T> {
    inner: ::flume::Receiver<T>,
}

pub struct ChannelRxFut<'a, T> {
    inner: ::flume::r#async::RecvFut<'a, T>,
}

impl<T> Clone for ChannelTx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

pub fn new_bounded<T>(bound: usize) -> (ChannelTx<T>, ChannelRx<T>) {
    let (tx, rx) = ::flume::bounded(bound);
    let tx = ChannelTx { inner: tx };
    let rx = ChannelRx { inner: rx };
    (tx, rx)
}

impl<T> ChannelTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<()> {
        self.inner.send_async(message).await
            .simple(ErrorKind::CommunicationChannelFlumeMpmc)
    }
}

impl<T> ChannelRx<T> {
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T> {
        let inner = self.inner.recv_async();
        ChannelRxFut { inner }
    }

    #[inline]
    pub fn drain_into(&mut self, buf: &mut Vec<T>, up_to: usize) {
        let messages = self.inner
            .drain()
            .take(up_to);
        buf.extend(messages);
    }
}

impl<'a, T> Future for ChannelRxFut<'a, T> {
    type Output = Result<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T>> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|r| r.simple(ErrorKind::CommunicationChannelFlumeMpmc))
    }
}

impl<'a, T> FusedFuture for ChannelRxFut<'a, T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
