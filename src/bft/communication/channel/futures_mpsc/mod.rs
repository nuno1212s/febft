use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};

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

pub struct ChannelTx<T> {
    inner: mpsc::Sender<T>,
}

pub struct ChannelRx<T> {
    inner: mpsc::Receiver<T>,
}

pub struct ChannelRxFut<'a, T> {
    inner: &'a mut mpsc::Receiver<T>,
}

impl<T> Clone for ChannelTx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

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
    #[inline]
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
