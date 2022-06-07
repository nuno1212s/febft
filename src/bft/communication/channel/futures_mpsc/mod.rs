use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};
use async_channel::Recv;

use futures::channel::mpsc;
use futures::channel::mpsc::TrySendError;
use futures::stream::{
    Stream,
    FusedStream,
};
use futures::future::{
    poll_fn,
    FusedFuture,
};
use crate::bft::communication::channel::{RecvError, SendError};

pub struct ChannelAsyncTx<T> {
    inner: mpsc::Sender<T>,
}

pub struct ChannelRx<T> {
    inner: mpsc::Receiver<T>,
}

pub struct ChannelRxFut<'a, T> {
    inner: &'a mut mpsc::Receiver<T>,
}

impl<T> Clone for ChannelAsyncTx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

pub fn new_bounded<T>(bound: usize) -> (ChannelAsyncTx<T>, ChannelRx<T>) {
    let (tx, rx) = mpsc::channel(bound);
    let tx = ChannelAsyncTx { inner: tx };
    let rx = ChannelRx { inner: rx };
    (tx, rx)
}

impl<T> ChannelAsyncTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<(), SendError<T>> {
        match self.ready().await {
            Ok(_) => {}
            Err(_) => {
                return Err(SendError(message));
            }
        };

        match self.inner
            .try_send(message) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError(err.into_inner()))
            }
        }
    }

    #[inline]
    async fn ready(&mut self) -> Result<(), ()> {
        poll_fn(|cx| match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) if e.is_full() => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(())),
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
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<T, RecvError>> {
        Pin::new(&mut self.inner)
            .poll_next(cx)
            .map(|opt| opt.ok_or(RecvError::ChannelDc))
    }
}

impl<'a, T> FusedFuture for ChannelRxFut<'a, T> {
    #[inline]
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
