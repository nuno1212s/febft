use std::pin::Pin;
use std::future::Future;
use std::task::{Poll, Context};

use async_channel::{
    Sender,
    Receiver,
};
use futures::stream::{
    Stream,
    FusedStream,
};
use futures::future::FusedFuture;
use crate::channel::{RecvError, SendError};

pub struct ChannelAsyncTx<T> {
    inner: Sender<T>,
}

pub struct ChannelAsyncRx<T> {
    inner: Receiver<T>,
}

pub struct ChannelRxFut<'a, T> {
    inner: &'a mut Receiver<T>,
}

impl<T> Clone for ChannelAsyncTx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

impl<T> Clone for ChannelAsyncRx<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

pub fn new_bounded<T>(bound: usize) -> (ChannelAsyncTx<T>, ChannelAsyncRx<T>) {
    let (tx, rx) = async_channel::bounded(bound);
    let tx = ChannelAsyncTx { inner: tx };
    let rx = ChannelAsyncRx { inner: rx };
    (tx, rx)
}

impl<T> ChannelAsyncTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<(), SendError<T>> {
        match self.inner
            .send(message)
            .await {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError(err.into_inner()))
            }
        }
    }
}

impl<T> ChannelAsyncRx<T> {
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
