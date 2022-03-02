//! FIFO channels used to send messages between async tasks.

use std::collections::LinkedList;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use chrono::offset::Utc;
use event_listener::Event;
use futures::future::FusedFuture;
use futures::select;
use parking_lot::Mutex;

use crate::bft::async_runtime as rt;
use crate::bft::communication::message::{
    ConsensusMessage,
    Message,
    ReplyMessage,
    RequestMessage,
    StoredMessage,
    SystemMessage,
};
use crate::bft::error::*;

#[cfg(feature = "channel_futures_mpsc")]
mod futures_mpsc;

#[cfg(feature = "channel_flume_mpmc")]
mod flume_mpmc;

#[cfg(feature = "channel_async_channel_mpmc")]
mod async_channel_mpmc;

#[cfg(feature = "channel_custom_dump")]
mod flume_mpmc;

#[cfg(feature = "channel_custom_dump")]
mod custom_dump;


/// General purpose channel's sending half.
pub struct ChannelTx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelTx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelTx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelTx<T>,

    #[cfg(feature = "channel_custom_dump")]
    inner: flume_mpmc::ChannelTx<T>,
}

unsafe impl<T> Send for ChannelTx<T> {}

/// General purpose channel's receiving half.
pub struct ChannelRx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelRx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelRx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelRx<T>,

    #[cfg(feature = "channel_custom_dump")]
    inner: flume_mpmc::ChannelRx<T>,
}

/// Future for a general purpose channel's receiving operation.
pub struct ChannelRxFut<'a, T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelRxFut<'a, T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelRxFut<'a, T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelRxFut<'a, T>,

    #[cfg(feature = "channel_custom_dump")]
    inner: flume_mpmc::ChannelRxFut<'a, T>,
}

impl<T> Clone for ChannelTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

impl<T> Clone for ChannelRx<T> {

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

        #[cfg(feature = "channel_custom_dump")]
            { flume_mpmc::new_bounded(bound) }
    };

    let ttx = ChannelTx { inner: tx };

    let rrx = ChannelRx { inner: rx };

    (ttx, rrx)
}

#[cfg(feature = "channel_custom_dump")]
#[inline]
pub fn new_bounded_mult<T>(bound: usize) -> (custom_dump::ChannelTx<T>, custom_dump::ChannelRxMult<T>) {
    custom_dump::bounded_mult_channel(bound)
}

impl<T> ChannelTx<T> {
    #[inline]
    pub async fn send(&mut self, message: T) -> Result<()> {
        match self.inner.send(message).await {
            Ok(_) => {
                Ok(())
            }
            Err(_) => {
                Err(Error::simple(ErrorKind::CommunicationChannelAsyncChannelMpmc))
            }
        }
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