//! FIFO channels used to send messages between async tasks.

use std::fmt::{Debug, Display, Formatter, write};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::FusedFuture;

use crate::bft::error::*;

///TODO: This has to be reworked in order to incorporate
/// The fact that most of these channels do not support
/// sync operation. We want to add maybe a mix of these?
/// idk
#[cfg(feature = "channel_futures_mpsc")]
///TODO: Remove this
mod futures_mpsc;

#[cfg(feature = "channel_flume_mpmc")]
mod flume_mpmc;

#[cfg(feature = "channel_async_channel_mpmc")]
///TODO: Remove this
mod async_channel_mpmc;

#[cfg(feature = "channel_custom_dump")]
mod custom_dump;

#[cfg(feature = "channel_sync_crossbeam")]
mod crossbeam;

/**
 * ASYNCHRONOUS CHANNEL
 */
/// General purpose channel's sending half.
pub struct ChannelAsyncTx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelAsyncTx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelMixedTx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelAsyncTx<T>,
}

unsafe impl<T> Send for ChannelAsyncTx<T> {}

/// General purpose channel's receiving half.
pub struct ChannelAsyncRx<T> {
    #[cfg(feature = "channel_futures_mpsc")]
    inner: futures_mpsc::ChannelRx<T>,

    #[cfg(feature = "channel_flume_mpmc")]
    inner: flume_mpmc::ChannelMixedRx<T>,

    #[cfg(feature = "channel_async_channel_mpmc")]
    inner: async_channel_mpmc::ChannelAsyncRx<T>,
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

impl<T> Clone for ChannelAsyncTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

impl<T> Clone for ChannelAsyncRx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}

impl<T> ChannelAsyncTx<T> {

    //Can have length because future mpsc doesn't implement it

    //Asynchronously send message through channel
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

impl<T> ChannelAsyncRx<T> {
    //Asynchronously recv message from channel
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

/// Creates a new general purpose channel that can queue up to
/// `bound` messages from different async senders.
#[inline]
pub fn new_bounded_async<T>(bound: usize) -> (ChannelAsyncTx<T>, ChannelAsyncRx<T>) {
    let (tx, rx) = {
        #[cfg(feature = "channel_futures_mpsc")]
        { futures_mpsc::new_bounded(bound) }
        #[cfg(feature = "channel_flume_mpmc")]
        { flume_mpmc::new_bounded(bound) }
        #[cfg(feature = "channel_async_channel_mpmc")]
        { async_channel_mpmc::new_bounded(bound) }
    };

    let ttx = ChannelAsyncTx { inner: tx };

    let rrx = ChannelAsyncRx { inner: rx };

    (ttx, rrx)
}

/**
Sync channels
 */
pub struct ChannelSyncRx<T> {
    #[cfg(feature = "channel_sync_crossbeam")]
    inner: crossbeam::ChannelSyncRx<T>,
    #[cfg(feature = "channel_sync_flume")]
    inner: flume_mpmc::ChannelMixedRx<T>,
}

pub struct ChannelSyncTx<T> {
    #[cfg(feature = "channel_sync_crossbeam")]
    inner: crossbeam::ChannelSyncTx<T>,
    #[cfg(feature = "channel_sync_flume")]
    inner: flume_mpmc::ChannelMixedTx<T>,
}

impl<T> ChannelSyncRx<T> {

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn try_recv(&self) -> std::result::Result<T, ()> {
        match self.inner.try_recv() {
            Ok(res) => {
                Ok(res)
            }
            Err(_err) => {
                Err(())
            }
        }
    }

    #[inline]
    pub fn recv(&self) -> std::result::Result<T, ()> {
        match self.inner.recv() {
            Ok(res) => {
                Ok(res)
            }
            Err(_err) => {
                Err(())
            }
        }
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> std::result::Result<T, ()> {
        match self.inner.recv_timeout(timeout) {
            Ok(result) => {
                Ok(result)
            }
            Err(_) => {
                Err(())
            }
        }
    }
}

impl<T> ChannelSyncTx<T> {

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn send(&self, value: T) -> std::result::Result<(), SendError<T>> {
        match self.inner.send(value) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError::ChannelDc(err.into_inner()))
            }
        }
    }

    #[inline]
    pub fn send_timeout(&self, value: T, timeout: Duration) -> std::result::Result<(), SendError<T>> {
        match self.inner.send_timeout(value, timeout) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError::ChannelDc(err.into_inner()))
            }
        }
    }
}

impl<T> Clone for ChannelSyncTx<T> {
    fn clone(&self) -> Self {
        ChannelSyncTx {
            inner: self.inner.clone()
        }
    }
}

impl<T> Clone for ChannelSyncRx<T> {
    fn clone(&self) -> Self {
        ChannelSyncRx {
            inner: self.inner.clone()
        }
    }
}

#[inline]
pub fn new_bounded_sync<T>(bound: usize) -> (ChannelSyncTx<T>, ChannelSyncRx<T>) {
    #[cfg(feature = "channel_sync_crossbeam")]
    {
        let (tx, rx) = crossbeam::new_bounded(bound);

        (ChannelSyncTx { inner: tx }, ChannelSyncRx { inner: rx })
    }

    #[cfg(feature = "channel_sync_flume")]
    {
        let (tx, rx) = flume_mpmc::new_bounded(bound);

        (ChannelSyncTx { inner: tx }, ChannelSyncRx { inner: rx })
    }
}

/**
Async and sync mixed channels (Allows us to connect async and sync environments together)
 */
pub struct ChannelMixedRx<T> {
    #[cfg(feature = "channel_mixed_flume")]
    inner: flume_mpmc::ChannelMixedRx<T>,
}

pub struct ChannelMixedTx<T> {
    #[cfg(feature = "channel_mixed_flume")]
    inner: flume_mpmc::ChannelMixedTx<T>,
}

impl<T> ChannelMixedRx<T> {

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn recv(&self) -> std::result::Result<T, RecvError> {
        match self.inner.recv_sync() {
            Ok(res) => {
                Ok(res)
            }
            Err(_err) => {
                Err(RecvError::ChannelDc)
            }
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> std::result::Result<T, RecvError> {
        match self.inner.recv_timeout(timeout) {
            Ok(result) => {
                Ok(result)
            }
            Err(_err) => {
                Err(RecvError::ChannelDc)
            }
        }
    }

    pub async fn recv_async(&mut self) -> std::result::Result<T,RecvError> {
        match self.inner.recv().await {
            Ok(val) => {
                Ok(val)
            }
            Err(_err) => {
                Err(RecvError::ChannelDc)
            }
        }
    }
}

impl<T> ChannelMixedTx<T> {

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub async fn send_async(&self, value: T) -> std::result::Result<(), SendError<T>> {
        self.inner.send(value).await
    }

    #[inline]
    pub fn send(&self, value: T) -> std::result::Result<(), SendError<T>> {
        self.inner.send_sync(value)
    }

    #[inline]
    pub fn send_timeout(&self, value: T, timeout: Duration) -> std::result::Result<(), SendError<T>> {
        self.inner.send_timeout(value, timeout)
    }
}


impl<T> Clone for ChannelMixedTx<T> {
    fn clone(&self) -> Self {
        ChannelMixedTx {
            inner: self.inner.clone()
        }
    }
}

impl<T> Clone for ChannelMixedRx<T> {
    fn clone(&self) -> Self {
        ChannelMixedRx {
            inner: self.inner.clone()
        }
    }
}

pub fn new_bounded_mixed<T>(bound: usize) -> (ChannelMixedTx<T>, ChannelMixedRx<T>) {
    let (tx, rx) = {
        #[cfg(feature = "channel_mixed_flume")]
        {
            flume_mpmc::new_bounded(bound)
        }
    };

    (ChannelMixedTx { inner: tx }, ChannelMixedRx { inner: rx })
}

/**
Channel with capability of dumping multiple members in a couple of CAS operations
 */

pub struct ChannelMultTx<T> {

    #[cfg(feature = "channel_mult_custom_dump")]
    inner: custom_dump::ChannelTx<T>

}

pub struct ChannelMultRx<T> {
    #[cfg(feature = "channel_mult_custom_dump")]
    inner: custom_dump::ChannelRxMult<T>
}

#[cfg(feature = "channel_custom_dump")]
#[inline]
pub fn new_bounded_mult<T>(bound: usize) -> (custom_dump::ChannelTx<T>, custom_dump::ChannelRxMult<T>) {
    custom_dump::bounded_mult_channel(bound)
}

pub enum RecvError {
    ChannelDc,
    ChannelEmpty
}

impl Debug for RecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to recv message")
    }
}

impl Display for RecvError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for RecvError {

}

pub enum SendError<T> {
    ChannelDc(T),

}

impl<T> Debug for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to send message")
    }
}

impl<T> Display for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl<T> std::error::Error for SendError<T> {

}