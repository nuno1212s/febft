use std::pin::Pin;
use std::future::Future;
use std::ops::Deref;
use std::task::{Poll, Context};
use std::time::Duration;

use futures::future::FusedFuture;
use crate::bft::communication::channel::SendError;

use crate::bft::error::*;

/**
Mixed channels
 */
pub struct ChannelMixedRx<T> {
    inner: flume::Receiver<T>,
}

pub struct ChannelMixedTx<T> {
    inner: ::flume::Sender<T>,
}

pub struct ChannelRxFut<'a, T> {
    inner: ::flume::r#async::RecvFut<'a, T>,
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

impl<T> ChannelMixedTx<T> {
    #[inline]
    pub async fn send(&self, message: T) -> std::result::Result<(), SendError<T>> {
        match self.inner.send_async(message).await {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError::ChannelDc(err.into_inner()))
            }
        }
    }

    #[inline]
    pub fn send_sync(&self, message: T) ->std::result::Result<(), SendError<T>> {
        match self.inner.send(message) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError::ChannelDc(err.into_inner()))
            }
        }
    }

    #[inline]
    pub fn send_timeout(&self, message: T, timeout: Duration) -> std::result::Result<(), SendError<T>> {
        match self.inner.send_timeout(message, timeout){
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                Err(SendError::ChannelDc(err.into_inner()))
            }
        }
    }
}

impl<T> ChannelMixedRx<T> {
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T> {
        let inner = self.inner.recv_async();
        ChannelRxFut { inner }
    }

    #[inline]
    pub fn recv_sync(&self) -> Result<T> {
        match self.inner.recv() {
            Ok(elem) => {
                Ok(elem)
            }
            Err(err) => {
                Err(Error::simple_with_msg(ErrorKind::CommunicationChannelFlumeMpmc,
                                           format!("{:?}", err).as_str()))
            }
        }
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T> {
        match self.inner.recv_timeout(timeout) {
            Ok(elem) => {
                Ok(elem)
            }
            Err(err) => {
                Err(Error::simple_with_msg(ErrorKind::CommunicationChannelFlumeMpmc,
                                           format!("{:?}", err).as_str()))
            }
        }
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

pub fn new_bounded<T>(bound: usize) -> (ChannelMixedTx<T>, ChannelMixedRx<T>) {
    let (tx, rx) = flume::bounded(bound);

    (ChannelMixedTx { inner: tx }, ChannelMixedRx { inner: rx })
}