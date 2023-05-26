use std::ops::Deref;
use std::time::Duration;
use crossbeam_channel::{RecvTimeoutError, SendTimeoutError};
use crate::channel::{RecvError, SendError, TryRecvError, TrySendError};

pub struct ChannelSyncRx<T> {
    inner: crossbeam_channel::Receiver<T>,
}

pub struct ChannelSyncTx<T> {
    inner: crossbeam_channel::Sender<T>,
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

impl<T> ChannelSyncTx<T> {
    #[inline]
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.inner.send(value) {
            Ok(_) => { Ok(()) }
            Err(err) => {
                Err(SendError(err.into_inner()))
            }
        }
    }

    #[inline]
    pub fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), TrySendError<T>> {
        match self.inner.send_timeout(value, timeout) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                match err {
                    SendTimeoutError::Timeout(t) => {
                        Err(TrySendError::Timeout(t))
                    }
                    SendTimeoutError::Disconnected(t) => {
                        Err(TrySendError::Disconnected(t))
                    }
                }
            }
        }
    }

    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match self.inner.try_send(value) {
            Ok(_) => {
                Ok(())
            }
            Err(err) => {
                match err {
                    crossbeam_channel::TrySendError::Full(value) => {
                        Err(TrySendError::Full(value))
                    }
                    crossbeam_channel::TrySendError::Disconnected(value) => {
                        Err(TrySendError::Disconnected(value))
                    }
                }
            }
        }
    }
}

impl<T> ChannelSyncRx<T> {
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.inner.try_recv() {
            Ok(res) => {
                Ok(res)
            }
            Err(err) => {
                match err {
                    crossbeam_channel::TryRecvError::Empty => {
                        Err(TryRecvError::ChannelEmpty)
                    }
                    crossbeam_channel::TryRecvError::Disconnected => {
                        Err(TryRecvError::ChannelDc)
                    }
                }
            }
        }
    }

    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.inner.recv() {
            Ok(res) => {
                Ok(res)
            }
            Err(_err) => {
                Err(RecvError::ChannelDc)
            }
        }
    }

    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, TryRecvError> {
        match self.inner.recv_timeout(timeout) {
            Ok(result) => {
                Ok(result)
            }
            Err(err) => {
                match err {
                    RecvTimeoutError::Timeout => {
                        Err(TryRecvError::Timeout)
                    }
                    RecvTimeoutError::Disconnected => {
                        Err(TryRecvError::ChannelDc)
                    }
                }
            }
        }
    }
}

//TODO: Maybe make this actually implement the methods so we can return our own errors?
impl<T> Deref for ChannelSyncRx<T> {
    type Target = crossbeam_channel::Receiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Deref for ChannelSyncTx<T> {
    type Target = crossbeam_channel::Sender<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[inline]
pub fn new_bounded<T>(bound: usize) -> (ChannelSyncTx<T>, ChannelSyncRx<T>) {
    let (tx, rx) = crossbeam_channel::bounded(bound);

    (ChannelSyncTx { inner: tx }, ChannelSyncRx { inner: rx })
}