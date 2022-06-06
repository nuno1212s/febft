use std::ops::Deref;

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