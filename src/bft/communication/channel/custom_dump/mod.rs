use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use dsrust::channels::async_ch::{ReceiverFut, ReceiverMultFut};
use dsrust::channels::queue_channel::{Receiver, ReceiverMult, Sender};
use dsrust::queues::rooms_array_queue::LFBRArrayQueue;
use futures::future::FusedFuture;

use crate::bft::error::*;

pub struct ChannelTx<T> where {
    inner: Sender<T, LFBRArrayQueue<T>>,
}

pub struct ChannelRx<T> where
{
    inner: Receiver<T, LFBRArrayQueue<T>>,
}

pub struct ChannelRxFut<'a, T> where {
    inner: ReceiverFut<'a, T, LFBRArrayQueue<T>>,
}

pub struct ChannelRxMult<T> where
{
    inner: ReceiverMult<T, LFBRArrayQueue<T>>,
}

pub struct ChannelRxMultFut<'a, T> where
{
    inner: ReceiverMultFut<'a, T, LFBRArrayQueue<T>>,
}

impl<T> ChannelTx<T> where {
    #[inline]
    pub async fn send(&self, message: T) -> Result<()> {
        match self.inner.send_async(message).await {
            Ok(_) => { Ok(()) }
            Err(_) => { Err(Error::simple(ErrorKind::CommunicationChannelFlumeMpmc)) }
        }
    }

    #[inline]
    pub fn send_blk(&self, message: T) -> Result<()> {
        match self.inner.send(message) {
            Ok(_) => { Ok(()) }
            Err(_) => { Err(Error::simple(ErrorKind::CommunicationChannelFlumeMpmc)) }
        }
    }
}

impl<T> ChannelRx<T> where {
    ///Async receiver with no backoff (Turns straight to event notifications)
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxFut<'a, T> {
        let inner = self.inner.recv_fut();

        ChannelRxFut { inner }
    }
}

impl<T> ChannelRxMult<T> {
    ///Async receiver with no backoff (Turns straight to event notifications)
    #[inline]
    pub fn recv<'a>(&'a mut self) -> ChannelRxMultFut<'a, T> {
        let inner = self.inner.recv_fut();

        ChannelRxMultFut { inner }
    }
}

impl<T> Clone for ChannelRx<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<T> Clone for ChannelTx<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<T> Clone for ChannelRxMult<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

impl<'a, T> Future for ChannelRxFut<'a, T> {
    type Output = Result<T>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|opt| opt.ok().ok_or(Error::simple(ErrorKind::CommunicationChannelFlumeMpmc)))
    }
}

impl<'a, T> FusedFuture for ChannelRxFut<'a, T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

///Receiver to use with the dump method
impl<'a, T> Future for ChannelRxMultFut<'a, T> {
    type Output = Result<Vec<T>>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|opt| opt.ok().ok_or(Error::simple(ErrorKind::CommunicationChannelFlumeMpmc)))
    }
}

impl<'a, T> FusedFuture for ChannelRxMultFut<'a, T> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}

pub fn bounded_mult_channel<T>(bound: usize) -> (ChannelTx<T>, ChannelRxMult<T>) {
    let (tx, rx) = dsrust::channels::queue_channel::bounded_lf_room_queue(bound);

    let receiver = dsrust::channels::queue_channel::make_mult_recv_from(rx);

    (ChannelTx { inner: tx }, ChannelRxMult { inner: receiver })
}