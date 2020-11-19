#[cfg(feature = "async_runtime_tokio")]
mod tokio;

use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};

use once_cell::sync::OnceCell;

use crate::bft::error::prelude::*;

#[cfg(feature = "async_runtime_tokio")]
static RUNTIME: OnceCell<tokio::Runtime> = OnceCell::new();

pub struct JoinHandle<T> {
    #[cfg(feature = "async_runtime_tokio")]
    inner: tokio::JoinHandle<T>,
}

pub fn init(num_threads: usize) -> Result<()> {
    #[cfg(feature = "async_runtime_tokio")]
    tokio::init(num_threads).and_then(|rt| {
        RUNTIME.set(rt)
            .simple_msg(ErrorKind::AsyncRuntime, "Failed to set global runtime instance")
    })
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|result| result.wrapped_msg(ErrorKind::AsyncRuntime, "Failed to join handle"))
    }
}
