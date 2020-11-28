#[cfg(feature = "async_runtime_tokio")]
mod tokio;

#[cfg(feature = "async_runtime_async_std")]
mod async_std;

use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};

use once_cell::sync::OnceCell;

use crate::bft::error::*;

#[cfg(feature = "async_runtime_tokio")]
static RUNTIME: OnceCell<tokio::Runtime> = OnceCell::new();

#[cfg(feature = "async_runtime_async_std")]
static RUNTIME: OnceCell<async_std::Runtime> = OnceCell::new();

pub struct JoinHandle<T> {
    #[cfg(feature = "async_runtime_tokio")]
    inner: tokio::JoinHandle<T>,

    #[cfg(feature = "async_runtime_async_std")]
    inner: async_std::JoinHandle<T>,
}

pub fn init(num_threads: usize) -> Result<()> {
    #[cfg(feature = "async_runtime_tokio")]
    {
        tokio::init(num_threads).and_then(|rt| {
            RUNTIME.set(rt)
                .simple_msg(ErrorKind::AsyncRuntime, "Failed to set global runtime instance")
        })
    }
    #[cfg(feature = "async_runtime_async_std")]
    {
        async_std::init(num_threads).and_then(|rt| {
            RUNTIME.set(rt)
                .simple_msg(ErrorKind::AsyncRuntime, "Failed to set global runtime instance")
        })
    }
}

pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match RUNTIME.get() {
        Some(ref rt) => {
            let inner = rt.spawn(future);
            JoinHandle { inner }
        },
        None => panic!("Async runtime wasn't initialized"),
    }
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    match RUNTIME.get() {
        Some(ref rt) => rt.block_on(future),
        None => panic!("Async runtime wasn't initialized"),
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(|result| result.wrapped_msg(ErrorKind::AsyncRuntime, "Failed to join handle"))
    }
}
