use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};

use crate::error::*;

pub struct JoinHandle<T> {
    inner: async_std::task::JoinHandle<T>,
}

pub struct Runtime;

pub fn init(num_threads: usize) -> Result<Runtime> {
    std::env::set_var("ASYNC_STD_THREAD_COUNT", format!("{}", num_threads));
    Ok(Runtime)
}

impl Runtime {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let inner = ::async_std::task::spawn(future);
        JoinHandle { inner }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        ::async_std::task::block_on(future)
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(Ok)
    }
}
