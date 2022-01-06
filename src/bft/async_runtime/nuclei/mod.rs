use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};

use crate::bft::error::*;

pub struct JoinHandle<T> {
    inner: ::nuclei::join_handle::JoinHandle<T>,
}

pub struct Runtime;

pub fn init(num_threads: usize) -> Result<Runtime> {
    std::env::set_var("BASTION_BLOCKING_THREADS", format!("{}", num_threads));
    Ok(Runtime)
}

impl Runtime {
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let inner = ::nuclei::spawn(future);
        JoinHandle { inner }
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        ::nuclei::block_on(future)
    }

    pub fn drive<F: Future>(&self, future: F) -> F::Output {
        ::nuclei::drive(future)
    }
}

impl<T: Send + 'static> Future for JoinHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map(Ok)
    }
}
