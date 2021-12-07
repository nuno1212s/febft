use std::ops::Deref;
use std::future::Future;

use crate::bft::error::*;

pub type JoinHandle<T> = ::tokio::task::JoinHandle<T>;

pub struct Runtime {
    inner: ::tokio::runtime::Runtime,
    _enter: ::tokio::runtime::EnterGuard<'static>,
}

pub fn init(num_threads: usize) -> Result<Runtime> {
    ::tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .thread_name("tokio-worker")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .map(|inner| {
            let _enter: ::tokio::runtime::EnterGuard<'static> = unsafe {
                std::mem::transmute(inner.enter())
            };
            Runtime { inner, _enter }
        })
        .wrapped_msg(ErrorKind::AsyncRuntimeTokio, "Failed to build tokio runtime")
}

impl Runtime {
    pub fn spawn_named<F>(&self, name: &str, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        ::tokio::task::Builder::new()
            .name(name)
            .spawn(future)
    }
}

impl Deref for Runtime {
    type Target = ::tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
