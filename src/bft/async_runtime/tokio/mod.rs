use std::future::Future;

use crate::bft::error::*;

pub type JoinHandle<T> = ::tokio::task::JoinHandle<T>;

pub type Runtime = ::tokio::runtime::Runtime;

pub fn init(num_threads: usize) -> Result<Runtime> {
    ::tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads)
        .thread_name("tokio-worker")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .wrapped_msg(ErrorKind::AsyncRuntimeTokio, "Failed to build tokio runtime")
}

pub trait TokioDrive {
    fn drive<F: Future>(&self, future: F) -> F::Output;
}

impl TokioDrive for Runtime {
    #[inline(always)]
    fn drive<F: Future>(&self, future: F) -> F::Output {
        self.block_on(future)
    }
}
