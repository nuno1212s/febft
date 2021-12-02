use crate::bft::error::*;

pub type JoinHandle<T> = ::tokio::task::JoinHandle<T>;

// FIXME: if users call `tokio::spawn`, nothing will happen!
// we need to include an `EnterGuard<'static>` in the returned
// runtime type
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
