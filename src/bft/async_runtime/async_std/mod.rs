use crate::bft::error::*;

pub type LocalSet = ::async_std::task::LocalSet;

pub type JoinHandle<T> = ::async_std::task::JoinHandle<T>;

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
        ::async_std::task::spawn(future)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        ::async_std::task::block_on(future)
    }
}
