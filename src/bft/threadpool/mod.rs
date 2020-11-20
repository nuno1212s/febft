#[cfg(feature = "threadpool_crossbeam")]
mod crossbeam;

pub struct ThreadPool {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::ThreadPool,
}
