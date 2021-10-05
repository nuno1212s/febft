//! A thread pool abstraction over a range of other crates. 

#[cfg(feature = "threadpool_crossbeam")]
mod crossbeam;

#[cfg(feature = "threadpool_cthpool")]
mod cthpool;

use crate::bft::globals::Global;
use crate::bft::error::*;

/// A thread pool type, used to run intensive CPU tasks.
///
/// The thread pool implements `Clone` with a cheap reference
/// count increase operation. This means that if we drop its
/// handle, the thread pool can continue to be used, as long
/// as at least another instance of the original pool remains.
#[derive(Clone)]
pub struct ThreadPool {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::ThreadPool,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::ThreadPool,
}

/// Helper type used to construct a new thread pool.
pub struct Builder {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::Builder,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::Builder,
}

impl Builder {
    /// Returns a new thread pool builder.
    pub fn new() -> Builder {
        let inner = {
            #[cfg(feature = "threadpool_crossbeam")]
            { crossbeam::Builder::new() }

            #[cfg(feature = "threadpool_cthpool")]
            { cthpool::Builder::new() }
        };
        Builder { inner }
    }

    /// Returns the handle to a new thread pool.
    pub fn build(self) -> ThreadPool {
        let inner = self.inner.build();
        ThreadPool { inner }
    }

    /// Configures the number of threads used by the thread pool.
    pub fn num_threads(self, num_threads: usize) -> Self {
        let inner = self.inner.num_threads(num_threads);
        Builder { inner }
    }

    // ...eventually add more options?
}

impl ThreadPool {
    /// Spawns a new job into the thread pool.
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.execute(job)
    }

    /// Synchronously waits for all the jobs queued in the pool
    /// to complete.
    pub fn join(&self) {
        self.inner.join()
    }
}

static mut POOL: Global<ThreadPool> = Global::new();

macro_rules! pool {
    () => {
        match unsafe { POOL.get() } {
	        Some(ref pool) => pool,
            None => panic!("Global thread pool wasn't initialized"),
        }
    }
}

/// This function initializes the global thread pool.
///
/// It should be called once before the core protocol starts executing.
pub unsafe fn init(num_threads: usize) -> Result<()> {
    let pool = Builder::new()
        .num_threads(num_threads)
        .build();
    POOL.set(pool);
    Ok(())
}

/// This function drops the global thread pool.
///
/// It shouldn't be needed to be called manually called, as the
/// `InitGuard` should take care of calling this.
pub unsafe fn drop() -> Result<()> {
    POOL.drop();
    Ok(())
}

/// Spawns a new job into the global thread pool.
pub fn execute<F>(job: F)
where
    F: FnOnce() + Send + 'static,
{
    pool!().execute(job)
}

/// Synchronously waits for all the jobs queued in the
/// global thread pool to complete.
pub fn join() {
    pool!().join()
}
