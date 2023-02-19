//! A thread pool abstraction over a range of other crates. 

#[cfg(feature = "threadpool_crossbeam")]
mod crossbeam;

#[cfg(feature = "threadpool_cthpool")]
mod cthpool;

#[cfg(feature = "threadpool_rayon")]
mod rayon;



use crate::bft::globals::Global;
use crate::bft::error::*;

/// A thread pool type, used to run intensive CPU tasks.
///
/// The thread pool implements `Clone` with a cheap reference
/// count increase operation. This means that if we drop its
/// handle, the thread pool can continue to be used, as long
/// as at least another instance of the original pool remains.
//#[derive(Clone)]
pub struct ThreadPool {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::ThreadPool,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::ThreadPool,

    #[cfg(feature = "threadpool_rayon")]
    inner: rayon::ThreadPool,
}

/// Helper type used to construct a new thread pool.
pub struct Builder {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::Builder,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::Builder,

    #[cfg(feature = "threadpool_rayon")]
    inner: rayon::Builder,
}

impl Builder {
    /// Returns a new thread pool builder.
    pub fn new() -> Builder {
        let inner = {
            #[cfg(feature = "threadpool_crossbeam")]
            { crossbeam::Builder::new() }

            #[cfg(feature = "threadpool_cthpool")]
            { cthpool::Builder::new() }

            #[cfg(feature = "threadpool_rayon")]
            { rayon::Builder::new() }
        };
        Builder { inner}
    }
    /// Returns the handle to a new thread pool.
    pub fn build(self) -> ThreadPool {
        let inner = self.inner.build();

        let thread_pool = ThreadPool { inner };

        thread_pool

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

///We use two separate thread pools because these are mostly used to respond/send messages.
///Therefore, if we used the same threadpool for sending messages to replicas and to clients,
///We could get a situation where client responding would flood the threadpool and cause much larger latency
/// On the requests that are meant for the other replicas, leading to possibly much worse performance
/// By splitting these up we are assuring that does not happen as frequently at least
///
/// UPDATE: This is no longer the case given that we no longer use threadpools for message sending, just
/// for signing and serializing them
static mut POOL: Global<ThreadPool> = Global::new();

macro_rules! pool {
    () => {
        match unsafe { POOL.get() } {
	        Some(ref pool) => pool,
            None => panic!("Client thread pool wasn't initialized"),
        }
    }
}

/// This function initializes the thread pools.
///
/// It should be called once before the core protocol starts executing.
pub unsafe fn init(num_threads: usize) -> Result<()> {
    let replica_pool = Builder::new()
        .num_threads(num_threads)
        .build();

    POOL.set(replica_pool);
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
    pool!().join();
}
