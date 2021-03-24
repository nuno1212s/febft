//! A thread pool abstraction over a range of other crates. 

#[cfg(feature = "threadpool_crossbeam")]
mod crossbeam;

#[cfg(feature = "threadpool_cthpool")]
mod cthpool;

#[derive(Clone)]
pub struct ThreadPool {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::ThreadPool,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::ThreadPool,
}

pub struct Builder {
    #[cfg(feature = "threadpool_crossbeam")]
    inner: crossbeam::Builder,

    #[cfg(feature = "threadpool_cthpool")]
    inner: cthpool::Builder,
}

impl Builder {
    pub fn new() -> Builder {
        let inner = {
            #[cfg(feature = "threadpool_crossbeam")]
            { crossbeam::Builder::new() }

            #[cfg(feature = "threadpool_cthpool")]
            { cthpool::Builder::new() }
        };
        Builder { inner }
    }

    pub fn build(self) -> ThreadPool {
        let inner = self.inner.build();
        ThreadPool { inner }
    }

    pub fn num_threads(self, num_threads: usize) -> Self {
        let inner = self.inner.num_threads(num_threads);
        Builder { inner }
    }

    // ...eventually add more options?
}

impl ThreadPool {
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.execute(job)
    }

    pub fn join(&self) {
        self.inner.join()
    }
}
