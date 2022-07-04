use rayon::ThreadPoolBuilder;

pub struct ThreadPool {
    inner: rayon::ThreadPool,
}

impl ThreadPool {
    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.inner.spawn(job)
    }

    pub fn join(&self) {
        // no-op
    }
}

pub struct Builder {
    threads: Option<usize>,
}

impl Builder {
    pub fn new() -> Builder {
        Builder { threads: None }
    }

    pub fn build(self) -> ThreadPool {
        let inner = match self.threads {
            Some(n) => ThreadPoolBuilder::new().num_threads(n),
            None => ThreadPoolBuilder::new(),
        }.build().unwrap();

        ThreadPool { inner }
    }

    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.threads = Some(num_threads);
        self
    }
}
