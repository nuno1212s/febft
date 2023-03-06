

pub type ThreadPool = ::cthpool::arc::ThreadPool;

pub struct Builder {
    inner: ::cthpool::Builder,
}

impl Builder {
    pub fn new() -> Builder {
        let inner = ::cthpool::Builder::new();
        Builder { inner }
    }

    pub fn build(self) -> ThreadPool {
        self.inner
            .build()
            .into()
    }

    pub fn num_threads(self, num_threads: usize) -> Self {
        let inner = self.inner.num_threads(num_threads);
        Builder { inner }
    }
}
