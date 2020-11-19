use std::fmt;
use std::error;
use std::result;

/// Wrapper Result for the Rust standard library Result type.
pub type Result<T> = result::Result<T, Error>;

/// The error type used throughout this crate.
pub struct Error {
    inner: ErrorInner,
}

#[derive(Debug)]
enum ErrorInner {
    Simple(ErrorKind),
    Wrapped(ErrorKind, Box<dyn error::Error + Send + Sync>),
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn kind(&self) -> ErrorKind {
        match &self.inner {
            ErrorInner::Simple(k) => *k,
            ErrorInner::Wrapped(k, _) => *k,
        }
    }

    pub fn swap_kind(self, k: ErrorKind) -> Self {
        let inner = match self.inner {
            ErrorInner::Simple(_) => ErrorInner::Simple(k),
            ErrorInner::Wrapped(_, e) => ErrorInner::Wrapped(k, e),
        };
        Error { inner }
    }
}

pub use error_kind::ErrorKind;

mod error_kind {
    include!(concat!(env!("OUT_DIR"), "/error_kind.rs"));
}
