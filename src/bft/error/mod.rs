use std::fmt;
use std::error;
use std::result;

pub trait ResultExt {
    type T;
    type E: Into<Box<dyn error::Error + Send + Sync>>;

    fn wrap_as(self, kind: ErrorKind, msg: &str) -> Result<Self::T>;
}

impl<T, E> ResultExt for result::Result<T, E>
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    type T = T;
    type E = E;

    fn wrap_as(self, kind: ErrorKind, msg: &str) -> Result<Self::T> {
        self.map_err(|e| Error::wrapped(kind, format!("{}: {}", msg, e.into())))
    }
}

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
    pub fn simple(kind: ErrorKind) -> Self {
        let inner = ErrorInner::Simple(kind);
        Error { inner }
    }

    pub fn wrapped<E>(kind: ErrorKind, e: E) -> Self
    where
        E: Into<Box<dyn error::Error + Send + Sync>>,
    {
        let inner = ErrorInner::Wrapped(kind, e.into());
        Error { inner }
    }

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

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            ErrorInner::Simple(k) => write!(fmt, "{:?}", k),
            ErrorInner::Wrapped(k, e) => write!(fmt, "{:?}: {}", k, e),
        }
    }
}

impl error::Error for Error {}

pub use error_kind::ErrorKind;

mod error_kind {
    include!(concat!(env!("OUT_DIR"), "/error_kind.rs"));
}
