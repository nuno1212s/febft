use std::fmt;
use std::error;
use std::result;

/// Wrapper Result for the Rust standard library Result type.
// TODO: create an actual error type to use instead of ()
pub type Result<T> = result::Result<T, ()>;

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

pub use error_kind::ErrorKind;

mod error_kind {
    include!(concat!(env!("OUT_DIR"), "/error_kind.rs"));
}
