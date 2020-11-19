use std::fmt;
use std::error;

/// Wrapper Result for the Rust standard library Result type.
// TODO: create an actual error type to use instead of ()
pub type Result<T> = std::result::Result<T, ()>;

/// Includes a list of all the library software components.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ErrorKind {
    AsyncRuntime,
    Communication,
    Other,
}

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
