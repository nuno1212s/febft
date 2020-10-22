/// Wrapper Result for the Rust standard library Result type.
// TODO: create an actual error type to use instead of ()
pub type Result<T> = std::result::Result<T, ()>;
