#[cfg(feature = "expose_impl")]
pub mod bft;

#[cfg(not(feature = "expose_impl"))]
mod bft;

#[cfg(feature = "bench")]
pub mod bench;

// let library users have access to our result type
pub use bft::error::Result;
