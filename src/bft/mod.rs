//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

pub mod async_runtime;
pub mod communication;
pub mod threadpool;
pub mod history;
pub mod crypto;
pub mod error;

use error::*;

/// Configure the init process of the library.
pub struct InitConfig {
    pub async_threads: usize,
}

/// Initializes global data.
///
/// Should always be called before other methods, otherwise runtime
/// panics may ensue.
pub fn init(c: InitConfig) -> Result<()> {
    async_runtime::init(c.async_threads)?;
    Ok(())
}
