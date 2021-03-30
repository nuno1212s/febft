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

use std::sync::atomic::{AtomicBool, Ordering};

use error::*;

static INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Configure the init process of the library.
pub struct InitConfig {
    /// Number of threads used by the async runtime.
    pub async_threads: usize,
}

/// Initializes global data.
///
/// Should always be called before other methods, otherwise runtime
/// panics may ensue.
pub fn init(c: InitConfig) -> Result<()> {
    if INITIALIZED.load(Ordering::Acquire) {
        return Ok(());
    }
    async_runtime::init(c.async_threads)?;
    INITIALIZED.store(true, Ordering::Release);
    Ok(())
}
