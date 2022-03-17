//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

pub mod benchmarks;
pub mod async_runtime;
pub mod communication;
pub mod collections;
pub mod executable;
pub mod threadpool;
pub mod consensus;
pub mod ordering;
pub mod timeouts;
pub mod globals;
pub mod crypto;
pub mod error;
pub mod prng;
pub mod core;
pub mod sync;
pub mod cst;

use std::ops::Drop;

use error::*;
use globals::Flag;

static INITIALIZED: Flag = Flag::new();

/// Configure the init process of the library.
pub struct InitConfig {
    /// Number of threads used by the async runtime.
    pub async_threads: usize,
    /// Number of threads used by the global thread pool.
    pub pool_threads: usize,
}

/// Handle to the global data.
///
/// When dropped, the data is deinitialized.
pub struct InitGuard;

/// Initializes global data.
///
/// Should always be called before other methods, otherwise runtime
/// panics may ensue.
pub unsafe fn init(c: InitConfig) -> Result<Option<InitGuard>> {
    if INITIALIZED.test() {
        return Ok(None);
    }

    env_logger::init();

    threadpool::init(c.pool_threads)?;
    async_runtime::init(c.async_threads)?;

    println!("Async threads {}", c.async_threads);

    communication::socket::init()?;
    INITIALIZED.set();
    Ok(Some(InitGuard))
}

impl Drop for InitGuard {
    fn drop(&mut self) {
        unsafe { drop().unwrap() }
    }
}

unsafe fn drop() -> Result<()> {
    INITIALIZED.unset();
    threadpool::drop()?;
    async_runtime::drop()?;
    communication::socket::drop()?;
    Ok(())
}