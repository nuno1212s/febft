//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

pub mod async_runtime;
pub mod benchmarks;
pub mod collections;
pub mod communication;
pub mod consensus;
pub mod core;
pub mod crypto;
pub mod cst;
pub mod error;
pub mod executable;
pub mod globals;
pub mod ordering;
pub mod persistentdb;
pub mod prng;
pub mod proposer;
pub mod sync;
pub mod threadpool;
pub mod timeouts;
pub mod msg_log;
pub mod config;

use std::ops::Drop;

use error::*;
use globals::Flag;
use log::{LevelFilter, debug};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Logger, Root},
    encode::pattern::PatternEncoder,
    Config,
};

static INITIALIZED: Flag = Flag::new();

/// Configure the init process of the library.
pub struct InitConfig {
    /// Number of threads used by the async runtime.
    pub async_threads: usize,
    /// Number of threads used by the thread pool.
    pub threadpool_threads: usize,

    ///Unique ID, used to specify the log file this replica should use
    pub id: Option<String>
}

/// Handle to the global data.
///
/// When dropped, the data is deinitialized.
#[repr(transparent)]
pub struct InitGuard;

/// Initializes global data.
///
/// Should always be called before other methods, otherwise runtime
/// panics may ensue.
pub unsafe fn init(c: InitConfig) -> Result<Option<InitGuard>> {
    if INITIALIZED.test() {
        return Ok(None);
    }

    let path = match c.id {
        Some(id) => {
            format!("./log/febft_{}.log", id)
        },
        None => {
            String::from("./log/febft.log")
        },
    };

    let appender = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{l} {d} - {m}{n}")))
        .build(path)
        .wrapped(ErrorKind::MsgLog)?;

    let config = Config::builder()
        .appender(Appender::builder().build("appender", Box::new(appender)))
        .logger(
            Logger::builder()
                .appender("appender")
                .additive(false)
                .build("app::appender", LevelFilter::Debug),
        )
        .build(
            Root::builder()
                .appender("appender")
                .build(LevelFilter::Debug),
        )
        .wrapped(ErrorKind::MsgLog)?;

    let _handle = log4rs::init_config(config).wrapped(ErrorKind::MsgLog)?;

    //tracing_subscriber::fmt::init();

    threadpool::init(c.threadpool_threads)?;
    async_runtime::init(c.async_threads)?;

    debug!("Async threads {}, sync threads {}", c.async_threads, c.threadpool_threads);

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
