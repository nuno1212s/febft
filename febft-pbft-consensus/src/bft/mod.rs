//! This module contains the implementation details of `febft`.
//!
//! By default, it is hidden to the user, unless explicitly enabled
//! with the feature flag `expose_impl`.

pub mod consensus;
pub mod cst;
pub mod proposer;
pub mod sync;
pub mod timeouts;
pub mod msg_log;
pub mod config;
pub mod message;
pub mod observer;
pub mod follower;

use std::ops::Drop;

use log::{debug, LevelFilter};
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Logger, Root},
    Config,
    encode::pattern::PatternEncoder,
};
use febft_common::error::*;
use febft_common::{async_runtime, socket, threadpool};
use febft_common::globals::Flag;
use febft_communication::serialize::Serializable;
use febft_messages::serialize::System;
use crate::bft::message::serialize::PBFTConsensus;

pub type PBFT<D> = System<D, PBFTConsensus<D>>;
pub type SysMsg<D> = <PBFT<D> as Serializable>::Message;

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

    socket::init()?;
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
    socket::drop()?;
    Ok(())
}

