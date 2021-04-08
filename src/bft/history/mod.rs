//! Message history log and tools to make it persistent.

use std::collections::VecDeque;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::channel::{
    self,
    ChannelRx,
    ChannelTx,
    MessageChannelTx,
};
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    ConsensusMessageKind,
};

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log is full. We are ready to perform a
    /// garbage collection operation.
    Full,
}

enum LogOperation<O, R> {
    Insert(Header, SystemMessage<O, R>),
}

struct StoredMessage<O, R> {
    header: Header,
    message: SystemMessage<O, R>,
}

/// Represents a log of messages received by the BFT system.
pub struct Log<O, R> {
    pre_prepares: VecDeque<StoredMessage<O, R>>,
    prepares: VecDeque<StoredMessage<O, R>>,
    commits: VecDeque<StoredMessage<O, R>>,
    // others: ...
}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<O, R> Log<O, R> {
    /// Creates a new message log.
    pub fn new() -> Self {
        Self {
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    /// Replaces the current `Log` with an empty one, and returns
    /// the replaced instance.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, Log::new())
    }

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&mut self, header: Header, message: SystemMessage<O, R>) -> Info {
        let message = StoredMessage { header, message };
        if let SystemMessage::Consensus(ref m) = &message.message {
            match m.kind() {
                ConsensusMessageKind::PrePrepare(_) => self.pre_prepares.push_back(message),
                ConsensusMessageKind::Prepare => self.prepares.push_back(message),
                ConsensusMessageKind::Commit => self.commits.push_back(message),
            }
        }
        Info::Nil
    }
}

/// Represents a handle to the logger.
pub struct LoggerHandle<O, R> {
    my_tx: ChannelTx<LogOperation<O, R>>,
}

impl<O, R> LoggerHandle<O, R> {
    /// Adds a new `message` and its respective `header` to the log.
    pub async fn insert(&mut self, header: Header, message: SystemMessage<O, R>) -> Result<()> {
        self.my_tx.send(LogOperation::Insert(header, message)).await
    }
}

impl<O, R> Clone for LoggerHandle<O, R> {
    fn clone(&self) -> Self {
        let my_tx = self.my_tx.clone();
        Self { my_tx }
    }
}

/// Represents an async message logging task.
pub struct Logger<O, R> {
    // handle used to receive messages to be logged
    my_rx: ChannelRx<LogOperation<O, R>>,
    // handle to the master channel used by the `Replica`;
    // signals checkpoint messages
    system_tx: MessageChannelTx<O, R>,
    // the message log itself
    log: Log<O, R>,
}

impl<O, R> Logger<O, R> {
    // max no. of messages allowed in the channel
    const CHAN_BOUND: usize = 128;

    /// Spawns a new logging task into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(system_tx: MessageChannelTx<O, R>) -> LoggerHandle<O, R>
    where
        O: Send + 'static,
    {
        let log = Log::new();
        let (my_tx, my_rx) = channel::new_bounded(Self::CHAN_BOUND);
        let mut logger = Logger {
            my_rx,
            system_tx,
            log,
        };
        rt::spawn(async move {
            while let Ok(log_op) = logger.my_rx.recv().await {
                // TODO: add other log operations, namely garbage collection
                match log_op {
                    LogOperation::Insert(header, message) => {
                        logger.log.insert(header, message);
                    },
                }
            }
        });
        LoggerHandle { my_tx }
    }
}
