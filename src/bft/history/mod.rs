//! Message history log and tools to make it persistent.

use std::collections::VecDeque;

use futures::channel::mpsc;
use futures::stream::StreamExt;

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

const CHAN_BOUND: usize = 128;

/// Information reported after a logging operation.
pub enum Info {
    /// Nothing to report.
    Nil,
    /// The log is full. We are ready to perform a
    /// garbage collection operation.
    Full,
}

enum LogOperation<O> {
    Insert(Header, SystemMessage<O>),
}

struct StoredMessage<O> {
    header: Header,
    message: SystemMessage<O>,
}

/// Represents a log of messages received by the BFT system.
pub struct Log<O> {
    pre_prepares: VecDeque<StoredMessage<O>>,
    prepares: VecDeque<StoredMessage<O>>,
    commits: VecDeque<StoredMessage<O>>,
    // others: ...
}

// TODO:
// - garbage collect the log
// - save the log to persistent storage
impl<O> Log<O> {
    /// Creates a new message log.
    pub fn new() -> Self {
        Self {
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    /// Adds a new `message` and its respective `header` to the log.
    pub fn insert(&mut self, header: Header, message: SystemMessage<O>) -> Info {
        let message = StoredMessage { header, message };
        if let SystemMessage::Consensus(ref m) = &message.message {
            match m.kind {
                ConsensusMessageKind::PrePrepare(_) => self.pre_prepares.push_back(message),
                ConsensusMessageKind::Prepare => self.prepares.push_back(message),
                ConsensusMessageKind::Commit => self.commits.push_back(message),
            }
        }
        Info::Nil
    }
}

/// Represents a handle to the logger.
pub struct LoggerHandle<O> {
    my_tx: ChannelTx<(Header, SystemMessage<O>)>,
}

impl<O> LoggerHandle<O> {
    /// Adds a new `message` and its respective `header` to the log.
    pub async fn insert(&mut self, header: Header, message: SystemMessage<O>) -> Result<()> {
        self.my_tx.send(LogOperation::Insert(header, message)).await;
    }
}

impl<O> Clone for LoggerHandle<O> {
    fn clone(&self) -> Self {
        let my_tx = self.my_tx.clone();
        Self { my_tx }
    }
}

/// Represents an async message logging task.
pub struct Logger<O> {
    // handle used to receive messages to be logged
    my_rx: ChannelRx<LogOperation<O>>,
    // handle to the master channel used by the `System`;
    // signals checkpoint messages
    system_tx: MessageChannelTx<O>,
    // the message log itself
    log: Log<O>,
}

impl<O> Logger<O> {
    /// Spawns a new logging task into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(system_tx: MessageChannelTx<O>) -> LoggerHandle<O> {
        let log = Log::new();
        let (my_tx, my_rx) = channel::new_bounded(CHAN_BOUND);
        let mut logger = Logger {
            my_rx,
            system_tx,
            log,
        };
        rt::spawn(async move {
            loop {
                // TODO: add other log operations, namely garbage collection
                match logger.my_rx.recv() {
                    LogOperation::Insert(header, message) => {
                        logger.log.insert(header, message);
                    },
                }
            }
        });
        LoggerHandle { my_tx }
    }
}
