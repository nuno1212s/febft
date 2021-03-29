//! Message history log and tools to make it persistent.

use std::collections::VecDeque;

use futures::channel::mpsc;
use futures::stream::StreamExt;

use crate::bft::error::*;
use crate::bft::communication::channel::MessageChannelTx;
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    ConsensusMessageKind,
};

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

// TODO: garbage collect the log
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
    pub fn insert(&mut self, header: Header, message: SystemMessage<O>) {
        let message = StoredMessage { header, message };
        if let SystemMessage::Consensus(ref m) = &message.message {
            match m.kind {
                ConsensusMessageKind::PrePrepare(_) => self.pre_prepares.push_back(message),
                ConsensusMessageKind::Prepare => self.prepares.push_back(message),
                ConsensusMessageKind::Commit => self.commits.push_back(message),
            }
        }
    }
}

/// Represents an async message logging task.
pub struct Logger<O> {
    // handle used to receive messages to be logged
    my_rx: ChannelRx<(Header, SystemMessage)>,
    // handle to the master channel used by the `System`;
    // signals checkpoint messages
    system_tx: MessageChannelTx<O>,
    // the message log itself
    log: Log<O>,
}

impl<O> Logger<O> {
    pub fn new(tx: MessageChannelTx<O>) -> Self {
        unimplemented!()
    }
}
