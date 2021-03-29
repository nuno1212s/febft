//! Message history log and tools to make it persistent.

use std::collections::VecDeque;

use futures::channel::mpsc;
use futures::stream::StreamExt;

use crate::bft::error::*;
use crate::bft::communication::channel::MessageChannelTx;
use crate::bft::communication::message::{
    Header,
    SystemMessage,
};

struct StoredMessage<O> {
    header: Header,
    message: SystemMessage<O>,
}

/// Represents a log of messages received by the BFT system.
// TODO: garbage collect the log
pub struct Log<O> {
    pre_prepares: VecDeque<StoredMessage<O>>,
    prepares: VecDeque<StoredMessage<O>>,
    commits: VecDeque<StoredMessage<O>>,
    // others: ...
}

/// Represents an async message logging task.
pub struct Logger<O> {
    // handle to the master channel used by the `System`
    system_tx: MessageChannelTx<O>,
    // the message log itself
    log: Log<O>,
}

//impl Logger {
//    pub fn new(tx: MessageChannelTx) -> Self {
//        ads
//    }
//}
