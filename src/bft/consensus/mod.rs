//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;

use crate::bft::error::*;
use crate::bft::communication::message::{
    ConsensusMessage,
    SystemMessage,
    Message,
};

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchrony of the Internet, messages may arrive out of
/// context, e.g. for the same consensus instance, a `PRE-PREPARE` reaches
/// a node after a `PREPARE`. A `TBOQueue` arranges these messages to be
/// processed in the correct order.
pub struct TBOQueue {
    curr_seq: i32,
    get_queue: bool,
    pre_prepares: VecDeque<VecDeque<ConsensusMessage>>,
    prepares: VecDeque<VecDeque<ConsensusMessage>>,
    commits: VecDeque<VecDeque<ConsensusMessage>>,
}

// XXX: details
impl TBOQueue {
    fn new_impl(curr_seq: i32) -> Self {
        Self {
            curr_seq,
            get_queue: false,
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    fn pop_message(tbo: &mut VecDeque<VecDeque<ConsensusMessage>>) -> Option<ConsensusMessage> {
        if tbo.is_empty() {
            None
        } else {
            tbo[0].pop_front()
        }
    }

    fn queue_message(curr_seq: i32, tbo: &mut VecDeque<VecDeque<ConsensusMessage>>, m: ConsensusMessage) {
        let index = m.sequence_number() - curr_seq;
        if index < 0 {
            // drop old messages
            return;
        }
        let index = index as usize;
        if index >= tbo.len() {
            let len = index - tbo.len() + 1;
            tbo.extend(std::iter::repeat_with(VecDeque::new).take(len));
        }
        tbo[index].push_back(m);
    }

    fn advance_message_queue(tbo: &mut VecDeque<VecDeque<ConsensusMessage>>) {
        tbo.pop_front();
    }
}

// XXX: api
impl TBOQueue {
    /// Creates a new instance of `TBOQueue`.
    ///
    /// The integer `curr_seq` represents the id of the currently
    /// running consensus instance.
    pub fn new(curr_seq: i32) -> Self {
        Self::new_impl(curr_seq)
    }

    //pub async fn next_message(
    //    &mut self,
    //    node: &mut Node,
    //    consensus: &Consensus,
    //) -> Result<Message> {
    //    match self.phase {
    //        ProtoPhase::End => return Err("System has shut down").wrapped(ErrorKind::Consensus),
    //        ProtoPhase::Init => {
    //            if let Some(request) = self.requests.pop_front() {
    //                if self.leader == self.node.id {
    //                    self.propose_value(request.value);
    //                }
    //                self.phase = ProtoPhase::PrePreparing;
    //            }
    //            let message = self.node.receive().await?;
    //            message
    //        },
    //        ProtoPhase::PrePreparing if get_queue => {
    //            if let Some(m) = pop_message(&mut self.tbo_pre_prepare) {
    //                Message::System(SystemMessage::Consensus(m))
    //            } else {
    //                get_queue = false;
    //                continue;
    //            }
    //        },
    //        ProtoPhase::Preparing(_) if get_queue => {
    //            if let Some(m) = pop_message(&mut self.tbo_prepare) {
    //                Message::System(SystemMessage::Consensus(m))
    //            } else {
    //                get_queue = false;
    //                continue;
    //            }
    //        },
    //        ProtoPhase::Commiting(_) if get_queue => {
    //            if let Some(m) = pop_message(&mut self.tbo_commit) {
    //                Message::System(SystemMessage::Consensus(m))
    //            } else {
    //                get_queue = false;
    //                continue;
    //            }
    //        },
    //        _ => {
    //            let message = self.node.receive().await?;
    //            message
    //        },
    //    }
    //}

    /// Advances the message queues, and updates the consensus instance id.
    pub fn next_instance(&mut self) {
        self.curr_seq += 1;
        Self::advance_message_queue(&mut self.pre_prepares);
        Self::advance_message_queue(&mut self.prepares);
        Self::advance_message_queue(&mut self.commits);
    }

    /// Queues a `PRE-PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    pub fn queue_pre_prepare(&mut self, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.pre_prepares, m)
    }

    /// Queues a `PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    pub fn queue_prepare(&mut self, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.prepares, m)
    }

    /// Queues a `COMMIT` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    pub fn queue_commit(&mut self, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.commits, m)
    }
}

#[derive(Debug, Copy, Clone)]
enum ProtoPhase {
    Init,
    PrePreparing,
    Preparing(u32),
    Commiting(u32),
    Executing,
    End,
}

pub struct Consensus {
    phase: ProtoPhase,
    tbo: TBOQueue,
}
