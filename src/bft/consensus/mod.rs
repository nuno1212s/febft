//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;

use crate::bft::communication::message::ConsensusMessage;

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchrony of the Internet, messages may arrive out of
/// context, e.g. for the same consensus instance, a `PRE-PREPARE` reaches
/// a node after a `PREPARE`. A `TBOQueue` arranges these messages to be
/// processed in the correct order.
pub struct TBOQueue {
    curr_seq: i32,
    pre_prepares: VecDeque<VecDeque<ConsensusMessage>>,
    prepares: VecDeque<VecDeque<ConsensusMessage>>,
    commits: VecDeque<VecDeque<ConsensusMessage>>,
}

// XXX: details
impl TBOQueue {
    fn new_impl(curr_seq: i32) -> Self {
        Self {
            curr_seq,
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
