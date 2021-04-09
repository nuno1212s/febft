//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};

use crate::bft::communication::message::ConsensusMessage;

/// Represents the status of calling `poll()` on a `TBOQueue`.
pub enum PollStatus {
    /// The `Replica` associated with this `TBOQueue` should
    /// poll its main channel for more messages.
    Recv,
    /// The `Replica` associated with this `TBOQueue` should
    /// propose a new client request to be ordered, if it is
    /// the leader.
    Propose,
    /// A new consensus message is available to be processed.
    NextMessage(ConsensusMessage),
}

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

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        if let Some(m) = Self::pop_message($q) {
            PollStatus::NextMessage(m)
        } else {
            *$g = false;
            PollStatus::Recv
        }
    }
}

// XXX: api
impl TBOQueue {
    fn new(curr_seq: i32) -> Self {
        Self::new_impl(curr_seq)
    }

    /// Signal this `TBOQueue` that it may be able to extract new
    /// consensus messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    /// Poll this `TBOQueue` for new consensus messages.
    fn poll_queue(&mut self, phase: ProtoPhase) -> PollStatus {
        match phase {
            ProtoPhase::Init => PollStatus::Propose,
            ProtoPhase::PrePreparing if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.pre_prepares)
            },
            ProtoPhase::Preparing(_) if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.prepares)
            },
            ProtoPhase::Commiting(_) if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.commits)
            },
            _ => PollStatus::Recv,
        }
    }

    /// Reports the id of the consensus this `TBOQueue` is tracking.
    pub fn tracking_instance(&self) -> i32 {
        self.curr_seq
    }

    /// Advances the message queue, and updates the consensus instance id.
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

/// Repreents the current phase of the consensus protocol.
#[derive(Debug, Copy, Clone)]
pub enum ProtoPhase {
    /// Start of a new consensus instance.
    Init,
    /// Running the `PRE-PREPARE` phase.
    PrePreparing,
    /// Running the `PREPARE` phase. The integer represents
    /// the number of votes received.
    Preparing(u32),
    /// Running the `COMMIT` phase. The integer represents
    /// the number of votes received.
    Commiting(u32),
}

/// Contains the state of an active consensus instance, as well
/// as future instances.
pub struct Consensus {
    phase: ProtoPhase,
    tbo: TBOQueue,
}

impl Consensus {
    /// Starts a new consensus protocol tracker.
    pub fn new() -> Self {
        Self {
            phase: ProtoPhase::Init,
            tbo: TBOQueue::new(0),
        }
    }

    /// Check if we can process new consensus messages.
    pub fn poll(&mut self) -> PollStatus {
        self.tbo.poll_queue(self.phase)
    }
}

impl Deref for Consensus {
    type Target = TBOQueue;

    fn deref(&self) -> &TBOQueue {
        &self.tbo
    }
}

impl DerefMut for Consensus {
    fn deref_mut(&mut self) -> &mut TBOQueue {
        &mut self.tbo
    }
}
