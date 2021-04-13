//! The consensus algorithm used for `febft` and other logic.

use std::marker::PhantomData;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};

use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::signature::Signature;
use crate::bft::history::LoggerHandle;
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    ConsensusMessage,
    ConsensusMessageKind,
};
//use crate::bft::collections::{
//    self,
//    HashSet,
//};
use crate::bft::communication::{
    Node,
    NodeId,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

/// Represents the status of calling `poll()` on a `TBOQueue`.
pub enum PollStatus {
    /// The `Replica` associated with this `TBOQueue` should
    /// poll its main channel for more messages.
    Recv,
    /// The `Replica` associated with this `TBOQueue` should
    /// propose a new client request to be ordered, if it is
    /// the leader, and then it should poll its main channel
    /// for more messages. Alternatively, if the request has
    /// already been decided, it should be queued for
    /// execution.
    TryProposeAndRecv,
    /// A new consensus message is available to be processed.
    NextMessage(Header, ConsensusMessage),
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
    pre_prepares: VecDeque<VecDeque<(Header, ConsensusMessage)>>,
    prepares: VecDeque<VecDeque<(Header, ConsensusMessage)>>,
    commits: VecDeque<VecDeque<(Header, ConsensusMessage)>>,
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

    fn pop_message(
        tbo: &mut VecDeque<VecDeque<(Header, ConsensusMessage)>>,
    ) -> Option<(Header, ConsensusMessage)> {
        if tbo.is_empty() {
            None
        } else {
            tbo[0].pop_front()
        }
    }

    fn queue_message(
        curr_seq: i32,
        tbo: &mut VecDeque<VecDeque<(Header, ConsensusMessage)>>,
        h: Header,
        m: ConsensusMessage,
    ) {
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
        tbo[index].push_back((h, m));
    }

    fn advance_message_queue(
        tbo: &mut VecDeque<VecDeque<(Header, ConsensusMessage)>>,
    ) {
        tbo.pop_front();
    }
}

macro_rules! extract_msg {
    ($g:expr, $q:expr) => {
        if let Some((header, message)) = Self::pop_message($q) {
            PollStatus::NextMessage(header, message)
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
            ProtoPhase::Init => PollStatus::TryProposeAndRecv,
            ProtoPhase::PrePreparing if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.pre_prepares)
            },
            ProtoPhase::Preparing(_) if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.prepares)
            },
            ProtoPhase::Committing(_) if self.get_queue => {
                extract_msg!(&mut self.get_queue, &mut self.commits)
            },
            _ => PollStatus::Recv,
        }
    }

    /// Reports the id of the consensus this `TBOQueue` is tracking.
    pub fn sequence_number(&self) -> i32 {
        self.curr_seq
    }

    /// Advances the message queue, and updates the consensus instance id.
    fn next_instance_queue(&mut self) {
        self.curr_seq += 1;
        Self::advance_message_queue(&mut self.pre_prepares);
        Self::advance_message_queue(&mut self.prepares);
        Self::advance_message_queue(&mut self.commits);
    }

    /// Queues a `PRE-PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_pre_prepare(&mut self, h: Header, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.pre_prepares, h, m)
    }

    /// Queues a `PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_prepare(&mut self, h: Header, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.prepares, h, m)
    }

    /// Queues a `COMMIT` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_commit(&mut self, h: Header, m: ConsensusMessage) {
        Self::queue_message(self.curr_seq, &mut self.commits, h, m)
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
    Preparing(usize),
    /// Running the `COMMIT` phase. The integer represents
    /// the number of votes received.
    Committing(usize),
}

/// Contains the state of an active consensus instance, as well
/// as future instances.
pub struct Consensus<S: Service> {
    phase: ProtoPhase,
    tbo: TBOQueue,
    current: Option<Signature>,
    //voted: HashSet<NodeId>,
    _phantom: PhantomData<S>,
}

/// Status returned from processing a consensus message.
pub enum ConsensusStatus {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// A `febft` quorum decided on the execution of
    /// the request with the given `Signature`.
    Decided(Signature),
}

impl<S> Consensus<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Starts a new consensus protocol tracker.
    pub fn new(initial_seq_no: i32) -> Self {
        Self {
            _phantom: PhantomData,
            phase: ProtoPhase::Init,
            tbo: TBOQueue::new(initial_seq_no),
            //voted: collections::hash_set(),
            current: None,
        }
    }

    /// Proposes a new request with signature `sig`.
    ///
    /// This function will only succeed if the `node` is
    /// the leader of the current `view` and the `node` is
    /// in the phase `ProtoPhase::Init`.
    pub fn propose(&mut self, sig: Signature, view: ViewInfo, node: &mut Node<S::Data>) {
        match self.phase {
            ProtoPhase::Init => self.phase = ProtoPhase::PrePreparing,
            _ => return,
        }
        if node.id() != view.leader() {
            return;
        }
        let message = SystemMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            ConsensusMessageKind::PrePrepare(sig),
        ));
        let targets = NodeId::targets(0..view.params().n());
        node.broadcast(message, targets);
    }

    /// Returns the current protocol phase.
    pub fn phase(&self) -> ProtoPhase {
        self.phase
    }

    /// Check if we can process new consensus messages.
    pub fn poll(&mut self) -> PollStatus {
        self.tbo.poll_queue(self.phase)
    }

    /// Starts a new consensus instance.
    pub fn next_instance(&mut self) {
        self.tbo.next_instance_queue();
        //self.voted.clear();
    }

    /// Process a message for a particular consensus instance.
    pub fn process_message(
        &mut self,
        header: Header,
        message: ConsensusMessage,
        view: ViewInfo,
        _log: &mut LoggerHandle<Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> ConsensusStatus {
        // FIXME: make sure a replica doesn't vote twice
        // by keeping track of who voted, and not just
        // the amount of votes received
        // FIXME: log the message
        match self.phase {
            ProtoPhase::Init => {
                // in the init phase, we can't do anything,
                // queue the message for later
                match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Prepare => {
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Commit => {
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    },
                }
            },
            ProtoPhase::PrePreparing => {
                // queue message if we're not pre-preparing
                // or in the same seq as the message
                self.current = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) if message.sequence_number() != self.sequence_number() => {
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::PrePrepare(sig) => {
                        Some(sig.clone())
                    },
                    ConsensusMessageKind::Prepare => {
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Commit => {
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    },
                };
                // leader can't vote for a prepare
                if node.id() != view.leader() {
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        self.sequence_number(),
                        ConsensusMessageKind::Prepare,
                    ));
                    let targets = NodeId::targets(0..view.params().n());
                    node.broadcast(message, targets);
                }
                // enter preparing phase
                self.phase = ProtoPhase::Preparing(0);
                ConsensusStatus::Deciding
            },
            ProtoPhase::Preparing(i) => {
                // queue message if we're not preparing
                // or in the same seq as the message
                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Prepare if message.sequence_number() != self.sequence_number() => {
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Prepare => i + 1,
                    ConsensusMessageKind::Commit => {
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    },
                };
                // check if we have gathered enough votes,
                // and transition to a new phase
                self.phase = if i == view.params().quorum() {
                    let message = SystemMessage::Consensus(ConsensusMessage::new(
                        self.sequence_number(),
                        ConsensusMessageKind::Commit,
                    ));
                    let targets = NodeId::targets(0..view.params().n());
                    node.broadcast(message, targets);
                    ProtoPhase::Committing(0)
                } else {
                    ProtoPhase::Preparing(i)
                };
                ConsensusStatus::Deciding
            },
            ProtoPhase::Committing(i) => {
                // queue message if we're not committing
                // or in the same seq as the message
                let i = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        self.queue_pre_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Prepare => {
                        self.queue_prepare(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Commit if message.sequence_number() != self.sequence_number() => {
                        self.queue_commit(header, message);
                        return ConsensusStatus::Deciding;
                    },
                    ConsensusMessageKind::Commit => i + 1,
                };
                // check if we have gathered enough votes,
                // and transition to a new phase
                if i == view.params().quorum() {
                    // we have reached a decision,
                    // notify core protocol
                    self.phase = ProtoPhase::Init;
                    let sig = self.current.take().unwrap();
                    ConsensusStatus::Decided(sig)
                } else {
                    self.phase = ProtoPhase::Committing(i);
                    ConsensusStatus::Deciding
                }
            },
        }
    }
}

impl<S> Deref for Consensus<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    type Target = TBOQueue;

    #[inline]
    fn deref(&self) -> &TBOQueue {
        &self.tbo
    }
}

impl<S> DerefMut for Consensus<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut TBOQueue {
        &mut self.tbo
    }
}
