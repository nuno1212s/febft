//! The consensus algorithm used for `febft` and other logic.

use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use crate::bft::communication::message::{
    ConsensusMessage, ConsensusMessageKind, Header, StoredMessage,
};

use super::communication::NodeId;
use crate::bft::core::server::ViewInfo;
use crate::bft::crypto::hash::Digest;
use crate::bft::cst::RecoveryState;
use crate::bft::executable::{Request, Service, State};
use crate::bft::ordering::{tbo_advance_message_queue, tbo_queue_message, Orderable, SeqNo};

pub mod follower_consensus;
pub mod log;
pub mod replica_consensus;

/// Represents the status of calling `poll()` on a `Consensus`.
pub enum ConsensusPollStatus<O> {
    /// The `Replica` associated with this `Consensus` should
    /// poll its main channel for more messages.
    Recv,
    /// The `Replica` associated with this `Consensus` should
    /// propose a new client request to be ordered, if it is
    /// the leader, and then it should poll its main channel
    /// for more messages. Alternatively, if the request has
    /// already been decided, it should be queued for
    /// execution.
    TryProposeAndRecv,
    /// A new consensus message is available to be processed.
    NextMessage(Header, ConsensusMessage<O>),
}

impl<O> Debug for ConsensusPollStatus<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsensusPollStatus::Recv => {
                write!(f, "Recv")
            }
            ConsensusPollStatus::TryProposeAndRecv => {
                write!(f, "TryPropose and recv")
            }
            ConsensusPollStatus::NextMessage(_, _) => {
                write!(f, "Next message")
            }
        }
    }
}

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchronicity of the Internet, messages may arrive out of
/// context, e.g. for the same consensus instance, a `PRE-PREPARE` reaches
/// a node after a `PREPARE`. A `TboQueue` arranges these messages to be
/// processed in the correct order.
pub struct TboQueue<O> {
    curr_seq: SeqNo,
    get_queue: bool,
    pre_prepares: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
    prepares: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
    commits: VecDeque<VecDeque<StoredMessage<ConsensusMessage<O>>>>,
}

impl<O> Orderable for TboQueue<O> {
    /// Reports the id of the consensus this `TboQueue` is tracking.
    fn sequence_number(&self) -> SeqNo {
        self.curr_seq
    }
}

impl<O> TboQueue<O> {
    fn new(curr_seq: SeqNo) -> Self {
        Self {
            curr_seq,
            get_queue: false,
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// consensus messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    /// Returns the seqno of the next consensus instance
    fn next_instance_no_advance(&self) -> SeqNo {
        self.curr_seq.clone().next()
    }

    /// Advances the message queue, and updates the consensus instance id.
    fn next_instance_queue(&mut self) {
        self.curr_seq = self.curr_seq.next();
        tbo_advance_message_queue(&mut self.pre_prepares);
        tbo_advance_message_queue(&mut self.prepares);
        tbo_advance_message_queue(&mut self.commits);
    }

    /// Queues a consensus message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    pub fn queue(&mut self, h: Header, m: ConsensusMessage<O>) {
        match m.kind() {
            ConsensusMessageKind::PrePrepare(_) => self.queue_pre_prepare(h, m),
            ConsensusMessageKind::Prepare(_) => self.queue_prepare(h, m),
            ConsensusMessageKind::Commit(_) => self.queue_commit(h, m),
        }
    }

    /// Queues a `PRE-PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_pre_prepare(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(
            self.curr_seq,
            &mut self.pre_prepares,
            StoredMessage::new(h, m),
        )
    }

    /// Queues a `PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_prepare(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.curr_seq, &mut self.prepares, StoredMessage::new(h, m))
    }

    /// Queues a `COMMIT` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_commit(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.curr_seq, &mut self.commits, StoredMessage::new(h, m))
    }
}

/// Represents the current phase of the consensus protocol.
#[derive(Debug, Copy, Clone)]
enum ProtoPhase {
    /// Start of a new consensus instance.
    Init,
    /// Running the `PRE-PREPARE` phase.
    PrePreparing,
    /// A replica has accepted a `PRE-PREPARE` message, but
    /// it doesn't have the entirety of the requests it references
    /// in its log.
    PreparingRequests,
    /// Running the `PREPARE` phase. The integer represents
    /// the number of votes received.
    Preparing(usize),
    /// Running the `COMMIT` phase. The integer represents
    /// the number of votes received.
    Committing(usize),
}

#[derive(Clone)]
pub struct ConsensusGuard {
    //TODO: Make sure the consensus is locked during view changes and Csts
    consensus_lock: Arc<Mutex<(SeqNo, ViewInfo)>>,
    //Consensus atomic bool guard. Set to true when the consensus is locked,
    //Set to false when it is unlocked and the proposer can propose the new
    //Consensus instance
    consensus_guard: Arc<AtomicBool>,
}

impl ConsensusGuard {
    fn new(seq_no: SeqNo, view: ViewInfo) -> Self {
        Self {
            consensus_lock: Arc::new(Mutex::new((seq_no, view))),
            consensus_guard: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn consensus_info(&self) -> &Arc<Mutex<(SeqNo, ViewInfo)>> {
        &self.consensus_lock
    }

    pub fn consensus_guard(&self) -> &Arc<AtomicBool> {
        &self.consensus_guard
    }
}

/// Status returned from processing a consensus message.
pub enum ConsensusStatus<'a> {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    Decided(Digest, &'a [Digest]),
}

/// An abstract consensus trait.
/// Contains the base methods that are required on both followers and replicas
pub trait AbstractConsensus<S: Service> {
    fn sequence_number(&self) -> SeqNo;

    fn install_new_phase(&mut self, phase: &RecoveryState<State<S>, Request<S>>);
}
