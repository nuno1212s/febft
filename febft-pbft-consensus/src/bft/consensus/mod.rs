pub mod decision;
pub mod accessory;

use std::collections::VecDeque;
use std::iter;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use chrono::Utc;
use either::Either;
use log::{debug, warn};
use socket2::Protocol;
use febft_common::error::*;
use febft_common::crypto::hash::Digest;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{InvalidSeqNo, Orderable, SeqNo, tbo_advance_message_queue, tbo_advance_message_queue_return, tbo_queue_message};
use febft_communication::message::{Header, StoredMessage};
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::messages::{RequestMessage, SystemMessage};
use febft_messages::serialize::StateTransferMessage;
use febft_messages::timeouts::Timeouts;
use crate::bft::msg_log::Info;
use crate::bft::consensus::decision::{ConsensusDecision, MessageQueue};
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::deciding_log::{CompletedBatch, DecidingLog};
use crate::bft::msg_log::decisions::{DecisionLog, IncompleteProof, Proof};
use crate::bft::{PBFT, SysMsg};
use crate::bft::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::sync::view::ViewInfo;

#[derive(Debug, Clone)]
/// Status returned from processing a consensus message.
pub enum ConsensusStatus {
    /// A particular node tried voting twice.
    VotedTwice(NodeId),
    /// A `febft` quorum still hasn't made a decision
    /// on a client request to be executed.
    Deciding,
    /// A `febft` quorum decided on the execution of
    /// the batch of requests with the given digests.
    /// The first digest is the digest of the Prepare message
    /// And therefore the entire batch digest
    /// THe second Vec<Digest> is a vec with digests of the requests contained in the batch
    /// The third is the messages that should be persisted for this batch to be considered persisted
    Decided,
}

#[derive(Debug, Clone)]
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
    /// The first consensus instance of the consensus queue is ready to be finalized
    /// as it has already been decided
    Decided,
}

/// Represents a queue of messages to be ordered in a consensus instance.
///
/// Because of the asynchronicity of the Internet, messages may arrive out of
/// context, e.g. for the same consensus instance, a `PRE-PREPARE` reaches
/// a node after a `PREPARE`. A `TboQueue` arranges these messages to be
/// processed in the correct order.
pub struct TboQueue<O> {
    curr_seq: SeqNo,
    watermark: u32,
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
    fn new(curr_seq: SeqNo, watermark: u32) -> Self {
        Self {
            curr_seq,
            watermark,
            get_queue: false,
            pre_prepares: VecDeque::new(),
            prepares: VecDeque::new(),
            commits: VecDeque::new(),
        }
    }

    fn base_seq(&self) -> SeqNo {
        self.curr_seq + SeqNo::from(self.watermark)
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// consensus messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    fn advance_queue(&mut self) -> MessageQueue<O> {
        self.curr_seq = self.curr_seq.next();

        let pre_prepares = tbo_advance_message_queue_return(&mut self.pre_prepares)
            .unwrap_or_else(|| VecDeque::new());
        let prepares = tbo_advance_message_queue_return(&mut self.prepares)
            .unwrap_or_else(|| VecDeque::new());
        let commits = tbo_advance_message_queue_return(&mut self.commits)
            .unwrap_or_else(|| VecDeque::new());

        MessageQueue::from_messages(pre_prepares, prepares, commits)
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
            self.base_seq(),
            &mut self.pre_prepares,
            StoredMessage::new(h, m),
        )
    }

    /// Queues a `PREPARE` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_prepare(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.base_seq(), &mut self.prepares, StoredMessage::new(h, m))
    }

    /// Queues a `COMMIT` message for later processing, or drops it
    /// immediately if it pertains to an older consensus instance.
    fn queue_commit(&mut self, h: Header, m: ConsensusMessage<O>) {
        tbo_queue_message(self.base_seq(), &mut self.commits, StoredMessage::new(h, m))
    }

    fn clear(&mut self) {
        self.get_queue = false;
        self.pre_prepares.clear();
        self.prepares.clear();
        self.commits.clear();
    }
}

/// The consensus handler. Responsible for multiplexing consensus instances and keeping track
/// of missing messages
pub struct Consensus<D: SharedData + 'static, ST: StateTransferMessage + 'static> {
    node_id: NodeId,
    /// The handle to the executor of the function
    executor_handle: ExecutorHandle<D>,
    /// How many consensus instances can we overlap at the same time.
    watermark: u32,
    /// The current seq no that we are currently in
    seq_no: SeqNo,
    /// The consensus instances that are currently being processed
    /// A given consensus instance n will only be finished when all consensus instances
    /// j, where j < n have already been processed, in order to maintain total ordering
    decisions: VecDeque<ConsensusDecision<D, ST>>,
    /// The queue for messages that sit outside the range seq_no + watermark
    /// These messages cannot currently be processed since they sit outside the allowed
    /// zone but they will be processed once the seq no moves forward enough to include them
    tbo_queue: TboQueue<D::Request>,
}

impl<D, ST> Consensus<D, ST> where D: SharedData + 'static,
                                   ST: StateTransferMessage + 'static {
    pub fn new_replica(node_id: NodeId, view: &ViewInfo, executor_handle: ExecutorHandle<D>, seq_no: SeqNo, watermark: u32) -> Self {
        let mut decision_deque = VecDeque::with_capacity(watermark as usize);

        for i in 0..watermark {
            let seq_add = SeqNo::from(i);

            let seq_no = seq_no + seq_add;

            let decision = ConsensusDecision::init_decision(
                node_id,
                seq_no,
                view,
            );

            decision_deque.push_back(decision);
        }

        Self {
            node_id,
            executor_handle,
            watermark,
            seq_no,
            decisions: decision_deque,
            tbo_queue: TboQueue::new(seq_no, watermark),
        }
    }

    pub fn queue(&mut self, header: Header, message: ConsensusMessage<D::Request>) {
        let i = match message.sequence_number().index(self.seq_no) {
            Either::Right(i) => i,
            Either::Left(_) => {
                return;
            }
        };

        if i >= self.decisions.len() {
            // We are not currently processing this consensus instance
            // so we need to queue the message
            self.tbo_queue.queue(header, message);
        } else {
            // Queue the message in the corresponding pending decision
            self.decisions.get_mut(i).unwrap().queue(header, message);
        }
    }

    pub fn poll(&mut self) -> ConsensusPollStatus<D::Request> {
        for ind in 0..self.decisions.len() {
            match self.decisions[ind].poll() {
                ConsensusPollStatus::NextMessage(header, message) => {
                    return ConsensusPollStatus::NextMessage(header, message);
                }
                _ => {}
            }
        }

        ConsensusPollStatus::Recv
    }

    pub fn process_message<NT>(&mut self,
                               header: Header,
                               message: ConsensusMessage<D::Request>,
                               synchronizer: &Synchronizer<D>,
                               timeouts: &Timeouts,
                               log: &mut Log<D>,
                               node: &NT) -> Result<ConsensusStatus>
        where NT: Node<PBFT<D, ST>> {
        let i = match message.sequence_number().index(self.seq_no) {
            Either::Right(i) => i,
            Either::Left(_) => {
                // FIXME: maybe notify peers if we detect a message
                // with an invalid (too large) seq no? return the
                // `NodeId` of the offending node.
                //
                // NOTE: alternatively, if this seq no pertains to consensus,
                // we can try running the state transfer protocol
                warn!("Message is behind our current sequence no {:?}", self.seq_no, );
                return Ok(ConsensusStatus::Deciding);
            }
        };

        if i >= self.decisions.len() {
            // We are not currently processing this consensus instance
            // so we need to queue the message
            self.tbo_queue.queue(header, message);

            return Ok(ConsensusStatus::Deciding);
        }

        // Get the correct consensus instance for this message
        let decision = self.decisions.get_mut(i).unwrap();

        decision.process_message(header, message, synchronizer, timeouts, log, node)
    }

    /// Are we able to finalize the next consensus instance on the queue?
    pub fn can_finalize(&self) -> bool {
        self.decisions.front().map(|d| d.is_finalizeable()).unwrap_or(false)
    }

    /// Finalize the next consensus instance if possible
    pub fn finalize(&mut self, view: &ViewInfo) -> Result<Option<CompletedBatch<D::Request>>> {

        // If the decision can't be finalized, then we can't finalize the batch
        if let Some(decision) = self.decisions.front() {
            if !decision.is_finalizeable() {
                return Ok(None);
            }
        } else {
            // This should never happen?
            return Ok(None);
        }

        // Move to the next instance of the consensus since the current one is going to be finalized
        let decision = self.next_instance(view);

        let batch = decision.finalize()?;

        Ok(Some(batch))
    }

    /// Advance to the next instance of the consensus
    /// This will also create the necessary new decision to keep the pending decisions
    /// equal to the water mark
    pub fn next_instance(&mut self, view: &ViewInfo) -> ConsensusDecision<D, ST> {
        self.seq_no = self.seq_no.next();

        let decision = self.decisions.pop_front().unwrap();

        //Get the next message queue from the tbo queue. If there are no messages present
        // (expected during normal operations, then we will create a new message queue)
        let queue = self.tbo_queue.advance_queue();

        // Create the decision to keep the queue populated
        self.decisions.push_back(ConsensusDecision::init_with_msg_log(self.node_id,
                                                                      self.seq_no,
                                                                      view,
                                                                      queue, ));

        decision
    }

    /// Install the received state into the consensus
    pub fn install_state(&mut self, state: D::State,
                         view_info: ViewInfo,
                         dec_log: &DecisionLog<D::Request>) -> Result<(D::State, Vec<D::Request>)> {
        // get the latest seq no
        let seq_no = {
            let last_exec = dec_log.last_execution();
            if last_exec.is_none() {
                self.sequence_number()
            } else {
                last_exec.unwrap()
            }
        };

        // skip old messages
        self.install_sequence_number(seq_no.next(), &view_info);

        let mut reqs = Vec::with_capacity(dec_log.proofs().len());

        for proof in dec_log.proofs() {
            if !proof.are_pre_prepares_ordered()? {
                unreachable!()
            }

            for pre_prepare in proof.pre_prepares() {
                let x: &ConsensusMessage<D::Request> = pre_prepare.message();

                match x.kind() {
                    ConsensusMessageKind::PrePrepare(pre_prepare_reqs) => {
                        for req in pre_prepare_reqs {
                            let rq_msg: &RequestMessage<D::Request> = req.message();

                            reqs.push(rq_msg.operation().clone());
                        }
                    }
                    _ => { unreachable!() }
                }
            }
        }

        Ok((state, reqs))
    }

    pub fn install_sequence_number(&mut self, seq_no: SeqNo, view: &ViewInfo) {
        match self.seq_no.index(seq_no) {
            Either::Left(_) => {
                self.decisions.clear();

                let mut sequence_no = seq_no;

                while self.decisions.len() < self.watermark as usize {
                    self.decisions.push_back(ConsensusDecision::init_decision(self.node_id, sequence_no, view));

                    sequence_no = sequence_no + SeqNo::ONE;
                }

                self.tbo_queue.clear();

                self.tbo_queue.curr_seq = seq_no;
                self.seq_no = seq_no;
            }
            Either::Right(0) => {
                // We are in the correct sequence number
            }
            Either::Right(limit) => if limit >= self.decisions.len() {
                // We have more skips to do than currently watermarked decisions,
                // so we must clear all our decisions and then consume from the tbo queue
                // Until all decisions that have already been saved to the log are discarded of
                self.decisions.clear();

                let mut sequence_no = seq_no;

                let mut overflow = limit - self.decisions.len();

                if overflow >= self.tbo_queue.pre_prepares.len() {
                    // If we have more overflow than stored in the tbo queue, then
                    // We must clear the entire tbo queue and start fresh
                    self.tbo_queue.clear();

                    self.tbo_queue.curr_seq = seq_no;
                } else {
                    for _ in 0..overflow {
                        // Read the next overflow consensus instances and dispose of them
                        // As they have already been registered to the log
                        self.tbo_queue.next_instance_queue();
                    }
                }

                /// Get the next few already populated message queues from the tbo queue.
                /// This will also adjust the tbo queue sequence number to the correct one
                while self.tbo_queue.sequence_number() < seq_no && self.decisions.len() < self.watermark as usize {
                    let messages = self.tbo_queue.advance_queue();

                    let decision = ConsensusDecision::init_with_msg_log(self.node_id, sequence_no, view, messages);

                    self.decisions.push_back(decision);

                    sequence_no = sequence_no + SeqNo::ONE;
                }

                /// Populate the rest of the watermark decisions with empty consensus decisions
                while self.decisions.len() < self.watermark as usize {
                    let decision = ConsensusDecision::init_decision(self.node_id, sequence_no, view);

                    self.decisions.push_back(decision);

                    sequence_no = sequence_no + SeqNo::ONE;
                }

                self.seq_no = seq_no;
            }
            Either::Right(limit) => {
                for _ in 0..limit {
                    // Pop the decisions that have already been made
                    self.decisions.pop_front();
                }

                // The decision at the head of the list is now seq_no

                // Get the last decision in the decision queue.
                // The following new consensus decisions will have the sequence number of the last decision
                let mut sequence_no: SeqNo = self.decisions.back().unwrap().sequence_number().next();

                while self.decisions.len() < self.watermark as usize {
                    // We advanced [`limit`] sequence numbers on the decisions,
                    // so by advancing the tbo queue the missing decisions, we will
                    // Also advance [`limit`] sequence numbers on the tbo queue, which is the intended
                    // Behaviour
                    let messages = self.tbo_queue.advance_queue();

                    let decision = ConsensusDecision::init_with_msg_log(self.node_id, sequence_no,
                                                                        view, messages);

                    self.decisions.push_back(decision);

                    sequence_no = sequence_no + SeqNo::ONE;
                }

                self.seq_no = seq_no;
            }
        }

        self.tbo_queue.signal();
    }

    /// Catch up to the quorums latest decided consensus
    pub fn catch_up_to_quorum(&mut self,
                              seq: SeqNo,
                              view: &ViewInfo,
                              proof: Proof<D::Request>,
                              log: &mut Log<D>) -> Result<()> {

        // If this is successful, it means that we are all caught up and can now start executing the
        // batch
        let should_execute = log.install_proof(seq, proof)?;

        //TODO: Should we remove the requests that are in the proof from the pending request log?

        if let Some(to_execute) = should_execute {
            let (info, update, _) = to_execute.into();

            match info {
                Info::Nil => {
                    self.executor_handle.queue_update(update)
                }
                Info::BeginCheckpoint => {
                    self.executor_handle.queue_update_and_get_appstate(update)
                }
            }.unwrap();
        }

        // Move to the next instance as this one has been finalized
        self.next_instance(view);

        Ok(())
    }

    /// Create a fake `PRE-PREPARE`. This is useful during the view
    /// change protocol.
    pub fn forge_propose<K>(
        &self,
        requests: Vec<StoredMessage<RequestMessage<D::Request>>>,
        synchronizer: &K,
    ) -> SysMsg<D, ST>
        where
            K: AbstractSynchronizer<D>,
    {
        SystemMessage::from_protocol_message(PBFTMessage::Consensus(ConsensusMessage::new(
            self.sequence_number(),
            synchronizer.view().sequence_number(),
            ConsensusMessageKind::PrePrepare(requests),
        )))
    }

    /// Finalize the view change protocol
    pub fn finalize_view_change<NT>(
        &mut self,
        (header, message): (Header, ConsensusMessage<D::Request>),
        synchronizer: &Synchronizer<D>,
        timeouts: &Timeouts,
        log: &mut Log<D>,
        node: &NT,
    ) where NT: Node<PBFT<D, ST>> {
        let view = synchronizer.view();
        //Prepare the algorithm as we are already entering this phase

        self.decisions.clear();
        self.tbo_queue.clear();

        for _ in 0..self.watermark {
            self.decisions.push_back(ConsensusDecision::init_decision(self.node_id,
                                                                      self.seq_no, &view));
        }

        //TODO: when we finalize a view change, we want to treat the pre prepare request
        // As the only pre prepare, since it already has info provided by everyone in the network.
        // Therefore, this should go straight to the Preparing phase instead of waiting for
        // All the view's leaders.

        self.process_message(header, message, synchronizer, timeouts, log, node).unwrap();
    }

    /// Collect the incomplete proof that is currently being decided
    pub fn collect_incomplete_proof(&self, f: usize) -> IncompleteProof {
        if let Some(decision) = self.decisions.front() {
            decision.deciding(f)
        } else {
            unreachable!()
        }
    }
}

impl<D, ST> Orderable for Consensus<D, ST> where D: SharedData + 'static, ST: StateTransferMessage + 'static {
    fn sequence_number(&self) -> SeqNo {
        self.seq_no
    }
}

/// The consensus guard for handling when the proposer should propose and to which consensus instance
pub struct ConsensusGuard {
    can_propose: AtomicBool,
    seq_no_queue: Mutex<VecDeque<SeqNo>>
}

impl ConsensusGuard {

    pub fn can_propose(&self) -> bool {
        self.can_propose.load(Ordering::Relaxed)
    }

    pub fn next_seq_no(&self) -> Option<SeqNo> {
        self.seq_no_queue.lock().unwrap().pop_front()
    }
    
}