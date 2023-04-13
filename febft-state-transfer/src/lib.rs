#![feature(inherent_associated_types)]

pub mod message;
pub mod config;


use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use febft_common::error::*;
use febft_common::globals::ReadOnly;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
use febft_common::collections;
use febft_common::collections::HashMap;
use febft_common::crypto::hash::Digest;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, NetworkMessageKind, StoredMessage};
use febft_communication::{Node};
use febft_execution::app::{Reply, Request, Service, State};
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::messages::{StateTransfer, SystemMessage};
use febft_messages::ordering_protocol::OrderingProtocol;
use febft_messages::serialize::{OrderingProtocolMessage, ServiceMsg, StatefulOrderProtocolMessage, StateTransferMessage, NetworkView};
use febft_messages::state_transfer::{Checkpoint, CstM, DecLog, StatefulOrderProtocol, StateTransferProtocol, STResult, ViewInfo};
use febft_messages::timeouts::Timeouts;
use crate::config::StateTransferConfig;

use crate::message::{CstMessage, CstMessageKind};
use crate::message::serialize::{CSTMsg};

enum ProtoPhase<S, V, O> {
    Init,
    WaitingCheckpoint(Header, CstMessage<S, V, O>),
    ReceivingCid(usize),
    ReceivingState(usize),
}

/// Contains state used by a recovering node.
///
/// Cloning this is better than it was because of the read only checkpoint,
/// and because decision log also got a lot easier to clone, but we still have
/// to be very careful thanks to the requests vector, which can be VERY large
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct RecoveryState<S, V, O> {
    pub checkpoint: Arc<ReadOnly<Checkpoint<S>>>,
    pub view: V,
    pub log: O,
}

/// Allow a replica to recover from the state received by peer nodes.
pub fn install_recovery_state<D, NT, OP>(
    recovery_state: RecoveryState<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>,
    order_protocol: &mut OP,
    executor: &mut ExecutorHandle<D>,
) -> Result<()>
    where
        D: SharedData + 'static,
        OP: StatefulOrderProtocol<D, NT>
{
    // TODO: maybe try to optimize this, to avoid clone(),
    // which may be quite expensive depending on the size
    // of the state and the amount of batched requests

    //Because we depend on this state to operate the consensus so it's not that bad that we block
    //Here, since we're not currently partaking in the consensus.
    //Also so the executor doesn't have to accept checkpoint types, which is kind of messing with the levels
    //of the architecture, so I think we can keep it this way

    // TODO: update pub/priv keys when reconfig is implemented?

    let RecoveryState {
        checkpoint,
        view,
        log
    } = recovery_state;

    let (state, req) = order_protocol.install_state(checkpoint, view, log)?;

    //Update the executor phase
    executor.install_state(state, req)?;

    Ok(())
}

impl<S, O, V> RecoveryState<S, V, O> {
    /// Creates a new `RecoveryState`.
    pub fn new(
        view: V,
        checkpoint: Arc<ReadOnly<Checkpoint<S>>>,
        declog: O,
    ) -> Self {
        Self {
            view,
            checkpoint,
            log: declog,
        }
    }

    /// Returns the view this `RecoveryState` is tracking.
    pub fn view(&self) -> &V {
        &self.view
    }

    /// Returns the local checkpoint of this recovery state.
    pub fn checkpoint(&self) -> &Arc<ReadOnly<Checkpoint<S>>> {
        &self.checkpoint
    }

    /// Returns a reference to the decided consensus messages of this recovery state.
    pub fn log(&self) -> &O {
        &self.log
    }
}

struct ReceivedState<S, V, O> {
    count: usize,
    state: RecoveryState<S, V, O>,
}


// NOTE: in this module, we may use cid interchangeably with
// consensus sequence number
/// The collaborative state transfer algorithm.
///
/// The implementation is based on the paper «On the Efﬁciency of
/// Durable State Machine Replication», by A. Bessani et al.
pub struct CollabStateTransfer<D: SharedData + 'static, OP: StatefulOrderProtocol<D, NT>, NT> {
    latest_cid: SeqNo,
    cst_seq: SeqNo,
    latest_cid_count: usize,
    base_timeout: Duration,
    curr_timeout: Duration,
    // NOTE: remembers whose replies we have
    // received already, to avoid replays
    //voted: HashSet<NodeId>,
    node: Arc<NT>,
    received_states: HashMap<Digest, ReceivedState<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>>,
    phase: ProtoPhase<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>,
}

/// Status returned from processing a state transfer message.
pub enum CstStatus<S, V, O> {
    /// We are not running the CST protocol.
    ///
    /// Drop any attempt of processing a message in this condition.
    Nil,
    /// The CST protocol is currently running.
    Running,
    /// We should request the latest cid from the view.
    RequestLatestCid,
    /// We should request the latest state from the view.
    RequestState,
    /// We have received and validated the largest consensus sequence
    /// number available.
    SeqNo(SeqNo),
    /// We have received and validated the state from
    /// a group of replicas.
    State(RecoveryState<S, V, O>),
}

/// Represents progress in the CST state machine.
///
/// To clarify, the mention of state machine here has nothing to do with the
/// SMR protocol, but rather the implementation in code of the CST protocol.
pub enum CstProgress<S, V, O> {
    // TODO: Timeout( some type here)
    /// This value represents null progress in the CST code's state machine.
    Nil,
    /// We have a fresh new message to feed the CST state machine, from
    /// the communication layer.
    Message(Header, CstMessage<S, V, O>),
}

macro_rules! getmessage {
    ($progress:expr, $status:expr) => {
        match $progress {
            CstProgress::Nil => return $status,
            CstProgress::Message(h, m) => (h, m),
        }
    };
    // message queued while waiting for exec layer to deliver app state
    ($phase:expr) => {{
        let phase = std::mem::replace($phase, ProtoPhase::Init);
        match phase {
            ProtoPhase::WaitingCheckpoint(h, m) => (h, m),
            _ => return CstStatus::Nil,
        }
    }};
}

impl<D, OP, NT> StateTransferProtocol<D, OP, NT> for CollabStateTransfer<D, OP, NT>
    where D: SharedData + 'static,
          OP: StatefulOrderProtocol<D, NT> + 'static
{
    type Serialization = CSTMsg<D, OP::StateSerialization>;
    type Config = StateTransferConfig;

    fn initialize(config: Self::Config, timeouts: Timeouts, node: Arc<NT>)
                  -> Result<Self>
        where
            Self: Sized {
        let StateTransferConfig {
            timeout_duration
        } = config;

        Ok(CollabStateTransfer::<D, OP, NT>::new(node, timeout_duration))
    }

    fn request_latest_state(&mut self) -> Result<()> {
        todo!()
    }

    fn handle_off_ctx_message(&mut self, order_protocol: &mut OP,
                              message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>)
                              -> Result<()>
        where NT: Node<ServiceMsg<D, OP::StateSerialization, Self::Serialization>> {
        let (header, message) = message.into_inner();

        let message = message.into_inner();

        let status = self.process_message(
            CstProgress::Message(header, message),
            order_protocol,
        );

        match status {
            CstStatus::Nil => (),
            // should not happen...
            _ => {
                return Err("Invalid state reached!").wrapped(ErrorKind::CoreServer);
            }
        }

        Ok(())
    }

    fn process_message(&mut self, order_protocol: &mut OP,
                       message: StoredMessage<StateTransfer<CstM<Self::Serialization>>>)
                       -> Result<STResult>
        where NT: Node<ServiceMsg<D, OP::StateSerialization, Self::Serialization>> {
        todo!()
    }

    fn handle_state_received_from_app(&mut self, order_protocol: &mut OP, state: Arc<ReadOnly<Checkpoint<D::State>>>) -> Result<()>
        where NT: Node<ServiceMsg<D, OP::StateSerialization, Self::Serialization>> {
        todo!()
    }
}

// TODO: request timeouts
impl<D, OP, NT> CollabStateTransfer<D, OP, NT>
    where
        D: SharedData + 'static,
        OP: StatefulOrderProtocol<D, NT>
{
    /// Craete a new instance of `CollabStateTransfer`.
    pub fn new(node: Arc<NT>, base_timeout: Duration) -> Self {
        Self {
            base_timeout,
            curr_timeout: base_timeout,
            node,
            received_states: collections::hash_map(),
            phase: ProtoPhase::Init,
            latest_cid: SeqNo::ZERO,
            latest_cid_count: 0,
            cst_seq: SeqNo::ZERO,
        }
    }

    /// Checks if the CST layer is waiting for a local checkpoint to
    /// complete.
    ///
    /// This is used when a node is sending state to a peer.
    pub fn needs_checkpoint(&self) -> bool {
        matches!(self.phase, ProtoPhase::WaitingCheckpoint(_, _))
    }

    fn process_reply_state(
        &mut self,
        header: Header,
        message: CstMessage<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>,
        op: &mut OP,
    ) where
        OP: StatefulOrderProtocol<D, NT>,
        NT: Node<ServiceMsg<D, OP::StateSerialization, CSTMsg<D, OP::StateSerialization>>>
    {
        let (state, view, dec_log) = match op.snapshot_log() {
            Ok((state, view, dec_log)) => { (state, view, dec_log) }
            Err(_) => {
                self.phase = ProtoPhase::WaitingCheckpoint(header, message);
                return;
            }
        };

        let reply = CstMessage::new(
            message.sequence_number(),
            CstMessageKind::ReplyState(RecoveryState {
                checkpoint: state,
                view,
                log: dec_log,
            }),
        );

        let network_msg = NetworkMessageKind::from(SystemMessage::from_state_transfer_message(reply));

        self.node.send(network_msg,
                       header.from(), true).unwrap();
    }

    /// Advances the state of the CST state machine.
    pub fn process_message(
        &mut self,
        progress: CstProgress<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>,
        order_protocol: &mut OP,
    ) -> CstStatus<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>>
        where
            OP: StatefulOrderProtocol<D, NT>,
            NT: Node<ServiceMsg<D, OP::StateSerialization, CSTMsg<D, OP::StateSerialization>>>
    {
        match self.phase {
            ProtoPhase::WaitingCheckpoint(_, _) => {
                let (header, message) = getmessage!(&mut self.phase);

                self.process_reply_state(header, message, order_protocol);

                CstStatus::Nil
            }
            ProtoPhase::Init => {
                let (header, message) = getmessage!(progress, CstStatus::Nil);
                match message.kind() {
                    CstMessageKind::RequestLatestConsensusSeq => {
                        let kind = CstMessageKind::ReplyLatestConsensusSeq(order_protocol.sequence_number());

                        let reply = CstMessage::new(message.sequence_number(), kind);

                        let network_msg = NetworkMessageKind::from(SystemMessage::from_state_transfer_message(reply));

                        self.node.send(network_msg, header.from(), true).unwrap();
                    }
                    CstMessageKind::RequestState => {
                        self.process_reply_state(header, message, order_protocol);
                    }
                    // we are not running cst, so drop any reply msgs
                    //
                    // TODO: maybe inspect cid msgs, and passively start
                    // the state transfer protocol, by returning
                    // CstStatus::RequestState
                    _ => (),
                }
                CstStatus::Nil
            }
            ProtoPhase::ReceivingCid(i) => {
                let (_header, message) = getmessage!(progress, CstStatus::RequestLatestCid);

                // drop cst messages with invalid seq no
                if message.sequence_number() != self.cst_seq {
                    // FIXME: how to handle old or newer messages?
                    // BFT-SMaRt simply ignores messages with a
                    // value of `queryID` different from the current
                    // `queryID` a replica is tracking...
                    // we will do the same for now
                    //
                    // TODO: implement timeouts to fix cases like this
                    return CstStatus::Running;
                }

                match message.kind() {
                    CstMessageKind::ReplyLatestConsensusSeq(seq) => {
                        match seq.cmp(&self.latest_cid) {
                            Ordering::Greater => {
                                self.latest_cid = *seq;
                                self.latest_cid_count = 1;
                            }
                            Ordering::Equal => {
                                self.latest_cid_count += 1;
                            }
                            Ordering::Less => (),
                        }
                    }
                    // drop invalid message kinds
                    _ => return CstStatus::Running,
                }

                // check if we have gathered enough cid
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                if i == order_protocol.view().quorum() {
                    self.phase = ProtoPhase::Init;
                    if self.latest_cid_count > order_protocol.view().f() {
                        // reset timeout, since req was successful
                        self.curr_timeout = self.base_timeout;

                        // the latest cid was available in at least
                        // f+1 replicas
                        CstStatus::SeqNo(self.latest_cid)
                    } else {
                        CstStatus::RequestLatestCid
                    }
                } else {
                    self.phase = ProtoPhase::ReceivingCid(i);
                    CstStatus::Running
                }
            }
            ProtoPhase::ReceivingState(i) => {
                let (header, mut message) = getmessage!(progress, CstStatus::RequestState);

                // NOTE: check comment above, on ProtoPhase::ReceivingCid
                if message.sequence_number() != self.cst_seq {
                    return CstStatus::Running;
                }

                let state = match message.take_state() {
                    Some(state) => state,
                    // drop invalid message kinds
                    None => return CstStatus::Running,
                };

                let received_state = self
                    .received_states
                    .entry(header.digest().clone())
                    .or_insert(ReceivedState { count: 0, state });

                received_state.count += 1;

                // check if we have gathered enough state
                // replies from peer nodes
                //
                // TODO: check for more than one reply from the same node
                let i = i + 1;

                if i != order_protocol.view().quorum() {
                    self.phase = ProtoPhase::ReceivingState(i);
                    return CstStatus::Running;
                }

                // NOTE: clear saved states when we return;
                // this is important, because each state
                // may be several GBs in size

                // check if we have at least f+1 matching states
                let digest = {
                    let received_state = self.received_states.iter().max_by_key(|(_, st)| st.count);
                    match received_state {
                        Some((digest, _)) => digest.clone(),
                        None => {
                            self.received_states.clear();
                            return CstStatus::RequestState;
                        }
                    }
                };
                let received_state = {
                    let received_state = self.received_states.remove(&digest);
                    self.received_states.clear();
                    received_state
                };

                // reset timeout, since req was successful
                self.curr_timeout = self.base_timeout;

                // return the state
                let f = order_protocol.view().f();
                match received_state {
                    Some(ReceivedState { count, state }) if count > f => CstStatus::State(state),
                    _ => CstStatus::RequestState,
                }
            }
        }
    }

    fn next_seq(&mut self) -> SeqNo {
        let next = self.cst_seq;
        self.cst_seq = self.cst_seq.next();
        next
    }

    /// Handle a timeout received from the timeouts layer.
    /// Returns a bool to signify if we must move to the Retrieving state
    /// If the timeout is no longer relevant, returns false (Can remain in current phase)
    pub fn cst_request_timed_out(&mut self, seq: SeqNo,
                                 timeouts: &Timeouts,
                                 order_protocol: &OP) -> bool
        where
            OP: StatefulOrderProtocol<D, NT>,
            NT: Node<ServiceMsg<D, OP::StateSerialization, CSTMsg<D, OP::StateSerialization>>> {
        let status = self.timed_out(seq);

        match status {
            CstStatus::RequestLatestCid => {
                self.request_latest_consensus_seq_no(
                    order_protocol,
                    timeouts,
                );

                true
            }
            CstStatus::RequestState => {
                self.request_latest_state(
                    order_protocol,
                    timeouts,
                );

                true
            }
            // nothing to do
            _ => false,
        }
    }

    fn timed_out(&mut self, seq: SeqNo) -> CstStatus<D::State, ViewInfo<OP::StateSerialization>, DecLog<OP::StateSerialization>> {
        if seq.next() != self.cst_seq {
            // the timeout we received is for a request
            // that has already completed, therefore we ignore it
            //
            // TODO: this check is probably not necessary,
            // as we have likely already updated the `ProtoPhase`
            // to reflect the fact we are no longer receiving state
            // from peer nodes
            return CstStatus::Nil;
        }

        match self.phase {
            // retry requests if receiving state and we have timed out
            ProtoPhase::ReceivingCid(_) => {
                self.curr_timeout *= 2;
                CstStatus::RequestLatestCid
            }
            ProtoPhase::ReceivingState(_) => {
                self.curr_timeout *= 2;
                CstStatus::RequestState
            }
            // ignore timeouts if not receiving any kind
            // of state from peer nodes
            _ => CstStatus::Nil,
        }
    }

    /// Used by a recovering node to retrieve the latest sequence number
    /// attributed to a client request by the consensus layer.
    pub fn request_latest_consensus_seq_no(
        &mut self,
        order_protocol: &OP,
        timeouts: &Timeouts,
    ) where
        OP: StatefulOrderProtocol<D, NT>,
        NT: Node<ServiceMsg<D, OP::StateSerialization, CSTMsg<D, OP::StateSerialization>>>
    {
        // reset state of latest seq no. request
        self.latest_cid = SeqNo::ZERO;
        self.latest_cid_count = 0;

        let cst_seq = self.next_seq();
        let current_view = order_protocol.view();

        timeouts.timeout_cst_request(self.curr_timeout,
                                     current_view.quorum() as u32,
                                     cst_seq);

        self.phase = ProtoPhase::ReceivingCid(0);

        let message = CstMessage::new(
            cst_seq,
            CstMessageKind::RequestLatestConsensusSeq,
        );

        let targets = NodeId::targets(0..current_view.n());

        self.node.broadcast(NetworkMessageKind::from(SystemMessage::from_state_transfer_message(message)), targets).unwrap();
    }

    /// Used by a recovering node to retrieve the latest state.
    pub fn request_latest_state(
        &mut self,
        order_protocol: &OP,
        timeouts: &Timeouts,
    ) where
        OP: StatefulOrderProtocol<D, NT>,
        NT: Node<ServiceMsg<D, OP::StateSerialization, CSTMsg<D, OP::StateSerialization>>>
    {
        // reset hashmap of received states
        self.received_states.clear();

        let cst_seq = self.next_seq();
        let current_view = order_protocol.view();

        timeouts.timeout_cst_request(self.curr_timeout,
                                     current_view.quorum() as u32,
                                     cst_seq);

        self.phase = ProtoPhase::ReceivingState(0);

        //TODO: Maybe attempt to use followers to rebuild state and avoid
        // Overloading the replicas
        let message = CstMessage::new(cst_seq, CstMessageKind::RequestState);
        let targets = NodeId::targets(0..current_view.n());

        self.node.broadcast(NetworkMessageKind::from(SystemMessage::from_state_transfer_message(message)), targets).unwrap();
    }
}
