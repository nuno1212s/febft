use std::collections::VecDeque;
use std::sync::Arc;
use chrono::Utc;
use log::{debug, warn};
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::message::{Header, StoredMessage};
use febft_communication::Node;
use febft_execution::ExecutorHandle;
use febft_execution::serialize::SharedData;
use febft_messages::serialize::StateTransferMessage;
use febft_messages::timeouts::Timeouts;
use crate::bft::consensus::ConsensusStatus;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind};
use crate::bft::msg_log::decided_log::Log;
use crate::bft::msg_log::deciding_log::DecidingLog;
use crate::bft::PBFT;
use crate::bft::sync::{AbstractSynchronizer, Synchronizer};
use crate::bft::sync::view::ViewInfo;

pub enum DecisionPhase {
    PrePreparing(usize),
    Preparing(usize),
    Committing(usize),
}

pub struct MessageQueue<O> {
    get_queue: bool,
    pre_prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
    prepares: VecDeque<StoredMessage<ConsensusMessage<O>>>,
    commits: VecDeque<StoredMessage<ConsensusMessage<O>>>,
}

pub struct ConsensusDecision<D: SharedData> {
    seq: SeqNo,
    phase: DecisionPhase,
    message_queue: MessageQueue<D::Request>,
    message_log: DecidingLog<D::Request>,
}

pub struct Consensus<D: SharedData + 'static, ST: StateTransferMessage + 'static> {
    node_id: NodeId,

    executor_handle: ExecutorHandle<D>,
}

impl<O> MessageQueue<O> {
    fn new() -> Self {
        Self {
            get_queue: false,
            pre_prepares: Default::default(),
            prepares: Default::default(),
            commits: Default::default(),
        }
    }

    fn signal(&mut self) {
        self.get_queue = true;
    }

    fn queue_pre_prepare(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.pre_prepares.push_back(message)
    }

    fn queue_prepare(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.prepares.push_back(message)
    }

    fn queue_commit(&mut self, message: StoredMessage<ConsensusMessage<O>>) {
        self.commits.push_back(message)
    }
}

impl<D: SharedData> ConsensusDecision<D> {
    pub fn init_decision(node_id: NodeId, seq_no: SeqNo, view: &ViewInfo) -> Self {
        Self {
            seq: seq_no,
            phase: DecisionPhase::PrePreparing(0),
            message_queue: MessageQueue::new(),
            message_log: DecidingLog::new(node_id, view),
        }
    }

    pub fn process_message<NT, ST>(&mut self,
                                   header: Header,
                                   message: ConsensusMessage<D::Request>,
                                   synchronizer: &Synchronizer<D>,
                                   timeouts: &Timeouts,
                                   log: &mut Log<D>,
                                   node: &NT) -> ConsensusStatus<D>
        where NT: Node<PBFT<D, ST>>, ST: StateTransferMessage + 'static {
        let view = synchronizer.view();

        match self.phase {
            DecisionPhase::PrePreparing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_)
                    if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        debug!("{:?} // Dropped pre prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), synchronizer.view().sequence_number());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_)
                    if !view.leader_set().contains(&header.from()) => {
                        // Drop proposed value since sender is not leader
                        debug!("{:?} // Dropped pre prepare message because the sender was not the leader {:?} vs {:?} (ours)",
                        self.node_id, header.from(), view.leader());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_)
                    if message.sequence_number() != self.seq => {
                        //Drop proposed value since it is not for this consensus instance
                        warn!("{:?} // Dropped pre prepare message because the sequence number was not the same {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), self.seq);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) => {
                        debug!("{:?} // Received prepare message {:?} from {:?} while in prepreparing ",
                            self.node_id, d, header.from());

                        self.message_queue.queue_prepare(StoredMessage::new(header, message));

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(d) => {
                        debug!("{:?} // Received commit message {:?} from {:?} while in pre preparing",
                            self.node_id, d, header.from());

                        self.message_queue.queue_commit(StoredMessage::new(header, message));

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::PrePrepare(_) => {
                        // Everything checks out, we can now process the message
                        received + 1
                    }
                };

                let stored_msg = Arc::new(ReadOnly::new(StoredMessage::new(header, message)));

                let batch_metadata = self.message_log.process_pre_prepare(stored_msg.clone(), received);

                if let Err(err) = &batch_metadata {
                    //There was an error when analysing the batch of requests, so it won't be counted
                    panic!("Failed to analyse request batch {:?}", err);
                }
                let batch_metadata = batch_metadata.unwrap();

                self.phase = if i == view.leader_set().len() {

                    //We have received all pre prepare requests for this consensus instance
                    //We are now ready to broadcast our prepare message and move to the next phase
                    {
                        //Update batch meta
                        let mut meta_guard = self.message_log.batch_meta().lock().unwrap();

                        meta_guard.prepare_sent_time = Utc::now();
                        meta_guard.pre_prepare_received_time = pre_prepare_received_time;
                    }

                    let seq_no = self.sequence_number();
                    let metadata = batch_metadata
                        .expect("Received all messages but still can't calculate batch digest?");

                    let current_digest = metadata.batch_digest();

                    // Register that all of the batches have been received
                    // The digest of the batch and the order of the batches
                    log.all_batches_received(metadata);

                    match &mut self.accessory {
                        ConsensusAccessory::Follower => {}
                        ConsensusAccessory::Replica(rep) => {
                            //Perform the rest of the necessary operations to handle the received message
                            //If we are a replica (In particular, bcast the messages)
                            rep.handle_pre_prepare_successful(seq_no,
                                                              current_digest,
                                                              view, stored_msg, node);
                        }
                    }

                    debug!("{:?} // Completed pre prepare phase with all pre prepares Seq {:?}", node.id(), self.sequence_number());

                    // We no longer start the count at 1 since all leaders must also send the prepare
                    // message with the digest of the entire batch
                    DecisionPhase::Preparing(0)
                } else {
                    DecisionPhase::PrePreparing(received)
                };
            }
            DecisionPhase::Preparing(received) => {
                let received = match message.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        // When we are in the preparing phase, we no longer accept any pre prepare message
                        warn!("{:?} // Dropped pre prepare message because we are in the preparing phase",
                            self.node_id);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Commit(_) => {
                        debug!("{:?} // Received commit message {:?} from {:?} while in preparing phase",
                            self.node_id, d, header.from());

                        self.message_queue.queue_commit(StoredMessage::new(header, message));

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) if message.view() != view.sequence_number() => {
                        // drop proposed value in a different view (from different leader)
                        debug!("{:?} // Dropped prepare message because of view {:?} vs {:?} (ours)",
                            self.node_id, message.view(), view.sequence_number());

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) if message.sequence_number() != self.seq => {
                        // drop proposed value in a different view (from different leader)
                        warn!("{:?} // Dropped prepare message because of seq no {:?} vs {:?} (Ours)",
                            self.node_id, message.sequence_number(), self.seq);

                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(d) if *d != self.message_log.current_digest().unwrap() => {
                        // drop msg with different digest from proposed value
                        debug!("{:?} // Dropped prepare message {:?} from {:?} because of digest {:?} vs {:?} (ours)",
                            self.node_id, message.sequence_number(), header.from(), d, self.message_log.current_digest());
                        return ConsensusStatus::Deciding;
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        // Everything checks out, we can now process the message
                        received + 1
                    }
                };

                let stored_msg = Arc::new(ReadOnly::new(
                    StoredMessage::new(header, message)));


            }
            DecisionPhase::Committing(received) => {}
        }


        return ConsensusStatus::Deciding;
    }
}

impl<D: SharedData> Orderable for ConsensusDecision<D> {
    fn sequence_number(&self) -> SeqNo {
        self.seq
    }
}