//! Contains the server side core protocol logic of `febft`.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool};
use std::sync::Arc;
use std::time::{Duration};

use chrono::offset::Utc;
use chrono::DateTime;
use log::{debug, warn};
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx};

use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{Orderable, SeqNo};
use febft_communication::{Node};
use febft_communication::config::NodeConfig;
use febft_communication::message::{Header, NetworkMessage, NetworkMessageKind, StoredMessage};
use febft_execution::app::{Reply, Request, Service, State};
use febft_execution::ExecutorHandle;
use febft_messages::messages::{ForwardedRequestsMessage, Message, RequestMessage, SystemMessage};
use febft_messages::ordering_protocol::{OrderingProtocol, OrderProtocolExecResult, OrderProtocolPoll};
use febft_messages::serialize::{StateTransferMessage, ServiceMsg};
use febft_messages::state_transfer::{Checkpoint, StatefulOrderProtocol, StateTransferProtocol, STResult};
use febft_messages::timeouts::{Timeout, TimeoutKind, Timeouts};
use crate::config::ReplicaConfig;
use crate::executable::{Executor, ReplicaReplier};
use crate::server::client_replier::Replier;

//pub mod observer;

pub mod client_replier;
pub mod follower_handling;
// pub mod rq_finalizer;


#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum ReplicaPhase {
    // The replica is currently executing the ordering protocol
    OrderingProtocol,
    // The replica is currently executing the state transfer protocol
    StateTransferProtocol,
}

pub struct Replica<S, OP, ST, NT> where S: Service {
    replica_phase: ReplicaPhase,
    // The ordering protocol
    ordering_protocol: OP,
    state_transfer_protocol: ST,
    timeouts: Timeouts,
    executor_handle: ExecutorHandle<S::Data>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
    // THe handle to the execution and timeouts handler
    execution_rx: ChannelSyncRx<Message<S::Data>>,
}

impl<S, OP, ST, NT> Replica<S, OP, ST, NT> where S: Service + 'static,
                                                 OP: StatefulOrderProtocol<S::Data, NT> + 'static,
                                                 ST: StateTransferProtocol<S::Data, NT> + 'static,
                                                 NT: Node<ServiceMsg<S::Data, OP::Serialization, ST::Serialization>> + 'static {
    pub async fn bootstrap<T>(cfg: ReplicaConfig<S, NT, OP, ST>) -> Result<Self> {
        let ReplicaConfig {
            service,
            n,
            f,
            view,
            next_consensus_seq,
            op_config,
            st_config,
            db_path,
            node: node_config
        } = cfg;

        let per_pool_batch_timeout = node_config.client_pool_config.batch_timeout_micros;
        let per_pool_batch_sleep = node_config.client_pool_config.batch_sleep_micros;
        let per_pool_batch_size = node_config.client_pool_config.batch_size;

        let log_node_id = node_config.id.clone();

        debug!("Bootstrapping replica, starting with networking");

        let node = NT::bootstrap(node_config).await?;

        let (executor, handle) = Executor::<S, NT>::init_handle();
        let (exec_tx, exec_rx) = channel::new_bounded_sync(1024);

        //CURRENTLY DISABLED, USING THREADPOOL INSTEAD
        let reply_handle = Replier::new(node.id(), node.clone());

        debug!("Initializing timeouts");
        // start timeouts handler
        let timeouts = Timeouts::new::<S::Data>(500, exec_tx.clone());

        // Initialize the ordering protocol
        let ordering_protocol = OP::initialize(op_config, executor.clone(), timeouts.clone(), node.clone())?;

        let state_transfer_protocol = ST::initialize(st_config, timeouts.clone(), node.clone())?;

        let state = None;

        // start executor
        Executor::<S, NT>::new::<OP::Serialization, ST::Serialization, ReplicaReplier>(
            reply_handle,
            handle,
            service,
            state,
            node.clone(),
            exec_tx.clone(),
        )?;

        debug!("{:?} // Started replica.", log_node_id);

        debug!(
            "{:?} // Per Pool Batch size: {}",
            log_node_id,
            per_pool_batch_size
        );
        debug!(
            "{:?} // Per pool batch sleep: {}",
            log_node_id,
            per_pool_batch_sleep
        );
        debug!(
            "{:?} // Per pool batch timeout: {}",
            log_node_id,
            per_pool_batch_timeout
        );

        Ok(Self {
            // We start with the state transfer protocol to make sure everything is up to date
            replica_phase: ReplicaPhase::StateTransferProtocol,
            ordering_protocol,
            state_transfer_protocol,
            timeouts,
            executor_handle: executor,
            node,
            execution_rx: exec_rx,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            match self.replica_phase {
                ReplicaPhase::OrderingProtocol => {
                    let poll_res = self.ordering_protocol.poll();
                    match poll_res {
                        OrderProtocolPoll::RePoll => {
                            //Continue
                        }
                        OrderProtocolPoll::ReceiveFromReplicas => {
                            let network_message = self.node.receive_from_replicas().unwrap();

                            let (header, message) = network_message.into_inner();

                            let message = message.into_system();

                            match message {
                                SystemMessage::ProtocolMessage(protocol) => {
                                    match self.ordering_protocol.process_message(StoredMessage::new(header, protocol))? {
                                        OrderProtocolExecResult::Success => {
                                            //Continue execution
                                        }
                                        OrderProtocolExecResult::RunCst => {
                                            self.run_state_transfer_protocol()?;
                                        }
                                    }
                                }
                                SystemMessage::StateTransferMessage(state_transfer) => {
                                    self.state_transfer_protocol.handle_off_ctx_message(&mut self.ordering_protocol,
                                                                                        StoredMessage::new(header, state_transfer)).unwrap();
                                }
                                _ => {
                                    todo!()
                                }
                            }
                        }
                        OrderProtocolPoll::Exec(message) => {
                            match self.ordering_protocol.process_message(message)? {
                                OrderProtocolExecResult::Success => {
                                    // Continue execution
                                }
                                OrderProtocolExecResult::RunCst => {
                                    self.run_state_transfer_protocol()?;
                                }
                            }
                        }
                        OrderProtocolPoll::RunCst => {
                            self.run_state_transfer_protocol()?;
                        }
                    }
                }
                ReplicaPhase::StateTransferProtocol => {
                    let (header, message) = self.node.receive_from_replicas().unwrap().into_inner();

                }
            }
        }
        Ok(())
    }

    /// Run the state transfer protocol on this replica
    fn run_state_transfer_protocol(&mut self) -> Result<()> {

        // Start by requesting the current state from neighbour replicas
        self.state_transfer_protocol.request_latest_state()?;

        self.replica_phase = ReplicaPhase::StateTransferProtocol;

        Ok(())
    }

    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: State<S>) -> Result<()> {

        let checkpoint = Checkpoint::new(seq, appstate);

        self.state_transfer_protocol.handle_state_received_from_app(&mut self.ordering_protocol, checkpoint)?;

        Ok(())
    }
}