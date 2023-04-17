//! Contains the server side core protocol logic of `febft`.

use std::sync::Arc;
use std::time::Duration;
use futures_timer::Delay;

use log::{debug, error, info};
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx};

use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::ordering::{SeqNo};
use febft_communication::{Node, NodeConnections};
use febft_communication::message::{StoredMessage};
use febft_execution::app::{Service, State};
use febft_execution::ExecutorHandle;
use febft_messages::messages::Message;
use febft_messages::messages::SystemMessage;
use febft_messages::ordering_protocol::OrderingProtocol;
use febft_messages::ordering_protocol::OrderProtocolExecResult;
use febft_messages::ordering_protocol::OrderProtocolPoll;
use febft_messages::serialize::ServiceMsg;
use febft_messages::state_transfer::{Checkpoint, StatefulOrderProtocol, StateTransferProtocol, STResult, STTimeoutResult};
use febft_messages::timeouts::{Timeout, TimeoutKind, Timeouts};
use crate::config::ReplicaConfig;
use crate::executable::{Executor, ReplicaReplier};
use crate::server::client_replier::Replier;

//pub mod observer;

pub mod client_replier;
pub mod follower_handling;
// pub mod rq_finalizer;


pub const REPLICA_WAIT_TIME: Duration = Duration::from_millis(50);

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
                                                 ST: StateTransferProtocol<S::Data, OP, NT> + 'static,
                                                 NT: Node<ServiceMsg<S::Data, OP::Serialization, ST::Serialization>> + 'static {
    pub async fn bootstrap(cfg: ReplicaConfig<S, OP, ST, NT>) -> Result<Self> {
        let ReplicaConfig {
            service,
            id: log_node_id,
            n,
            f,
            view,
            next_consensus_seq,
            op_config,
            st_config,
            node: node_config
        } = cfg;

        debug!("{:?} // Bootstrapping replica, starting with networking", log_node_id);

        let node = NT::bootstrap(node_config).await?;

        let (executor, handle) = Executor::<S, NT>::init_handle();
        let (exec_tx, exec_rx) = channel::new_bounded_sync(1024);

        //CURRENTLY DISABLED, USING THREADPOOL INSTEAD
        let reply_handle = Replier::new(node.id(), node.clone());

        debug!("{:?} // Initializing timeouts", log_node_id);
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

        debug!("{:?} // Connecting to other replicas.", log_node_id);

        let mut connections = Vec::new();

        for node_id in NodeId::targets(0..n) {
            if node_id == log_node_id {
                continue;
            }

            debug!("{:?} // Connecting to node {:?}", log_node_id, node_id);

            let mut connection_results = node.node_connections().connect_to_node(node_id);

            connections.append(&mut connection_results);
        }

        while node.node_connections().connected_nodes_count() + 1 < n {
            debug!("{:?} // Waiting for node connections. Currently {} of {} ({:?})",
                log_node_id, node.node_connections().connected_nodes_count() + 1, n, node.node_connections().connected_nodes());

            Delay::new(Duration::from_millis(500)).await;
        }

        /*
        for conn_result in connections {
            match conn_result.await {
                Ok(result) => {
                    if let Err(err) = result {
                        error!("Failed to connect to the given node. {:?}", err);
                    } else {
                        info!("Established a new connection.");
                    }
                }
                Err(error) => {
                    error!("Failed to connect to the given node. {:?}", error);
                }
            }
        }
         */

        debug!("{:?} // Finished bootstrapping node.", log_node_id);

        let mut replica = Self {
            // We start with the state transfer protocol to make sure everything is up to date
            replica_phase: ReplicaPhase::StateTransferProtocol,
            ordering_protocol,
            state_transfer_protocol,
            timeouts,
            executor_handle: executor,
            node,
            execution_rx: exec_rx,
        };

        debug!("{:?} // Requesting state", log_node_id);

        replica.state_transfer_protocol.request_latest_state(&mut replica.ordering_protocol)?;

        Ok(replica)
    }

    pub fn run(&mut self) -> Result<()> {
        loop {

            self.receive_internal()?;

            match self.replica_phase {
                ReplicaPhase::OrderingProtocol => {
                    let poll_res = self.ordering_protocol.poll();

                    match poll_res {
                        OrderProtocolPoll::RePoll => {
                            //Continue
                        }
                        OrderProtocolPoll::ReceiveFromReplicas => {
                            let network_message = self.node.receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

                            if let Some(network_message) = network_message {
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
                                    SystemMessage::ForwardedRequestMessage(fwd_reqs) => {

                                        self.ordering_protocol.handle_forwarded_requests(StoredMessage::new(header, fwd_reqs))?;

                                    }
                                    SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                                        match self.ordering_protocol.process_message(fwd_protocol.into_inner())? {
                                            OrderProtocolExecResult::Success => {
                                                //Continue execution
                                            }
                                            OrderProtocolExecResult::RunCst => {
                                                self.run_state_transfer_protocol()?;
                                            }
                                        }
                                    }
                                    _ => {
                                        error!("{:?} // Received off context message {:?}", self.node.id(), message);
                                    }
                                }
                            } else {
                                // Receive timeouts in the beginning of the next iteration
                                continue;
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
                    let message = self.node.receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

                    if let Some(message) = message {
                        let (header, message) = message.into_inner();

                        let message = message.into_system();

                        match message {
                            SystemMessage::ProtocolMessage(protocol) => {
                                self.ordering_protocol.handle_off_ctx_message(StoredMessage::new(header, protocol));
                            }
                            SystemMessage::StateTransferMessage(state_transfer) => {
                                let result = self.state_transfer_protocol.process_message(&mut self.ordering_protocol, StoredMessage::new(header, state_transfer))?;

                                match result {
                                    STResult::CstRunning => {}
                                    STResult::CstFinished(state, requests) => {
                                        self.executor_handle.install_state(state, requests).unwrap();
                                    }
                                    STResult::CstNotNeeded => {
                                        self.run_ordering_protocol()?;
                                    }
                                    STResult::RunCst => {
                                        self.run_state_transfer_protocol()?;
                                    }
                                }
                            }
                            _ => {}
                        }
                    } else {
                        // Receive timeouts
                        continue
                    }
                }
            }
        }

        Ok(())
    }

    /// FIXME: Do this with a select?
    fn receive_internal(&mut self) -> Result<()> {
        while let Ok(recvd) = self.execution_rx.try_recv() {
            match recvd {
                Message::ExecutionFinishedWithAppstate((seq, state)) => {
                    let checkpoint = Checkpoint::new(seq, state);

                    self.state_transfer_protocol.handle_state_received_from_app(&mut self.ordering_protocol, checkpoint)?;
                }
                Message::Timeout(timeout) => {
                    self.timeout_received(timeout)?;
                }
            }
        }

        Ok(())
    }

    fn timeout_received(&mut self, timeouts: Timeout) -> Result<()> {

        let mut client_rq = Vec::with_capacity(timeouts.len());
        let mut cst_rq = Vec::with_capacity(timeouts.len());

        for timeout in timeouts {
            match timeout {
                TimeoutKind::ClientRequestTimeout(rq) => {
                    client_rq.push(rq);
                }
                TimeoutKind::Cst(rq) => {
                    cst_rq.push(rq);
                }
            }

        }

        if !client_rq.is_empty() {
            debug!("{:?} // Received client request timeouts: {}", self.node.id(), client_rq.len());
            match self.ordering_protocol.handle_timeout(client_rq)? {
                OrderProtocolExecResult::RunCst => {
                    self.run_state_transfer_protocol()?;
                }
                _ => {}
            };
        }

        if !cst_rq.is_empty() {
            debug!("{:?} // Received cst timeouts: {}", self.node.id(), cst_rq.len());

            match self.state_transfer_protocol.handle_timeout(&mut self.ordering_protocol, cst_rq)? {
                STTimeoutResult::RunCst => {
                    self.run_state_transfer_protocol()?;
                }
                _ => {}
            };
        }

        Ok(())
    }

    /// Run the ordering protocol on this replica
    fn run_ordering_protocol(&mut self) -> Result<()> {
        info!("{:?} // Running ordering protocol.", self.node.id());

        self.replica_phase = ReplicaPhase::OrderingProtocol;

        self.ordering_protocol.handle_execution_changed(true)?;

        Ok(())
    }

    /// Run the state transfer protocol on this replica
    fn run_state_transfer_protocol(&mut self) -> Result<()> {

        if self.replica_phase == ReplicaPhase::StateTransferProtocol {
            //TODO: This is here for when we receive various timeouts that consecutively call run cst
            // In reality, this should also lead to a new state request since the previous one could have
            // Timed out
            return Ok(());
        }

        self.ordering_protocol.handle_execution_changed(false)?;

        // Start by requesting the current state from neighbour replicas
        self.state_transfer_protocol.request_latest_state(&mut self.ordering_protocol)?;

        self.replica_phase = ReplicaPhase::StateTransferProtocol;

        Ok(())
    }

    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: State<S>) -> Result<()> {
        let checkpoint = Checkpoint::new(seq, appstate);

        self.state_transfer_protocol.handle_state_received_from_app(&mut self.ordering_protocol, checkpoint)?;

        Ok(())
    }
}