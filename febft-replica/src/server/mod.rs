 //! Contains the server side core protocol logic of `febft`.

use std::sync::Arc;
use std::time::{Duration, Instant};
use futures_timer::Delay;

use log::{debug, error, info, trace};
use febft_common::{channel, threadpool};
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};

use febft_common::async_runtime as rt;
use febft_common::error::*;
use febft_common::globals::ReadOnly;
use febft_common::node_id::NodeId;
use febft_common::ordering::{SeqNo};
use febft_communication::{Node, NodeConnections, NodeIncomingRqHandler};
use febft_communication::message::{StoredMessage};
use febft_execution::app::{Request, Service, State};
use febft_execution::ExecutorHandle;
use febft_execution::serialize::digest_state;
use febft_messages::messages::Message;
use febft_messages::messages::SystemMessage;
use febft_messages::ordering_protocol::{OrderingProtocol, OrderingProtocolArgs};
use febft_messages::ordering_protocol::OrderProtocolExecResult;
use febft_messages::ordering_protocol::OrderProtocolPoll;
use febft_messages::request_pre_processing::{initialize_request_pre_processor, PreProcessorMessage, RequestPreProcessor};
use febft_messages::request_pre_processing::work_dividers::WDRoundRobin;
use febft_messages::serialize::ServiceMsg;
use febft_messages::state_transfer::{Checkpoint, StatefulOrderProtocol, StateTransferProtocol, STResult, STTimeoutResult};
use febft_messages::timeouts::{TimedOut, Timeout, TimeoutKind, Timeouts};
use febft_metrics::metrics::{metric_duration, metric_store_count};
use crate::config::ReplicaConfig;
use crate::executable::{Executor, ReplicaReplier};
use crate::metric::{ORDERING_PROTOCOL_POLL_TIME_ID, ORDERING_PROTOCOL_PROCESS_TIME_ID, REPLICA_RQ_QUEUE_SIZE_ID, RUN_LATENCY_TIME_ID, STATE_TRANSFER_PROCESS_TIME_ID, TIMEOUT_PROCESS_TIME_ID};
use crate::server::client_replier::Replier;

//pub mod observer;

pub mod client_replier;
pub mod follower_handling;
// pub mod rq_finalizer;


pub const REPLICA_WAIT_TIME: Duration = Duration::from_millis(1000);

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
    rq_pre_processor: RequestPreProcessor<Request<S>>,
    timeouts: Timeouts,
    executor_handle: ExecutorHandle<S::Data>,
    // The networking layer for a Node in the network (either Client or Replica)
    node: Arc<NT>,
    // THe handle to the execution and timeouts handler
    execution_rx: ChannelSyncRx<Message<S::Data>>,
    execution_tx: ChannelSyncTx<Message<S::Data>>,
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

        let (rq_pre_processor, batch_input) = initialize_request_pre_processor
            ::<WDRoundRobin, S::Data, OP::Serialization, ST::Serialization, NT>(8, node.clone());

        // start timeouts handler
        let timeouts = Timeouts::new::<S::Data>(log_node_id.clone(), 500, exec_tx.clone());

        //Calculate the initial state of the application so we can pass it to the ordering protocol
        let init_ex_state = S::initial_state()?;
        let digest = digest_state::<S::Data>(&init_ex_state)?;

        let initial_state = Checkpoint::new(SeqNo::ZERO, init_ex_state, digest);

        let op_args = OrderingProtocolArgs(executor.clone(), timeouts.clone(),
                                           rq_pre_processor.clone(),
                                           batch_input, node.clone());

        // Initialize the ordering protocol
        let ordering_protocol = OP::initialize_with_initial_state(op_config, op_args, initial_state)?;

        let state_transfer_protocol = ST::initialize(st_config, timeouts.clone(), node.clone())?;

        // Check with the order protocol to see if there were no stored states
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

        info!("{:?} // Connecting to other replicas.", log_node_id);

        let mut connections = Vec::new();

        for node_id in NodeId::targets(0..n) {
            if node_id == log_node_id {
                continue;
            }

            info!("{:?} // Connecting to node {:?}", log_node_id, node_id);

            let mut connection_results = node.node_connections().connect_to_node(node_id);

            connections.push((node_id, connection_results));
        }

        'outer: for (peer_id, conn_result) in connections {
            for conn in conn_result {
                match conn.await {
                    Ok(result) => {
                        if let Err(err) = result {
                            error!("{:?} // Failed to connect to {:?} for {:?}", log_node_id, peer_id, err);
                            continue 'outer;
                        }
                    }
                    Err(error) => {
                        error!("Failed to connect to the given node. {:?}", error);
                        continue 'outer;
                    }
                }
            }

            info!("{:?} // Established a new connection to node {:?}.", log_node_id, peer_id);
        }

        info!("{:?} // Connected to all other replicas.", log_node_id);

        info!("{:?} // Finished bootstrapping node.", log_node_id);

        let mut replica = Self {
            // We start with the state transfer protocol to make sure everything is up to date
            replica_phase: ReplicaPhase::StateTransferProtocol,
            ordering_protocol,
            state_transfer_protocol,
            rq_pre_processor,
            timeouts,
            executor_handle: executor,
            node,
            execution_rx: exec_rx,
            execution_tx: exec_tx,
        };

        info!("{:?} // Requesting state", log_node_id);

        replica.state_transfer_protocol.request_latest_state(&mut replica.ordering_protocol)?;

        Ok(replica)
    }

    pub fn run(&mut self) -> Result<()> {
        let mut last_loop = Instant::now();

        loop {
            self.receive_internal()?;

            match self.replica_phase {
                ReplicaPhase::OrderingProtocol => {
                    let poll_res = self.ordering_protocol.poll();

                    trace!("{:?} // Polling ordering protocol with result {:?}", self.node.id(), poll_res);

                    match poll_res {
                        OrderProtocolPoll::RePoll => {
                            //Continue
                        }
                        OrderProtocolPoll::ReceiveFromReplicas => {
                            let start = Instant::now();

                            let network_message = self.node.node_incoming_rq_handling().receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

                            metric_store_count(REPLICA_RQ_QUEUE_SIZE_ID, self.node.node_incoming_rq_handling().rqs_len_from_replicas());

                            if let Some(network_message) = network_message {
                                let (header, message) = network_message.into_inner();

                                let message = message.into_system();

                                match message {
                                    SystemMessage::ProtocolMessage(protocol) => {

                                        let start = Instant::now();

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
                                        // Send the forwarded requests to be handled, filtered and then passed onto the ordering protocol
                                        self.rq_pre_processor.send(PreProcessorMessage::ForwardedRequests(StoredMessage::new(header, fwd_reqs))).unwrap();
                                    }
                                    SystemMessage::ForwardedProtocolMessage(fwd_protocol) => {
                                        let start = Instant::now();

                                        match self.ordering_protocol.process_message(fwd_protocol.into_inner())? {
                                            OrderProtocolExecResult::Success => {
                                                //Continue execution
                                            }
                                            OrderProtocolExecResult::RunCst => {
                                                self.run_state_transfer_protocol()?;
                                            }
                                        }

                                        metric_duration(ORDERING_PROTOCOL_PROCESS_TIME_ID, start.elapsed());
                                    }
                                    _ => {
                                        error!("{:?} // Received unsupported message {:?}", self.node.id(), message);
                                    }
                                }
                            } else {
                                // Receive timeouts in the beginning of the next iteration
                                continue;
                            }

                            metric_duration(ORDERING_PROTOCOL_PROCESS_TIME_ID, start.elapsed());
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
                    let message = self.node.node_incoming_rq_handling().receive_from_replicas(Some(REPLICA_WAIT_TIME)).unwrap();

                    if let Some(message) = message {
                        let (header, message) = message.into_inner();

                        let message = message.into_system();

                        match message {
                            SystemMessage::ProtocolMessage(protocol) => {
                                self.ordering_protocol.handle_off_ctx_message(StoredMessage::new(header, protocol));
                            }
                            SystemMessage::StateTransferMessage(state_transfer) => {
                                let start = Instant::now();

                                let result = self.state_transfer_protocol.process_message(&mut self.ordering_protocol, StoredMessage::new(header, state_transfer))?;

                                match result {
                                    STResult::CstRunning => {}
                                    STResult::CstFinished(state, requests) => {
                                        info!("{:?} // State transfer finished. Installing state in executor and running ordering protocol", self.node.id());

                                        self.executor_handle.install_state(state, requests)?;

                                        self.run_ordering_protocol()?;
                                    }
                                    STResult::CstNotNeeded => {
                                        self.run_ordering_protocol()?;
                                    }
                                    STResult::RunCst => {
                                        self.run_state_transfer_protocol()?;
                                    }
                                }

                                metric_duration(STATE_TRANSFER_PROCESS_TIME_ID, start.elapsed());
                            }
                            _ => {}
                        }
                    }
                }
            }

            metric_duration(RUN_LATENCY_TIME_ID, last_loop.elapsed());

            last_loop = Instant::now();
        }

        Ok(())
    }

    /// FIXME: Do this with a select?
    fn receive_internal(&mut self) -> Result<()> {
        while let Ok(recvd) = self.execution_rx.try_recv() {
            match recvd {
                Message::ExecutionFinishedWithAppstate((seq, state)) => {
                    self.execution_finished_with_appstate(seq, state)?;
                }
                Message::Timeout(timeout) => {
                    self.timeout_received(timeout)?;
                    //info!("{:?} // Received and ignored timeout with {} timeouts {:?}", self.node.id(), timeout.len(), timeout);
                }
                Message::DigestedAppState(digested) => {
                    self.state_transfer_protocol.handle_state_received_from_app(&mut self.ordering_protocol, digested)?;
                }
            }
        }

        Ok(())
    }

    fn timeout_received(&mut self, timeouts: TimedOut) -> Result<()> {
        let start = Instant::now();

        let mut client_rq = Vec::with_capacity(timeouts.len());
        let mut cst_rq = Vec::with_capacity(timeouts.len());

        for timeout in timeouts {
            match timeout.timeout_kind() {
                TimeoutKind::ClientRequestTimeout(_) => {
                    client_rq.push(timeout);
                }
                TimeoutKind::Cst(_) => {
                    cst_rq.push(timeout);
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

        metric_duration(TIMEOUT_PROCESS_TIME_ID, start.elapsed());

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

        info!("{:?} // Running state transfer protocol.", self.node.id());

        self.ordering_protocol.handle_execution_changed(false)?;

        // Start by requesting the current state from neighbour replicas
        self.state_transfer_protocol.request_latest_state(&mut self.ordering_protocol)?;

        self.replica_phase = ReplicaPhase::StateTransferProtocol;

        Ok(())
    }

    fn execution_finished_with_appstate(&mut self, seq: SeqNo, appstate: State<S>) -> Result<()> {
        let return_tx = self.execution_tx.clone();

        // Digest the app state before passing it on to the ordering protocols
        threadpool::execute(move || {
            let result = febft_execution::serialize::digest_state::<S::Data>(&appstate);

            match result {
                Ok(digest) => {
                    let checkpoint = Checkpoint::new(seq, appstate, digest);

                    return_tx.send(Message::DigestedAppState(checkpoint)).unwrap();
                }
                Err(error) => {
                    error!("Failed to serialize and digest application state: {:?}", error)
                }
            }
        });

        Ok(())
    }
}