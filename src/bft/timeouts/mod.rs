use std::cmp::Ordering;
use std::collections::{BinaryHeap, BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use log::info;
use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx, new_bounded_sync, TryRecvError};
use crate::bft::communication::message::Message;
use crate::bft::communication::NodeId;
use crate::bft::communication::peer_handling::ConnectedPeer;
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::ordering::SeqNo;

const CHANNEL_SIZE: usize = 1024;

///Contains the requests that have just been timed out
pub type Timeout = Vec<TimeoutKind>;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
pub struct ClientRqInfo {
    client_id: NodeId,
    req_session: SeqNo,
    req_seq_no: SeqNo,
}

pub enum TimeoutKind {
    ///Relates to the timeout of a client request.
    /// Stores the client who sent it, along with the request
    /// session and request sequence number
    ClientRequestTimeout(ClientRqInfo),

    /// As for CST messages, these messages aren't particularly ordered, they are just
    /// for each own node to know to what messages the peers are responding to.
    Cst(SeqNo),
}

struct TimeoutRequest {
    time_made: u64,
    timeout: Duration,
    info: TimeoutKind,
}

type TimeoutMessage = MessageType;

enum MessageType {
    TimeoutRequest(RqTimeoutMessage),
    MessagesReceived(Vec<TimeoutKind>),
}

struct RqTimeoutMessage {
    timeout: Duration,
    timeout_info: Timeout,
}

#[derive(Clone)]
pub struct Timeouts {
    handle: ChannelSyncTx<TimeoutMessage>,
}

/// This structure is responsible for handling timeouts for the entire project
/// This includes timing out messages exchanged between replicas and between clients
struct TimeoutsThread<S: Service + 'static> {
    //Stores the pending timeouts, grouped by the time at which they timeout.
    //Iterating a binary tree is pretty quick and it keeps the elements ordered
    //So we can use that to our advantage when timing out requests
    pending_timeouts: BTreeMap<u64, Vec<TimeoutRequest>>,
    //Receive messages from other threads
    channel_rx: ChannelSyncRx<TimeoutMessage>,
    //Loopback so we can deliver the timeouts to the main consensus thread so they can be
    //processed
    loopback_channel: Arc<ConnectedPeer<Message<State<S>, Request<S>, Reply<S>>>>,
    //How long between each timeout iteration
    iteration_delay: u64,
}


impl Timeouts {
    ///Initialize the timeouts thread and return a handle to it
    /// This handle can then be used everywhere timeouts are needed.
    pub fn new<S: Service + 'static>(iteration_delay: u64,
               loopback_channel: Arc<ConnectedPeer<Message<State<S>, Request<S>, Reply<S>>>>) -> Self {
        let tx = TimeoutsThread::<S>::new(iteration_delay, loopback_channel);

        Self {
            handle: tx,
        }
    }

    pub fn timeout(&self, timeout: Duration, timeout_info: TimeoutKind) {
        self.handle.send(TimeoutMessage::TimeoutRequest ( RqTimeoutMessage{
            timeout,
            timeout_info: vec![timeout_info],
        })).expect("Failed to contact timeout thread");
    }

    pub fn timeout_requests(&self, timeout: Duration, timeout_infos: Vec<TimeoutKind>) {
        self.handle.send(TimeoutMessage::TimeoutRequest(RqTimeoutMessage {
            timeout,
            timeout_info: timeout_infos,
        })).expect("Failed to contact timeout thread");
    }

    pub fn received_requests(self, recvd_rqs: Vec<TimeoutKind>) {
        self.handle.send(TimeoutMessage::MessagesReceived(recvd_rqs))
            .expect("Failed to contact timeout thread");}
}

impl<S: Service + 'static> TimeoutsThread<S> {
    fn new(iteration_delay: u64, loopback_channel: Arc<ConnectedPeer<Message<State<S>, Request<S>, Reply<S>>>>) -> ChannelSyncTx<TimeoutMessage> {
        let (tx, rx) = new_bounded_sync(CHANNEL_SIZE);

        let timeout_thread = Self {
            pending_timeouts: BTreeMap::new(),
            channel_rx: rx,
            loopback_channel,
            iteration_delay,
        };

        std::thread::Builder::new().name("Timeout Thread".to_string())
            .spawn(move || {
                timeout_thread.run();
            }).expect("Failed to launch timeout thread");

        tx
    }

    fn run(mut self) {
        let iteration_delay = Duration::from_millis(self.iteration_delay);

        loop {
            let message = match self.channel_rx.recv_timeout(iteration_delay) {
                Ok(message) => { Some(message) }
                Err(err) => {
                    match err {
                        TryRecvError::Timeout => {
                            None
                        }
                        _ => {
                            info!("Timeouts received error from recv, shutting down");

                            break;
                        }
                    }
                }
            };

            ///Handle all incoming messages and update the pending timeouts accordingly
            if let Some(mut message) = message {
                match message {
                    MessageType::TimeoutRequest(RqTimeoutMessage { timeout, mut timeout_info }) => {
                        let current_timestamp = Utc::now().timestamp_millis() as u64;

                        let final_timestamp = current_timestamp + timeout.as_millis() as u64;

                        let mut timeout_rqs = Vec::with_capacity(timeout_info.len());

                        for timeout_kind in timeout_info {

                            timeout_rqs.push(TimeoutRequest {
                                time_made: current_timestamp,
                                timeout,
                                info: timeout_kind,
                            });
                        }

                        if self.pending_timeouts.contains_key(&final_timestamp) {
                            self.pending_timeouts.get_mut(&final_timestamp).unwrap()
                                .append(&mut timeout_rqs);
                        } else {
                            self.pending_timeouts.insert(final_timestamp, timeout_rqs);
                        }
                    }
                    MessageType::MessagesReceived(message) => {

                        let mut fast_rq_info = BTreeSet::new();

                        for timeout in message {
                            if let TimeoutKind::ClientRequestTimeout(rq_info) = timeout {
                                fast_rq_info.insert(rq_info);
                            }
                        }

                        for timeout_reqs in self.pending_timeouts.values_mut() {
                            timeout_reqs.retain(|rq| {

                                //retain all values that are not in the set to be removed
                                if let TimeoutKind::ClientRequestTimeout(rq_info) = &rq.info {
                                    fast_rq_info.contains(rq_info)
                                } else {
                                    true
                                }
                            });
                        }
                    }
                }
            }

            /// run timeouts
            ///
            let current_timestamp = Utc::now().timestamp_millis() as u64;

            let mut to_time_out = vec![];

            //Get the smallest timeout (which should be closest to our current time)
            while let Some((timeout, _)) = self.pending_timeouts.first_key_value() {
                if *timeout > current_timestamp {
                    //The time has not yet reached this value, so no timeout after it
                    //Needs to be considered, since they are all larger
                    break;
                }

                let (_, mut timeouts) = self.pending_timeouts.pop_first().unwrap();

                to_time_out.append(&mut timeouts);
            }

            if !to_time_out.is_empty() {

                //Get the underlying request information
                let to_time_out = to_time_out.into_iter().map(|req| {
                    req.info
                }).collect();

                if let Err(_) = self.loopback_channel.push_request_sync(Message::Timeout(to_time_out)) {
                    info!("Loopback channel has disconnected, disconnecting timeouts thread");

                    break;
                }
            }
        }
    }
}

impl ClientRqInfo {
    pub fn new(node_id: NodeId, session: SeqNo, seq_no: SeqNo) -> Self {
        Self {
            client_id: node_id,
            req_session: session,
            req_seq_no: seq_no,
        }
    }
}

impl PartialEq for TimeoutKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ClientRequestTimeout(client_1), Self::ClientRequestTimeout(client_2)) => {
                return client_1 == client_2;
            }
            (Self::Cst(seq_no_1), Self::Cst(seq_no_2)) => {
                return seq_no_1 == seq_no_2;
            }
            (_, _) => {
                false
            }
        }
    }
}
