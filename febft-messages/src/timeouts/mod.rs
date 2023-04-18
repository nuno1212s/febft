use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use chrono::Utc;
use log::{info, warn};
use febft_common::{channel, collections};
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx, TryRecvError};
use febft_common::collections::HashMap;
use febft_common::crypto::hash::Digest;
use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_execution::app::Service;
use febft_execution::serialize::SharedData;
use crate::messages::Message;
use crate::serialize::OrderingProtocolMessage;

const CHANNEL_SIZE: usize = 1024;

///Contains the requests that have just been timed out
pub type Timeout = Vec<TimeoutKind>;

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct ClientRqInfo {
    //The digest of the request in question
    pub digest: Digest,

    pub seqno: SeqNo,
    pub session: SeqNo,
}

#[derive(Eq, Ord, PartialOrd, Debug)]
pub enum TimeoutKind {
    ///Relates to the timeout of a client request.
    /// Stores the client who sent it, along with the request
    /// session and request sequence number
    ClientRequestTimeout(ClientRqInfo),

    ///TODO: Maybe add a timeout for synchronizer messages?
    /// Having a timeout for STOP messages is essential for liveness
    //Sync(),

    /// As for CST messages, these messages aren't particularly ordered, they are just
    /// for each own node to know to what messages the peers are responding to.
    Cst(SeqNo),
}

struct TimeoutRequest {
    time_made: u64,
    timeout: Duration,
    notifications_needed: u32,
    notifications_received: BTreeSet<NodeId>,
    info: TimeoutKind,
}

type TimeoutMessage = MessageType;

enum MessageType {
    TimeoutRequest(RqTimeoutMessage),
    MessagesReceived(ReceivedRequest),
    ClearClientTimeouts(Option<Vec<Digest>>),
    ClearCstTimeouts(Option<SeqNo>),
}

enum ReceivedRequest {
    //The node that proposed the message and all the requests contained within it
    PrePrepareRequestReceived(NodeId, Vec<ClientRqInfo>),
    //Receive a CST message relating to the following sequence number from the given
    //Node
    Cst(NodeId, SeqNo),
}

struct RqTimeoutMessage {
    timeout: Duration,
    notifications_needed: u32,
    timeout_info: Timeout,
}

#[derive(Clone)]
pub struct Timeouts {
    handle: ChannelSyncTx<TimeoutMessage>,
}

/// This structure is responsible for handling timeouts for the entire project
/// This includes timing out messages exchanged between replicas and between clients
struct TimeoutsThread<D: SharedData + 'static> {
    my_id: NodeId,
    //Stores the pending timeouts, grouped by the time at which they timeout.
    //Iterating a binary tree is pretty quick and it keeps the elements ordered
    //So we can use that to our advantage when timing out requests
    pending_timeouts: BTreeMap<u64, Vec<TimeoutRequest>>,
    // Requests that we have already seen but have not been requested to timeout
    // So when we receive the timeout request we can instantly cancel it
    done_requests: HashMap<Digest, Vec<NodeId>>,
    //Allows us to quickly find the correct bucket for the request we are looking for
    pending_timeouts_reverse_search: BTreeMap<Rc<TimeoutRequest>, u64>,
    //Receive messages from other threads
    channel_rx: ChannelSyncRx<TimeoutMessage>,
    //Loopback so we can deliver the timeouts to the main consensus thread so they can be
    //processed
    loopback_channel: ChannelSyncTx<Message<D>>,
    //How long between each timeout iteration
    iteration_delay: u64,
}


impl Timeouts {
    ///Initialize the timeouts thread and return a handle to it
    /// This handle can then be used everywhere timeouts are needed.
    pub fn new<D: SharedData + 'static>(node_id: NodeId, iteration_delay: u64,
                                        loopback_channel: ChannelSyncTx<Message<D>>) -> Self {
        let tx = TimeoutsThread::<D>::new(node_id, iteration_delay, loopback_channel);

        Self {
            handle: tx,
        }
    }

    /// Start a timeout request on the list of digests that have been provided
    pub fn timeout_client_requests(&self, timeout: Duration, requests: Vec<(Digest, SeqNo, SeqNo)>) {
        let requests: Vec<TimeoutKind> = requests.into_iter()
            .map(|(req, seq, session)|
                TimeoutKind::ClientRequestTimeout(ClientRqInfo::new(req, seq, session)))
            .collect();

        self.handle.send(TimeoutMessage::TimeoutRequest(RqTimeoutMessage {
            timeout,
            // we choose 1 here because we only need to receive one valid pre prepare containing
            // this request for it to be considered valid
            notifications_needed: 1,
            timeout_info: requests,
        })).expect("Failed to contact timeout thread")
    }

    /// Notify that a pre prepare with the following requests has been received and we must therefore
    /// Disable any timeouts pertaining to the received requests
    pub fn received_pre_prepare(&self, from: NodeId, recvd_rqs: Vec<ClientRqInfo>) {
        self.handle.send(TimeoutMessage::MessagesReceived(
            ReceivedRequest::PrePrepareRequestReceived(from, recvd_rqs)
        ))
            .expect("Failed to contact timeout thread");
    }

    /// Cancel timeouts of player requests.
    /// This accepts an option. If this Option is None, then the
    /// timeouts for all client requests are going to be disabled
    pub fn cancel_client_rq_timeouts(&self, requests_to_clear: Option<Vec<Digest>>) {
        self.handle.send(TimeoutMessage::ClearClientTimeouts(requests_to_clear))
            .expect("Failed to contact timeout thread")
    }

    /// Timeout a CST request
    pub fn timeout_cst_request(&self, timeout: Duration, requests_needed: u32, seq_no: SeqNo) {
        self.handle.send(TimeoutMessage::TimeoutRequest(RqTimeoutMessage {
            timeout,
            notifications_needed: requests_needed,
            timeout_info: vec![TimeoutKind::Cst(seq_no)],
        })).expect("Failed to contact timeout thread");
    }

    /// Handle having received a cst request
    pub fn received_cst_request(&self, from: NodeId, seq_no: SeqNo) {
        self.handle.send(TimeoutMessage::MessagesReceived(
            ReceivedRequest::Cst(from, seq_no)))
            .expect("Failed to contact timeout thread");
    }

    /// Cancel timeouts of CST messages.
    /// This accepts an option. If this Option is None, then the
    /// timeouts for all CST requests are going to be disabled.
    pub fn cancel_cst_timeout(&self, seq_no: Option<SeqNo>) {
        self.handle.send(TimeoutMessage::ClearCstTimeouts(seq_no))
            .expect("Failed to contact timeout thread");
    }
}

impl<D: SharedData + 'static> TimeoutsThread<D> {
    fn new(node_id: NodeId, iteration_delay: u64, loopback_channel: ChannelSyncTx<Message<D>>) -> ChannelSyncTx<TimeoutMessage> {
        let (tx, rx) = channel::new_bounded_sync(CHANNEL_SIZE);

        std::thread::Builder::new().name("Timeout Thread".to_string())
            .spawn(move || {
                let timeout_thread = Self {
                    my_id: node_id,
                    pending_timeouts: Default::default(),
                    done_requests: collections::hash_map(),
                    pending_timeouts_reverse_search: Default::default(),
                    channel_rx: rx,
                    loopback_channel,
                    iteration_delay,
                };

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

            //Handle all incoming messages and update the pending timeouts accordingly
            if let Some(mut message) = message {
                match message {
                    MessageType::TimeoutRequest(timeout_rq) => {
                        self.handle_message_timeout_request(timeout_rq);
                    }
                    MessageType::MessagesReceived(message) => {
                        self.handle_messages_received(message);
                    }
                    MessageType::ClearClientTimeouts(requests) => {
                        self.handle_clear_client_rqs(requests);
                    }
                    MessageType::ClearCstTimeouts(seq_no) => {
                        self.handle_clear_cst_rqs(seq_no);
                    }
                }
            }

            // run timeouts
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

                if let Err(_) = self.loopback_channel.send(Message::Timeout(to_time_out)) {
                    info!("Loopback channel has disconnected, disconnecting timeouts thread");

                    break;
                }
            }
        }
    }

    fn handle_message_timeout_request(&mut self, message: RqTimeoutMessage) {
        let RqTimeoutMessage {
            timeout,
            notifications_needed,
            mut timeout_info
        } = message;

        let current_timestamp = Utc::now().timestamp_millis() as u64;

        let final_timestamp = current_timestamp + timeout.as_millis() as u64;

        let mut timeout_rqs = Vec::with_capacity(timeout_info.len());

        'outer: for timeout_kind in timeout_info {
            let mut timeout_rq = TimeoutRequest {
                time_made: current_timestamp,
                timeout,
                notifications_needed,
                notifications_received: Default::default(),
                info: timeout_kind,
            };

            match &timeout_rq.info {
                TimeoutKind::ClientRequestTimeout(req) => {
                    if let Some(reqs) = self.done_requests.remove(&req.digest) {
                        for x in reqs {
                            if timeout_rq.register_received_from(x) {
                                //If we have all the needed messages, then we can continue
                                continue 'outer;
                            }
                        }
                    }
                }
                _ => {}
            }

            timeout_rqs.push(timeout_rq);
        }

        if self.pending_timeouts.contains_key(&final_timestamp) {
            self.pending_timeouts.get_mut(&final_timestamp).unwrap()
                .append(&mut timeout_rqs);
        } else {
            self.pending_timeouts.insert(final_timestamp, timeout_rqs);
        }
    }

    fn handle_messages_received(&mut self, mut received_request: ReceivedRequest) {

        if let ReceivedRequest::PrePrepareRequestReceived(node, _) = &received_request {
            if *node == self.my_id {
                // We don't receive timeouts from requests in our own associated space
                return;
            }
        }

        for timeout_requests in self.pending_timeouts.values_mut() {
            timeout_requests.retain_mut(|rq| {
                return match (&rq.info, &mut received_request) {
                    (TimeoutKind::ClientRequestTimeout(rq_info),
                        ReceivedRequest::PrePrepareRequestReceived(received, rqs)) => {
                        if let Some(index) = rqs.iter().position(|digest| { digest.digest == rq_info.digest }) {
                            rqs.swap_remove(index);

                            !rq.register_received_from(received.clone())
                        } else {
                            true
                        }
                    }
                    (TimeoutKind::Cst(seq_no), ReceivedRequest::Cst(from, seq_no_2)) => {
                        if *seq_no == *seq_no_2 {
                            !rq.register_received_from(from.clone())
                        } else {
                            true
                        }
                    }
                    (_, _) => { true }
                };
            });
        }

        match received_request {
            ReceivedRequest::PrePrepareRequestReceived(node, ids) => {
                if !ids.is_empty() {
                    // This means we have not yet seen these requests (in the proposer).\
                    // As soon as we see them, they will be skipped over
                    warn!("{:?} // Received requests but did not have a timeout for them: {:?}, {:?}.", self.my_id, node, ids);

                    for rq_id in ids {
                        self.done_requests.entry(rq_id.digest).or_insert_with(|| {
                            vec![]
                        }).push(node.clone());
                    }
                }
            }
            _ => {}
        }
    }

    fn handle_clear_client_rqs(&mut self, requests: Option<Vec<Digest>>) {
        for timeout_rqs in self.pending_timeouts.values_mut() {
            timeout_rqs.retain(|rq| {
                match &rq.info {
                    TimeoutKind::ClientRequestTimeout(rq_info) => {
                        if let Some(requests) = &requests {
                            return !requests.contains(&rq_info.digest);
                        }

                        //We want to delete all of the client request timeouts
                        false
                    }
                    TimeoutKind::Cst(_) => {
                        true
                    }
                }
            });
        }
    }

    fn handle_clear_cst_rqs(&mut self, seq_no: Option<SeqNo>) {
        for timeout_rqs in self.pending_timeouts.values_mut() {
            timeout_rqs.retain(|rq| {
                match &rq.info {
                    TimeoutKind::ClientRequestTimeout(_) => {
                        true
                    }
                    TimeoutKind::Cst(rq_seq_no) => {
                        if let Some(seq_no) = &seq_no {
                            return *seq_no == *rq_seq_no;
                        }

                        false
                    }
                }
            });
        }
    }
}

impl ClientRqInfo {
    pub fn new(digest: Digest, seqno: SeqNo, session: SeqNo) -> Self {
        Self {
            digest,
            seqno,
            session,
        }
    }
}

impl TimeoutRequest {
    fn is_disabled(&self) -> bool {
        return self.notifications_needed <= self.notifications_received.len() as u32;
    }

    fn register_received_from(&mut self, from: NodeId) -> bool {
        self.notifications_received.insert(from);

        return self.is_disabled();
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
