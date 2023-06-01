use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use chrono::Utc;
use intmap::{Entry, IntMap};
use log::info;

use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx, TryRecvError};
use febft_common::crypto::hash::Digest;
use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_execution::serialize::SharedData;

use crate::messages::{ClientRqInfo, Message};
use crate::request_pre_processing::operation_key_raw;

use super::{ReceivedRequest, RqTimeout, RqTimeoutMessage};
use super::{TimeoutKind, TimeoutMessage};
use super::CHANNEL_SIZE;
use super::TimeoutPhase;
use super::TimeoutWorkerId;

pub(super) type TimeoutWorkerMessage = TimeoutMessage;

const ITERATION_DELAY: Duration = Duration::from_millis(1);

/// A given timeout request
struct TimeoutRequest {
    time_made: u64,
    timeout: Duration,
    notifications_needed: u32,
    notifications_received: BTreeSet<NodeId>,
    info: TimeoutKind,
}


/// Timeout information about a given client request
#[derive(Clone, Debug)]
struct ClientRqTimeoutInfo {
    seq_no: SeqNo,
    // This is cached here so we don't have to traverse the entire pending_timeouts map
    timeout_time: u64,
    timeout_phase: TimeoutPhase,
    timeout_info: ClientRqInfo,
}

pub(super) struct TimeoutWorker<D: SharedData + 'static> {
    my_node_id: NodeId,
    worker_id: TimeoutWorkerId,
    default_timeout: Duration,

    // Work reception channel
    work_rx: ChannelSyncRx<TimeoutMessage>,

    // A list of all of the watched requests
    client_watched_requests: IntMap<ClientRqTimeoutInfo>,
    // Requests that are pending timeouts
    pending_timeouts: BTreeMap<u64, Vec<TimeoutRequest>>,

    // Channel to deliver the timeouts to the main thread
    loopback_channel: ChannelSyncTx<Message<D>>,
}

impl<D: SharedData + 'static> TimeoutWorker<D> {
    pub(super) fn new(worker_id: TimeoutWorkerId, node_id: NodeId, default_timeout: Duration, loopback: ChannelSyncTx<Message<D>>) -> ChannelSyncTx<TimeoutMessage> {
        let (work_tx, work_rx) = channel::new_bounded_sync(CHANNEL_SIZE);

        let worker = Self {
            my_node_id: node_id,
            worker_id,
            default_timeout,
            work_rx,
            client_watched_requests: Default::default(),
            pending_timeouts: Default::default(),
            loopback_channel: loopback,
        };

        std::thread::Builder::new()
            .name(format!("Timeout-Worker-{}", worker_id))
            .spawn(move || {
                worker.run();
            }).expect("Failed to launch timeout worker thread");

        work_tx
    }

    fn run(mut self) {
        loop {
            let mut message = match self.work_rx.recv_timeout(ITERATION_DELAY) {
                Ok(message) => { Some(message) }
                Err(TryRecvError::Timeout) => {
                    None
                }
                Err(err) => {
                    info!("Timeout worker #{:?} // Timeouts received error from recv {:?}, shutting down", self.worker_id, err);

                    break;
                }
            };

            if let Some(work_message) = message {
                self.process_work_message(work_message);
            }
            
            self.check_current_timeouts();
        }
    }

    fn process_work_message(&mut self, message: TimeoutWorkerMessage) {
        match message {
            TimeoutWorkerMessage::TimeoutRequest(request_message) => {
                self.handle_message_timeout_request(request_message, None);
            }
            TimeoutWorkerMessage::MessagesReceived(received_messages) => {
                self.handle_messages_received(received_messages);
            }
            TimeoutWorkerMessage::ResetClientTimeouts(new_timeout_dur) => {
                self.handle_reset_client_timeouts(new_timeout_dur);
            }
            TimeoutWorkerMessage::ClearClientTimeouts(client_rqs) => {
                self.handle_clear_client_rqs(client_rqs)
            }
            TimeoutWorkerMessage::ClearCstTimeouts(seq) => {
                self.handle_clear_cst_rqs(seq);
            }
        }
    }

    fn check_current_timeouts(&mut self) {
        // run timeouts
        let current_timestamp = Utc::now().timestamp_millis() as u64;

        let mut to_time_out = Vec::new();

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
            let mut timeout_per_phase = BTreeMap::new();

            //Get the underlying request information
            let to_time_out = to_time_out.into_iter().map(|req| {
                let timeout = req.info;

                let info = self.get_rq_timeout_info(&timeout);

                let timeouts = timeout_per_phase.entry(info.timeout_count()).or_insert_with(Vec::new);

                timeouts.push(timeout.clone());

                RqTimeout {
                    timeout_kind: timeout,
                    timeout_phase: info,
                }
            }).collect();

            for (phase, timeout) in timeout_per_phase {
                let message = RqTimeoutMessage {
                    timeout: self.default_timeout,
                    notifications_needed: 1,
                    timeout_info: timeout,
                };

                // Re add the messages to the timeouts
                self.handle_message_timeout_request(message, Some(TimeoutPhase::TimedOut(phase + 1, Instant::now())));
            }

            if let Err(_) = self.loopback_channel.send(Message::Timeout(to_time_out)) {
                info!("Loopback channel has disconnected, disconnecting timeouts thread");
            }
        }
    }

    /// Get the timeout information for a given request
    fn get_rq_timeout_info(&mut self, timeout_kind: &TimeoutKind) -> TimeoutPhase {
        match timeout_kind {
            TimeoutKind::ClientRequestTimeout(client_rq) => {
                let operation_key = operation_key_raw(client_rq.sender, client_rq.session);

                if let Some(info) = self.client_watched_requests.remove(operation_key) {
                    return info.timeout_phase;
                }
            }
            TimeoutKind::Cst(rq) => {}
        }

        TimeoutPhase::TimedOut(0, Instant::now())
    }


    fn handle_message_timeout_request(&mut self, message: RqTimeoutMessage, phase: Option<TimeoutPhase>) {
        let RqTimeoutMessage {
            timeout,
            notifications_needed,
            mut timeout_info
        } = message;
        
        let current_timestamp = Utc::now().timestamp_millis() as u64;

        let final_timestamp = current_timestamp + timeout.as_millis() as u64;

        let mut timeout_rqs = Vec::with_capacity(timeout_info.len());

        let final_phase = phase.clone().unwrap_or(TimeoutPhase::TimedOut(0, Instant::now()));

        'outer: for timeout_kind in timeout_info {
            if !self.register_request(&timeout_kind, final_phase.clone(), final_timestamp) {
                continue 'outer;
            }

            let mut timeout_rq = TimeoutRequest {
                time_made: current_timestamp,
                timeout,
                notifications_needed,
                notifications_received: Default::default(),
                info: timeout_kind,
            };

            timeout_rqs.push(timeout_rq);
        }

        self.pending_timeouts.insert(final_timestamp, timeout_rqs);
    }

    /// Register the timeout request into the queue
    fn register_request(&mut self, timeout: &TimeoutKind, timeout_phase: TimeoutPhase, timestamp: u64) -> bool {
        let (registered, to_delete) = match timeout {
            TimeoutKind::ClientRequestTimeout(client_rq) => {
                let operation_key = operation_key_raw(client_rq.sender, client_rq.session);

                match self.client_watched_requests.entry(operation_key) {
                    Entry::Occupied(occupied) => {
                        let mut info = occupied.into_mut();

                        if info.seq_no < client_rq.seq_no {

                            // Remove the timeout from the pending timeouts, since we have already receive a newer request, meaning this
                            // Request has already been answered by the system
                            let to_remove = info.clone();

                            info.update_with_timeout(client_rq.clone(), timeout_phase, timestamp);

                            (true, Some(to_remove))
                        } else {
                            // We have already seen a more recent request
                            (false, None)
                        }
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(ClientRqTimeoutInfo::from_timeout_and_rq(client_rq.clone(), timeout_phase, timestamp));

                        (true, None)
                    }
                }
            }
            TimeoutKind::Cst(rq) => {
                (true, None)
            }
        };

        if let Some(info) = to_delete {
            self.remove_timeout_from_pending(&info);
        }

        return registered;
    }

    fn handle_messages_received(&mut self, message: ReceivedRequest) {

        let mut cleared_requests = 0;

        match message {
            ReceivedRequest::PrePrepareRequestReceived(sender, pre_prepare) => {
                for client_request in pre_prepare {

                    let operation_key = operation_key_raw(client_request.sender, client_request.session);

                    let should_remove_timeout = if let Some(info) = self.client_watched_requests.get_mut(operation_key) {
                        if info.seq_no > client_request.seq_no {
                            // We have already seen a newer request, ignore this one
                            None
                        } else if info.seq_no == client_request.seq_no {
                            // We want to remove the timeout associated with this request

                            cleared_requests+=1;

                            Some(info.clone())
                        } else {
                            // We have a new request, we need to update the timeout
                            // But since this is a seen request, we don't want to add a timeout to it

                            let to_return = info.clone();

                            info.update_from_decided(client_request);

                            Some(to_return)
                        }
                    } else {
                        None
                    };

                    if let Some(info) = should_remove_timeout {
                        self.remove_timeout_from_pending(&info);
                    }
                }
            }
            ReceivedRequest::Cst(sender, message) => {
                for timeout_requests in self.pending_timeouts.values_mut() {
                    timeout_requests.drain_filter(|timeout_rq| {
                        if let TimeoutKind::Cst(cst_rq) = &timeout_rq.info {
                            if *cst_rq == message {
                                return timeout_rq.register_received_from(sender.clone());
                            }
                        }

                        false
                    });
                }

            }
        }
    }

    /// Remove a given timeout from the pending timeouts
    fn remove_timeout_from_pending(&mut self, client_rq: &ClientRqTimeoutInfo) {

        let timeouts = self.pending_timeouts.get_mut(&client_rq.timeout_time);

        if let Some(timeouts) = timeouts {
            timeouts.iter().position(|timeout| {
                if let TimeoutKind::ClientRequestTimeout(rq) = &timeout.info {
                    let client_request = &client_rq.timeout_info;

                    return !(rq.sender == client_request.sender && rq.session == client_request.session && rq.seq_no == client_request.seq_no);
                }

                false
            }).map(|index| {
                // Remove the timeout from the list
                timeouts.swap_remove(index);
            });
        }
    }

    /// Remove all of the timeouts that are present in the given list (or all timeouts if there is no list)
    fn handle_clear_client_rqs(&mut self, requests: Option<Vec<ClientRqInfo>>) {
        for timeouts in self.pending_timeouts.values_mut() {
            timeouts.drain_filter(|rq| {
                match &rq.info {
                    TimeoutKind::ClientRequestTimeout(rq_info) => {
                        return if let Some(requests) = &requests {
                            return requests.contains(&rq_info);
                        } else {
                            //We want to delete all of the client request timeouts
                            true
                        };
                    }
                    TimeoutKind::Cst(_) => {
                        false
                    }
                }
            });
        }
    }

    /// Remove all CST timeout requests that match the given sequence number (or all timeouts if there is no sequence number)
    fn handle_clear_cst_rqs(&mut self, seq_no: Option<SeqNo>) {
        for timeout_rqs in self.pending_timeouts.values_mut() {
            timeout_rqs.drain_filter(|rq| {
                match &rq.info {
                    TimeoutKind::ClientRequestTimeout(_) => {
                        false
                    }
                    TimeoutKind::Cst(rq_seq_no) => {
                        return if let Some(seq_no) = &seq_no {
                            return *seq_no != *rq_seq_no;
                        } else {
                            // We want to delete all of the cst timeouts
                            true
                        };
                    }
                }
            });
        }
    }

    ///
    fn handle_reset_client_timeouts(&mut self, timeout_dur: Duration) {

        let mut timeouts_cleared = Vec::with_capacity(self.pending_timeouts.len());

        //Clear all of the pending timeouts
        for timeouts in self.pending_timeouts.values_mut() {
            timeouts.drain_filter(|rq| {
                match &rq.info {
                    TimeoutKind::ClientRequestTimeout(_) => {
                        true
                    }
                    TimeoutKind::Cst(_) => {
                        false
                    }
                }
            }).for_each(|timeout| {
                timeouts_cleared.push(timeout.info);
            });
        }

        let timeout_phase = TimeoutPhase::TimedOut(0, Instant::now());
        let timestamp = Utc::now().timestamp_millis() as u64;

        let mut timeouts = Vec::with_capacity(timeouts_cleared.len());

        for timeout in timeouts_cleared {
            if let TimeoutKind::ClientRequestTimeout(rq_info) = timeout {

                let operation_key = operation_key_raw(rq_info.sender, rq_info.session);

                if let Some(info) = self.client_watched_requests.get_mut(operation_key) {

                    if info.seq_no <= rq_info.seq_no {

                        info.update_with_timeout(rq_info.clone(), timeout_phase.clone(), timestamp);

                        let to_timeout = TimeoutRequest {
                            time_made: timestamp,
                            timeout: timeout_dur,
                            notifications_needed: 1,
                            notifications_received: Default::default(),
                            info: TimeoutKind::ClientRequestTimeout(rq_info),
                        };

                        timeouts.push(to_timeout);
                    }
                }
            }
        }

        self.pending_timeouts.insert(timestamp, timeouts);
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

impl ClientRqTimeoutInfo {

    fn from_timeout_and_rq(rq: ClientRqInfo, timeout_phase: TimeoutPhase, timestamp: u64) -> Self {
        Self {
            seq_no: rq.seq_no,
            timeout_time: timestamp,
            timeout_phase,
            timeout_info: rq,
        }
    }

    fn update_with_timeout(&mut self, seen: ClientRqInfo,
                           timeout: TimeoutPhase, timestamp: u64) {
        self.seq_no = seen.seq_no;
        self.timeout_phase = timeout;
        self.timeout_time = timestamp;
        self.timeout_info = seen;
    }

    fn update_from_decided(&mut self, seen: ClientRqInfo) {
        self.seq_no = seen.seq_no;
        self.timeout_time = 0;
        self.timeout_phase = TimeoutPhase::TimedOut(0, Instant::now());
        self.timeout_info = seen;
    }
}