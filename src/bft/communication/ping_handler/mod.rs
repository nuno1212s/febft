use std::collections::BTreeMap;
use std::sync::{Mutex, Arc};
use std::time::Duration;
use chrono::{DurationRound, Utc};
use dashmap::DashMap;
use log::error;
use crate::bft::communication::{Node, NodeId, SendNode};
use crate::bft::communication::channel::{ChannelAsyncRx, ChannelMixedRx, ChannelMixedTx, ChannelSyncTx, new_bounded_mixed, new_bounded_mult, new_bounded_sync};
use crate::bft::communication::message::SystemMessage;
use crate::bft::communication::serialize::SharedData;
use crate::bft::error::*;

struct PingInfo {
    tx: ChannelMixedTx<PingResponse>,
    time_sent: u128
}

pub type PingInformation = PingInfo;

pub type PingResponse = Result<()>;

pub type PingResponseReceiver = ChannelMixedRx<PingResponse>;

pub const PING_TIMEOUT : u128 = 2500;

const PING_INTERVAL: u64 = 500;

/// Handles pinging other nodes
/// Will timeout the ping after a set timeout.
/// To receive the ping request, one must use the
pub struct PingHandler<D> where D: SharedData + 'static {
    //A map detailing the pings that are still awaiting response
    awaiting_response: Mutex<BTreeMap<u64, PingInformation>>,
}

impl<D> PingHandler<D> where D: SharedData + 'static {

    /// Initialize the ping handler
    pub fn new() -> Arc<Self> {
        let ping_handler = Arc::new(Self {
            awaiting_response: Mutex::new(Default::default())
        });

        ping_handler.clone().start_timeout_thread();

        ping_handler
    }

    /// Ping a peer node
    /// Returns a response receiver that you should receive from to
    /// receive the response to the peer request
    /// The returned channel is blocking on send
    pub fn ping_peer(&self, node: &Arc<Node<D>>, peer_id: NodeId) -> Result<PingResponseReceiver> {
        let (tx, rx) = new_bounded_mixed(1);

        {
            let awaiting_response = self.awaiting_response.lock().unwrap();

            if awaiting_response.contains_key(&peer_id.into()) {
                Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler, "Already attempting to ping this peer"))
            }

            let time = Utc::now().timestamp_millis();

            awaiting_response.insert(peer_id.into(), PingInformation { tx: tx,
                time_sent: time as u128 });
        }

        node.send(SystemMessage::Ping, peer_id, true);

        Ok(rx)
    }

    /// Handles a received ping from other replicas.
    pub fn handle_ping_response(&self, peer_id: NodeId) {
        let response = {
            let awaiting_response = self.awaiting_response.lock().unwrap();

            awaiting_response.remove(&peer_id.into())
        };

        if let Some(information) = response {

            let ping_response = Ok(());

            information.tx.send(ping_response);
        } else {
            error!("Received ping that was not requested? {:?}", peer_id);
        }
    }

    /// Start the thread responsible for performing timeout checks
    fn start_timeout_thread(self: Arc<Self>) {
        std::thread::Builder::new().name("Ping Timeout Thread")
            .spawn(|| {
                loop {
                    self.handle_ping_timeouts();

                    std::thread::sleep(Duration::from_millis(PING_INTERVAL));
                }

            });

    }

    ///Perform a check on whether any pending ping request has timed out
    fn handle_ping_timeouts(&self) {
        let current_time = Utc::now().timestamp_millis() as u128;

        let to_remove = Vec::new();

        let awaiting_pings_lock = self.awaiting_response.lock().unwrap();

        for (id, ping_info) in awaiting_pings_lock.iter() {
            let time_sent = ping_info.time_sent;

            if current_time - time_sent >= PING_TIMEOUT {
                ping_info.tx.send(
                    Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler,
                                               "Ping request has timed out")));

                to_remove.push(id.clone());
            }
        }

        for id in to_remove {
            awaiting_pings_lock.remove(&id);
        }
    }
}