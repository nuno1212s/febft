use std::collections::BTreeMap;
use std::sync::{Mutex, Arc};
use std::time::Duration;
use chrono::{Utc};

use log::{debug, error};
use crate::bft::communication::{Node, NodeId};
use crate::bft::communication::channel::{ChannelMixedRx, ChannelMixedTx, new_bounded_mixed};
use crate::bft::communication::message::{PingMessage, SystemMessage};
use crate::bft::communication::serialize::SharedData;
use crate::bft::error::*;

pub struct PingInfo {
    tx: ChannelMixedTx<PingResponse>,
    time_sent: u128,
}

pub type PingInformation = PingInfo;

pub type PingResponse = Result<()>;

pub type PingResponseReceiver = PingRespReceiver;

/// The receiver of ping responses
pub struct PingRespReceiver {
    rx: ChannelMixedRx<PingResponse>
}

pub const PING_TIMEOUT: u128 = 100;

const PING_INTERVAL: u64 = 50;

/// Handles pinging other nodes
/// Will timeout the ping after a set timeout.
/// To receive the ping request, one must use the
pub struct PingHandler {
    //A map detailing the pings that are still awaiting response
    awaiting_response: Mutex<BTreeMap<u64, PingInformation>>,
}

impl PingHandler {
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
    pub fn ping_peer<D>(&self, node: &Arc<Node<D>>, peer_id: NodeId) -> Result<PingResponseReceiver> where D: SharedData + 'static {
        let (tx, rx) = new_bounded_mixed(1);

        {
            let mut awaiting_response = self.awaiting_response.lock().unwrap();

            if awaiting_response.contains_key(&peer_id.into()) {
                return Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler, "Already attempting to ping this peer"));
            }

            let time = Utc::now().timestamp_millis();

            awaiting_response.insert(peer_id.into(), PingInformation {
                tx,
                time_sent: time as u128,
            });
        }

        debug!("Pinging the node {:?}", peer_id);

        node.send(SystemMessage::Ping(PingMessage::new(true)), peer_id, true);

        Ok(PingRespReceiver {
            rx
        })
    }

    /// Handles a received ping from other replicas.
    pub fn handle_ping_received<D>(&self, node: &Arc<Node<D>>,
                                   ping: &PingMessage,
                                   peer_id: NodeId) where D: SharedData + 'static {
        let response = {
            let mut awaiting_response = self.awaiting_response.lock().unwrap();

            awaiting_response.remove(&peer_id.into())
        };

        if ping.is_request() {
            debug!("Received ping request from node {:?}, sending ping response", peer_id);

            node.send(SystemMessage::Ping(PingMessage::new(false)), peer_id, true);
        } else {
            if let Some(information) = response {
                let ping_response = Ok(());

                information.tx.send(ping_response).unwrap();

                debug!("Received ping response from peer {:?}", peer_id);
            } else {
                error!("Received ping that was not requested? {:?}", peer_id);
            }
        }
    }

    /// Start the thread responsible for performing timeout checks
    fn start_timeout_thread(self: Arc<Self>) {
        std::thread::Builder::new().name("Ping Timeout Thread".to_string())
            .spawn(move || {
                loop {
                    self.handle_ping_timeouts();

                    std::thread::sleep(Duration::from_millis(PING_INTERVAL));
                }
            })
            .expect("Failed to allocate ping timeout thread");
    }

    ///Perform a check on whether any pending ping request has timed out
    fn handle_ping_timeouts(&self) {
        let current_time = Utc::now().timestamp_millis() as u128;

        let mut to_remove = Vec::new();

        let mut awaiting_pings_lock = self.awaiting_response.lock().unwrap();

        for (id, ping_info) in awaiting_pings_lock.iter() {
            let time_sent = ping_info.time_sent;

            if current_time - time_sent >= PING_TIMEOUT {
                ping_info.tx.send(
                    Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler,
                                               "Ping request has timed out"))).unwrap();

                debug!("Timed out ping to node {:?}", id);

                to_remove.push(id.clone());
            }
        }

        for id in to_remove {
            awaiting_pings_lock.remove(&id);
        }
    }
}

impl PingRespReceiver {

    pub fn recv_resp(&self) -> Result<()> {
        match self.rx.recv() {
            Ok(ping_response) => {
                match ping_response {
                    Ok(_) => {
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            Err(err) => {
                Err(Error::wrapped(ErrorKind::CommunicationPingHandler, err))
            }
        }
    }

    pub async fn recv_resp_async(&mut self) -> Result<()> {
        match self.rx.recv_async().await {
            Ok(ping_response) => {
                match ping_response {
                    Ok(_) => {
                        Ok(())
                    }
                    Err(err) => {
                        Err(err)
                    }
                }
            }
            Err(err) => {
                Err(Error::wrapped(ErrorKind::CommunicationPingHandler, err))
            }
        }
    }

}