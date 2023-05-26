use std::collections::BTreeMap;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use chrono::Utc;
use log::{debug, error};
use febft_common::channel;
use febft_common::channel::{ChannelMixedRx, ChannelMixedTx, new_bounded_mixed, OneShotRx, OneShotTx};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use crate::message::{NetworkMessage, NetworkMessageKind, PingMessage};
use crate::Node;
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::TCPSimplexNode;

pub struct PingInfo {
    tx: OneShotTx<PingResponse>,
    time_sent: u128,
}

pub type PingInformation = PingInfo;

pub type PingResponse = Result<()>;

pub type PingResponseReceiver = PingRespReceiver;

/// The receiver of ping responses
pub struct PingRespReceiver {
    rx: OneShotRx<PingResponse>,
}

pub const PING_TIMEOUT: u128 = 100;
pub const PING_CHANNEL_SIZE: usize = 10;

const PING_INTERVAL: u64 = 50;

pub type PingChannel = ChannelMixedTx<()>;
pub type PingChannelReceiver = ChannelMixedRx<()>;

/// Handles pinging other nodes
/// Will timeout the ping after a set timeout.
/// To receive the ping request, one must use the
pub struct PingHandler {
    //A map detailing the pings that are still awaiting response
    awaiting_response: Mutex<BTreeMap<NodeId, BTreeMap<u32, PingInformation>>>,
    // A map detailing the ping channels for each node and connection
    ping_channels: Mutex<BTreeMap<NodeId, BTreeMap<u32, PingChannel>>>,
}

impl PingHandler {
    /// Initialize the ping handler
    pub fn new() -> Arc<Self> {
        let ping_handler = Arc::new(Self {
            awaiting_response: Mutex::new(Default::default()),
            ping_channels: Mutex::new(Default::default()),
        });

        ping_handler.clone().start_timeout_thread();

        ping_handler
    }

    /// Register a ping channel into the ping handler
    pub fn register_ping_channel(&self, peer_id: NodeId, conn_id: u32) -> PingChannelReceiver {
        let (tx, rx) = new_bounded_mixed(PING_CHANNEL_SIZE);

        let mut guard = self.ping_channels.lock().unwrap();

        let conns = guard.entry(peer_id).or_insert_with(BTreeMap::new);

        conns.insert(conn_id, tx);

        rx
    }

    pub fn remove_ping_channel(&self, peer_id: NodeId, conn_id: u32) {
        let mut ping_channel_guard = self.ping_channels.lock().unwrap();

        let mut should_remove = false;

        if let Some(conns) = ping_channel_guard.get_mut(&peer_id) {
            conns.remove(&conn_id);

            if conns.is_empty() {
                should_remove = true;
            }
        }

        ping_channel_guard.remove(&peer_id);
    }

    /// Ping a peer node
    /// Returns a response receiver that you should receive from to
    /// receive the response to the peer request
    /// The returned channel is blocking on send
    pub fn ping_peer(&self, peer_id: NodeId, conn_id: u32) -> Result<PingResponseReceiver> {
        let (tx, rx) = channel::new_oneshot_channel();

        {
            let mut awaiting_response = self.awaiting_response.lock().unwrap();

            let time = Utc::now().timestamp_millis();

            let map = awaiting_response.entry(peer_id).or_insert_with(BTreeMap::new);

            if map.contains_key(&conn_id) {
                return Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler, "Already attempting to ping this peer"));
            }

            map.insert(conn_id, PingInformation {
                tx,
                time_sent: time as u128,
            });
        }

        {
            let mut ping_channels = self.ping_channels.lock().unwrap();

            let map = ping_channels.get(&peer_id);

            if let Some(ping) = map {
                if let Some(ping_tx) = ping.get(&conn_id) {
                    ping_tx.send(()).unwrap();
                }
            }
        }

        Ok(PingRespReceiver {
            rx
        })
    }

    /// Handles a received ping from other replicas.
    pub fn handle_ping_received(&self, peer_id: NodeId, conn_id: u32) {
        let response = {
            let mut awaiting_response = self.awaiting_response.lock().unwrap();

            let (response, empty) = {
                let connections = awaiting_response.get_mut(&peer_id);

                if connections.is_none() {
                    return;
                }

                let connections = connections.unwrap();

                (connections.remove(&conn_id), connections.is_empty())
            };

            if empty { awaiting_response.remove(&peer_id); }

            response
        };

        if let Some(information) = response {
            let ping_response = Ok(());

            // Ignore if the receiver has been dropped
            let _ = information.tx.send(ping_response);

            debug!("Received ping response from peer {:?}", peer_id);
        } else {
            error!("Received ping that was not requested or was already timed out? {:?}", peer_id);
        }
    }

    /// Handle a ping request that has failed
    pub fn handle_ping_failed(&self, peer_id: NodeId, conn_id: u32) {
        let response = {
            let mut awaiting_response = self.awaiting_response.lock().unwrap();

            let (response, empty) = {
                let connections = awaiting_response.get_mut(&peer_id);

                if connections.is_none() {
                    return;
                }

                let connections = connections.unwrap();

                (connections.remove(&conn_id), connections.is_empty())
            };

            if empty { awaiting_response.remove(&peer_id); }

            response
        };

        if let Some(information) = response {
            let ping_response = Err(Error::simple(ErrorKind::CommunicationPingHandler));

            // Ignore if the receiver has been dropped
            information.tx.send(ping_response).unwrap();

            debug!("Received ping response from peer {:?}", peer_id);
        } else {
            error!("Received ping that was not requested or was already timed out? {:?}", peer_id);
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
            for (conn_id, ping_info) in ping_info {
                let time_sent = ping_info.time_sent;

                if current_time - time_sent >= PING_TIMEOUT {
                    to_remove.push((id.clone(), conn_id.clone()));
                }
            }
        }

        for (id, conn) in to_remove {
            let empty = {
                let id_conns = awaiting_pings_lock.get_mut(&id).unwrap();

                let ping_info = id_conns.remove(&conn).unwrap();

                // Ignore errors when delivering the response
                let _ = ping_info.tx.send(Err(Error::simple_with_msg(ErrorKind::CommunicationPingHandler, "Ping request has timed out")));

                debug!("Timed out ping to node {:?}", id);

                id_conns.is_empty()
            };

            if empty {
                awaiting_pings_lock.remove(&id);
            }
        }
    }
}

impl PingRespReceiver {
    pub fn recv_resp(self) -> Result<()> {
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

    pub async fn recv_resp_async(self) -> Result<()> {
        match self.rx.await {
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