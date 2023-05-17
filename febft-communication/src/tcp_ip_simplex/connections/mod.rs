pub mod conn_establish;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, AtomicUsize};
use std::time::Instant;
use intmap::IntMap;
use log::error;
use febft_common::channel::{ChannelMixedRx, ChannelMixedTx};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use crate::client_pooling::{ConnectedPeer, PeerIncomingRqHandling};
use crate::message::{NetworkMessage, WireMessage};
use crate::serialize::Serializable;
use crate::tcpip::connections::{Callback, ConnHandle, NetworkSerializedMessage};
use crate::tcpip::PeerAddr;

pub struct SimplexConnections<M: Serializable + 'static> {
    id: NodeId,
    first_cli: NodeId,
    address_map: IntMap<PeerAddr>,

    client_pooling_ref: Arc<PeerIncomingRqHandling<NetworkMessage<M>>>,
}

pub struct PeerConnection<M: Serializable + 'static> {
    peer_node_id: NodeId,
    //A handle to the request buffer of the peer we are connected to in the client pooling module
    client: Arc<ConnectedPeer<NetworkMessage<M>>>,
    //The channel used to send serialized messages to the tasks that are meant to handle them
    tx: ChannelMixedTx<NetworkSerializedMessage>,
    // The RX handle corresponding to the tx channel above. This is so we can quickly associate new
    // TX connections to a given connection, as we just have to clone this handle
    rx: ChannelMixedRx<NetworkSerializedMessage>,
    // Controls the incoming connections
    outgoing_connections: Connections,
    // Controls the outgoing connections
    incoming_connections: Connections,
}

pub struct Connections {
    // Counter to assign unique IDs to each of the underlying Tcp streams
    conn_id_generator: AtomicU32,
    // A map to manage the currently active connections and a cached size value to prevent
    // concurrency for simple length checks
    active_connection_count: AtomicUsize,
    active_connections: Mutex<BTreeMap<u32, ConnHandle>>,
}

impl<M> PeerConnection<M> where M: Serializable + 'static {

    /// Send a message through this connection. Only valid for peer connections
    pub(crate) fn peer_message(&self, msg: WireMessage, callback: Callback, should_flush: bool, send_rq_time: Instant) -> Result<()> {

        let from = msg.header().from();
        let to = msg.header().to();

        if let Err(_) = self.tx.send((msg, callback, Instant::now(), should_flush, send_rq_time)) {
            error!("{:?} // Failed to send peer message to {:?}", from,
                to);

            return Err(Error::simple(ErrorKind::Communication));
        }

        Ok(())
    }

    async fn peer_msg_return_async(&self,to_send: NetworkSerializedMessage) -> Result<()> {
        let send = self.tx.clone();

        if let Err(_) = send.send_async(to_send).await {
            return Err(Error::simple(ErrorKind::Communication));
        }

        Ok(())
    }



}