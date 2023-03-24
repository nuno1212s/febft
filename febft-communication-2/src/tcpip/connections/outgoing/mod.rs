use febft_common::channel::ChannelMixedRx;
use febft_common::socket::SecureSocketSend;
use crate::tcpip::connections::{PeerConnection, SerializedMessage};

pub mod asynchronous;
pub mod synchronous;

pub (super) fn spawn_outgoing_task_handler(connection_rx: ChannelMixedRx<SerializedMessage>,
                                           socket: SecureSocketSend) {
    match socket {
        SecureSocketSend::Async(asynchronous) => {
            asynchronous::spawn_outgoing_task(connection_rx, asynchronous.inner);
        }
        SecureSocketSend::Sync(synchronous) => {
            synchronous::spawn_outgoing_thread(connection_rx, synchronous.inner);
        }
    }
}