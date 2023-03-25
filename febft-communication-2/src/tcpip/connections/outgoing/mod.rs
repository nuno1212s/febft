use febft_common::channel::ChannelMixedRx;
use febft_common::socket::{SecureWriteHalf, SecureWriteHalfSync};

use crate::tcpip::connections::{PeerConnection, SerializedMessage};

pub mod asynchronous;
pub mod synchronous;

pub (super) fn spawn_outgoing_task_handler(connection_rx: ChannelMixedRx<SerializedMessage>,
                                           socket: SecureWriteHalf) {
    match socket {
        SecureWriteHalf::Async(asynchronous) => {
            asynchronous::spawn_outgoing_task(connection_rx, asynchronous);
        }
        SecureWriteHalf::Sync(synchronous) => {
            synchronous::spawn_outgoing_thread(connection_rx, synchronous);
        }
    }
}