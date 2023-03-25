use log::error;

use febft_common::async_runtime as rt;
use febft_common::channel::ChannelMixedRx;
use febft_common::error::*;
use febft_common::socket::SecureWriteHalfAsync;

use crate::tcpip::connections::SerializedMessage;

pub (super) fn spawn_outgoing_task(mut rx: ChannelMixedRx<SerializedMessage>,
                                   mut socket: SecureWriteHalfAsync) {

    rt::spawn(async move {
        loop {
            let to_send = match rx.recv_async().await {
                Ok(message) => {message}
                Err(error_kind) => {
                    error!("Failed to receive message to send. {:?}", error_kind);

                    break;
                }
            };

            match to_send.write_to(&mut socket, true).await {
                Ok(_) => {
                    //TODO: Statistics
                }
                Err(error_kind) => {
                    error!("Failed to write message to socket. {:?}", error_kind);

                    break
                }
            }
        }
    });
}

