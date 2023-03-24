use log::error;
use febft_common::channel::ChannelMixedRx;
use febft_common::socket::SecureSocketSendSync;
use crate::tcpip::connections::SerializedMessage;

pub (super) fn spawn_outgoing_thread(mut rx: ChannelMixedRx<SerializedMessage>, mut socket: SecureSocketSendSync) {

    std::thread::Builder::new()
        .name(format!("Outgoing connection thread"))
        .spawn(move || {
            loop {
                let to_send = match rx.recv() {
                    Ok(message) => {message}
                    Err(error_kind) => {
                        error!("Failed to receive message to send. {:?}", error_kind);

                        break;
                    }
                };

                match to_send.write_to_sync(&mut socket, true) {
                    Ok(_) => {
                        //TODO: Statistics
                    }
                    Err(error_kind) => {
                        error!("Failed to write message to socket. {:?}", error_kind);

                        break
                    }
                }
            }
        }).unwrap();

}