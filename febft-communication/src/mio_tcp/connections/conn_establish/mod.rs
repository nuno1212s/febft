use febft_common::socket::{MioListener, SyncListener};
use crate::mio_tcp::connections::epoll_workers::would_block;

pub struct ConnectionAcceptor {



}

fn accept_connections(&mut listener: MioListener) {
    loop {

        match listener.accept() {
            Ok((socket, addr)) => {



            }
            Err(err) if would_block(&err) => {
                // No more connections are ready to be accepted
                break;
            }
        }

    }

}