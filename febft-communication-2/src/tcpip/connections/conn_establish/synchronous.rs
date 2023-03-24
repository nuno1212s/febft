use std::sync::Arc;
use either::Either;
use log::error;

use rustls::{ServerConfig, ServerConnection};

use febft_common::socket::SyncListener;
use crate::tcpip::connections::conn_establish::ConnectionHandler;

pub(super) fn setup_conn_acceptor_thread(tcp_listener: SyncListener, conn_handler: Arc<ConnectionHandler>) {
    std::thread::Builder::new()
        .name(format!("Connection acceptor thread"))
        .spawn(|| {
            loop {
                match tcp_listener.accept() {
                    Ok(connection) => {
                        conn_handler.accept_conn(Either::Right(connection))
                    }
                    Err(err) => {
                        error!("Failed to accept connection. {:?}", err);
                    }
                }
            }
        }).unwrap();
}