use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use either::Either;
use log::{debug, error, warn};
use rustls::{ClientConnection, ServerConnection, ServerName};

use febft_common::{prng, socket, threadpool};
use febft_common::error::*;
use febft_common::channel::{new_oneshot_channel, OneShotRx};
use febft_common::node_id::NodeId;
use febft_common::socket::{SecureReadHalf, SecureSocketSync, SecureWriteHalf, SyncListener, SyncSocket};

use crate::message::{Header, WireMessage};
use crate::serialize::Serializable;
use crate::tcpip::{PeerAddr, TlsNodeAcceptor, TlsNodeConnector};
use crate::tcpip::connections::conn_establish::ConnectionHandler;
use crate::tcpip::connections::PeerConnections;

pub(super) fn setup_conn_acceptor_thread<M: Serializable + 'static>(tcp_listener: SyncListener,
                                                                    conn_handler: Arc<ConnectionHandler>,
                                                                    peer_connection: Arc<PeerConnections<M>>) {
    std::thread::Builder::new()
        .name(format!("Connection acceptor thread"))
        .spawn(move || {
            loop {
                match tcp_listener.accept() {
                    Ok(connection) => {
                        conn_handler.accept_conn::<M>(&peer_connection, Either::Right(connection))
                    }
                    Err(err) => {
                        error!("Failed to accept connection. {:?}", err);
                    }
                }
            }
        }).unwrap();
}

pub(super) fn connect_to_node_sync<M: Serializable + 'static>(conn_handler: Arc<ConnectionHandler>,
                                                              connections: Arc<PeerConnections<M>>,
                                                              peer_id: NodeId, addr: PeerAddr) -> OneShotRx<Result<()>> {
    let (tx, rx) = new_oneshot_channel();

    std::thread::Builder::new()
        .spawn(move || {
            let my_id = conn_handler.id();

            if !conn_handler.register_connecting_to_node(peer_id) {
                warn!("{:?} // Tried to connect to node that I'm already connecting to {:?}",
                my_id, peer_id);

                return;
            }

            debug!("{:?} // Connecting to the node {:?}", my_id, peer_id);

            let mut rng = prng::State::new();

            let nonce = rng.next_state();

            //Get the correct IP for us to address the node
            //If I'm a client I will always use the client facing addr
            //While if I'm a replica I'll connect to the replica addr (clients only have this addr)
            let addr = if conn_handler.id() >= conn_handler.first_cli() {
                addr.replica_facing_socket.clone()
            } else {
                //We are a replica, but we are connecting to a client, so
                //We need the client addr.
                if peer_id >= conn_handler.first_cli() {
                    addr.replica_facing_socket.clone()
                } else {
                    match addr.client_facing_socket.as_ref() {
                        Some(addr) => addr,
                        None => {
                            error!("{:?} // Failed to find IP address for peer {:?}",
                                my_id, peer_id);

                            return;
                        }
                    }.clone()
                }
            };

            debug!("{:?} // Starting connection to node {:?} with address {:?}",
            my_id,
            peer_id,
            addr.0);

            let connector = match &conn_handler.connector {
                TlsNodeConnector::Sync(connector) => { connector }
                TlsNodeConnector::Async(_) => { panic!("Failed, trying to use async connector in sync mode") }
            }.clone();

            const SECS: u64 = 1;
            const RETRY: usize = 3 * 60;


            // NOTE:
            // ========
            //
            // 1) not an issue if `tx` is closed, this is not a
            // permanently running task, so channel send failures
            // are tolerated
            //
            // 2) try to connect up to `RETRY` times, then announce
            // failure with a channel send op
            for _try in 0..RETRY {
                debug!(
                "Attempting to connect to node {:?} with addr {:?} for the {} time",
                peer_id, addr, _try);

                match socket::connect_sync(addr.0) {
                    Ok(mut sock) => {

                        // create header
                        let (header, _) =
                            WireMessage::new(my_id, peer_id,
                                             Bytes::new(), nonce,
                                             None, None).into_inner();

                        // serialize header
                        let mut buf = [0; Header::LENGTH];
                        header.serialize_into(&mut buf[..]).unwrap();

                        // send header
                        if let Err(err) = sock.write_all(&buf[..]) {
                            // errors writing -> faulty connection;
                            // drop this socket
                            error!("{:?} // Failed to connect to the node {:?} {:?} ", my_id, peer_id, err);
                            break;
                        }

                        if let Err(err) = sock.flush() {
                            // errors flushing -> faulty connection;
                            // drop this socket
                            error!("{:?} // Failed to connect to the node {:?} {:?} ", my_id, peer_id, err);
                            break;
                        }

                        // TLS handshake; drop connection if it fails
                        let sock = if peer_id >= conn_handler.first_cli()
                            || conn_handler.id() >= conn_handler.first_cli() {
                            debug!(
                            "{:?} // Connecting with plain text to node {:?}",
                            my_id, peer_id
                        );

                            SecureSocketSync::new_plain(sock)
                        } else {
                            let dns_ref = match ServerName::try_from(addr.1.as_str()) {
                                Ok(server_name) => server_name,
                                Err(err) => {
                                    error!("Failed to parse DNS name {:?}", err);

                                    break;
                                }
                            };

                            if let Ok(session) = ClientConnection::new(connector.clone(), dns_ref) {
                                SecureSocketSync::new_tls_client(session, sock)
                            } else {
                                error!("Failed to establish tls connection.");

                                break;
                            }
                        };

                        let (write, read) = sock.split();

                        let write = SecureWriteHalf::Sync(write);
                        let read = SecureReadHalf::Sync(read);

                        connections.handle_connection_established(peer_id, (write, read));

                        conn_handler.done_connecting_to_node(&peer_id);

                        if let Err(err) = tx.send(Ok(())) {
                            error!("Failed to deliver connection result {:?}",err);
                        }

                        return;
                    }

                    Err(err) => {
                        error!("{:?} // Error on connecting to {:?} addr {:?}: {:?}", my_id, peer_id, addr, err);
                    }
                }

                // sleep for `SECS` seconds and retry
                std::thread::sleep(Duration::from_secs(SECS));
            }

            conn_handler.done_connecting_to_node(&peer_id);

            // announce we have failed to connect to the peer node
            //if we fail to connect, then just ignore
            error!("{:?} // Failed to connect to the node {:?} ", my_id, peer_id);

            if let Err(err) =
                tx.send(Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to connect to node"))) {
                error!("Failed to deliver connection result {:?}", err);
            }
        }).unwrap();

    rx
}

pub(super) fn handle_server_conn_established<M: Serializable + 'static>(conn_handler: Arc<ConnectionHandler>,
                                                                        connections: Arc<PeerConnections<M>>,
                                                                        mut sock: SyncSocket) {
    threadpool::execute(move || {
        let acceptor = if let TlsNodeAcceptor::Sync(connector) = &conn_handler.tls_acceptor {
            connector.clone()
        } else {
            panic!("Using Tls async acceptor with sync networking")
        };

        let first_cli = conn_handler.first_cli();
        let my_id = conn_handler.id();

        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, Bytes::new()) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            if !conn_handler.register_connecting_to_node(peer_id) {
                warn!("{:?} // Tried to connect to node that I'm already connecting to {:?}",
                my_id, peer_id);
                //Drop the connection since we are already establishing connection

                return;
            }

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                SecureSocketSync::new_plain(sock)
            } else {
                match ServerConnection::new(acceptor) {
                    Ok(s) => {
                        SecureSocketSync::new_tls_server(s, sock)
                    }
                    Err(error) => {
                        error!(
                            "{:?} // Failed to setup tls connection to node {:?}. {:?}",
                            my_id, peer_id, error
                        );

                        break;
                    }
                }
            };

            debug!("{:?} // Received new connection from id {:?}", my_id, peer_id);

            let (write, read) = sock.split();

            let write = SecureWriteHalf::Sync(write);
            let read = SecureReadHalf::Sync(read);

            connections.handle_connection_established(peer_id, (write, read));

            conn_handler.done_connecting_to_node(&peer_id);

            return;
        }
    });
}