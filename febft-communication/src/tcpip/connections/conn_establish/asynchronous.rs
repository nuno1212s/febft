use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use either::Either;
use futures::{AsyncReadExt, AsyncWriteExt};
use futures_timer::Delay;
use log::{debug, error, info, warn};
use rustls::ServerName;
use tokio_rustls::TlsStream;
use febft_common::error::*;
use febft_common::socket::{AsyncListener, AsyncSocket, SecureReadHalf, SecureSocketAsync, SecureWriteHalf};
use febft_common::{async_runtime as rt, prng, socket};
use febft_common::channel::{new_oneshot_channel, OneShotRx};
use febft_common::node_id::NodeId;
use crate::message::{Header, WireMessage};
use crate::serialize::Serializable;
use crate::tcpip::connections::conn_establish::ConnectionHandler;
use crate::tcpip::connections::PeerConnections;
use crate::tcpip::{PeerAddr, TlsNodeAcceptor, TlsNodeConnector};

pub type Callback = Option<Box<dyn FnOnce(bool) + Send>>;

pub(super) fn setup_conn_acceptor_task<M: Serializable + 'static>(tcp_listener: AsyncListener,
                                                                  conn_handler: Arc<ConnectionHandler>,
                                                                  peer_connections: Arc<PeerConnections<M>>) {
    rt::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok(connection) => {
                    conn_handler.accept_conn::<M>(&peer_connections, Either::Left(connection))
                }
                Err(err) => {
                    error!("Failed to accept connection. {:?}", err);
                }
            }
        }
    });
}

pub(super) fn connect_to_node_async<M: Serializable + 'static>(conn_handler: Arc<ConnectionHandler>,
                                                               connections: Arc<PeerConnections<M>>,
                                                               peer_id: NodeId, addr: PeerAddr) -> OneShotRx<Result<()>> {
    let (tx, rx) = new_oneshot_channel();

    rt::spawn(async move {
        if !conn_handler.register_connecting_to_node(peer_id) {
            warn!("{:?} // Tried to connect to node that I'm already connecting to {:?}",
                conn_handler.id(), peer_id);

            return;
        }

        //TODO: Are we currently connected?

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
                        error!(
                            "{:?} // Failed to find IP address for peer {:?}",
                            conn_handler.id(), peer_id
                        );

                        return;
                    }
                }.clone()
            }
        };

        let connector = match &conn_handler.connector {
            TlsNodeConnector::Async(connector) => { connector }
            TlsNodeConnector::Sync(_) => { panic!("Failed, trying to use sync connector in async mode") }
        }.clone();

        const SECS: u64 = 1;
        const RETRY: usize = 3 * 60;

        let my_id = conn_handler.id();

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
                peer_id, addr, _try
            );

            match socket::connect_async(addr.0).await {
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
                    if let Err(err) = sock.write_all(&buf[..]).await {
                        // errors writing -> faulty connection;
                        // drop this socket
                        error!("{:?} // Failed to connect to the node {:?} {:?} ", conn_handler.id(), peer_id, err);
                        break;
                    }

                    if let Err(err) = sock.flush().await {
                        // errors flushing -> faulty connection;
                        // drop this socket
                        error!("{:?} // Failed to connect to the node {:?} {:?} ", conn_handler.id(), peer_id, err);
                        break;
                    }

                    // TLS handshake; drop connection if it fails
                    let sock = if peer_id >= conn_handler.first_cli()
                        || conn_handler.id() >= conn_handler.first_cli() {
                        debug!(
                            "{:?} // Connecting with plain text to node {:?}",
                            my_id, peer_id
                        );

                        SecureSocketAsync::new_plain(sock)
                    } else {

                        let dns_ref = match ServerName::try_from(addr.1.as_str()) {
                            Ok(server_name) => server_name,
                            Err(err) => {
                                error!("Failed to parse DNS name {:?}", err);

                                break;
                            }
                        };

                        match connector.connect(dns_ref, sock.compat_layer()).await {
                            Ok(s) => SecureSocketAsync::new_tls(TlsStream::from(s)),
                            Err(err) => {
                                error!("{:?} // Failed to connect to the node {:?} {:?} ", conn_handler.id(), peer_id, err);
                                break;
                            }
                        }
                    };

                    let (write, read) = sock.split();

                    let write = SecureWriteHalf::Async(write);
                    let read = SecureReadHalf::Async(read);

                    info!("{:?} // Established connection to node {:?}", my_id, peer_id);

                    connections.handle_connection_established(peer_id, (write, read));

                    conn_handler.done_connecting_to_node(&peer_id);

                    let _ = tx.send(Ok(()));
                    return;
                }
                Err(err) => {
                    error!(
                        "{:?} // Error on connecting to {:?} addr {:?}: {:?}",
                        conn_handler.id(), peer_id, addr, err
                    );
                }
            }

            // sleep for `SECS` seconds and retry
            Delay::new(Duration::from_secs(SECS)).await;
        }

        conn_handler.done_connecting_to_node(&peer_id);

        // announce we have failed to connect to the peer node
        //if we fail to connect, then just ignore
        error!("{:?} // Failed to connect to the node {:?} ", conn_handler.id(), peer_id);

        let _ = tx.send(Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to establish connection")));
    });

    rx
}

pub(super) fn handle_server_conn_established<M: Serializable + 'static>(conn_handler: Arc<ConnectionHandler>,
                                                                        connections: Arc<PeerConnections<M>>,
                                                                        mut sock: AsyncSocket) {
    rt::spawn(async move {
        let acceptor = if let TlsNodeAcceptor::Async(connector) = &conn_handler.tls_acceptor {
            connector.clone()
        } else {
            panic!("Using Tls sync acceptor with async networking")
        };

        let first_cli = conn_handler.first_cli();
        let my_id = conn_handler.id();

        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]).await {
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
                SecureSocketAsync::new_plain(sock)
            } else {
                match acceptor.accept(sock.compat_layer()).await {
                    Ok(s) => SecureSocketAsync::new_tls(TlsStream::from(s)),
                    Err(err) => {
                        error!(
                            "{:?} // Failed to setup tls connection to node {:?}, {:?}",
                            my_id, peer_id, err
                        );

                        conn_handler.done_connecting_to_node(&peer_id);
                        break;
                    }
                }
            };

            info!("{:?} // Received new connection from id {:?}", my_id, peer_id);

            let (write, read) = sock.split();

            let write = SecureWriteHalf::Async(write);
            let read = SecureReadHalf::Async(read);

            connections.handle_connection_established(peer_id, (write, read));

            conn_handler.done_connecting_to_node(&peer_id);

            return;
        }
    });
}