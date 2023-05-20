use std::collections::BTreeMap;
use std::io;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::{Bytes, BytesMut};
use futures::channel::oneshot;
use log::{debug, error, info, warn};
use mio::{Events, Interest, Poll, Registry, Token};
use mio::event::Event;
use mio::net::TcpStream;
use slab::Slab;
use febft_common::channel::OneShotRx;
use febft_common::node_id::NodeId;
use febft_common::error::*;
use febft_common::socket::{MioListener, MioSocket, SecureSocket, SecureSocketSync};
use febft_common::{channel, prng, socket, threadpool};
use crate::message::{Header, WireMessage};
use crate::mio_tcp::connections::Connections;
use crate::mio_tcp::connections::epoll_workers::{interrupted, would_block};
use crate::serialize::Serializable;
use crate::tcpip::connections::ConnCounts;
use crate::tcpip::PeerAddr;

const DEFAULT_ALLOWED_CONCURRENT_JOINS: usize = 128;
// Since the tokens will always start at 0, we limit the amount of concurrent joins we can have
// And then make the server token that limit + 1, since we know that it will never be exceeded
// (Since slab re utilizes tokens)
const SERVER_TOKEN: Token = Token(DEFAULT_ALLOWED_CONCURRENT_JOINS + 1);

pub struct ConnectionHandler {
    my_id: NodeId,
    first_cli: NodeId,

    concurrent_conn: ConnCounts,
    currently_connecting: Mutex<BTreeMap<NodeId, usize>>,
}

pub struct ServerWorker {
    my_id: NodeId,
    first_cli: NodeId,
    listener: MioListener,
    currently_accepting: Slab<(MioSocket, BytesMut)>,
    conn_handler: Arc<ConnectionHandler>,
}

enum ConnectionResult {
    Connected(NodeId),
    Working,
    ConnectionBroken,
}

impl ServerWorker {
    pub fn new(my_id: NodeId, first_cli: NodeId, listener: MioListener, conn_handler: Arc<ConnectionHandler>) -> Self {
        Self {
            my_id,
            first_cli,
            listener,
            currently_accepting: Slab::with_capacity(DEFAULT_ALLOWED_CONCURRENT_JOINS),
            conn_handler,
        }
    }

    /// Run the event loop of this worker
    fn event_loop(mut self) -> io::Result<()> {
        let mut poll = Poll::new()?;

        poll.registry()
            .register(&mut self.listener, SERVER_TOKEN, Interest::READABLE)?;

        let mut events = Events::with_capacity(DEFAULT_ALLOWED_CONCURRENT_JOINS);

        loop {
            poll.poll(&mut events, None)?;

            for event in events.iter() {
                match event.token() {
                    SERVER_TOKEN => {
                        self.accept_connections(poll.registry())?;
                    }
                    token => {
                        match self.handle_connection_readable(token, &event)? {
                            ConnectionResult::Connected(node_id) => {
                                // We have identified the peer and should now handle the connection

                                if let Some((mut conn, _)) = self.currently_accepting.try_remove(token.into()) {
                                    // Deregister from this poller as we are no longer
                                    // the ones that should handle this connection
                                    poll.registry().deregister(&mut conn)?;

                                    if self.conn_handler.register_connecting_to_node(node_id) {
                                        // If we can connect to this node, then pass
                                        // this connection to the correct worker

                                        todo!("Pass connection to correct worker")
                                    } else {
                                        // Ignore this connection
                                    }
                                }
                            }
                            ConnectionResult::ConnectionBroken => {
                                // Discard of the connection since it has been broken
                                if let Some((mut conn, _)) = self.currently_accepting.try_remove(token.into()) {
                                    poll.registry().deregister(&mut conn)?;
                                }
                            }
                            ConnectionResult::Working => {}
                        }
                    }
                }
            }
        }
    }

    fn accept_connections(&mut self, registry: &Registry) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((socket, addr)) => {
                    if self.currently_accepting.len() == DEFAULT_ALLOWED_CONCURRENT_JOINS {
                        // Ignore connections that would exceed our default concurrent join limit
                        warn!(" {:?} // Ignoring connection from {} since we have reached the concurrent join limit",
                            self.my_id, addr);

                        continue;
                    }

                    let read_buffer = BytesMut::with_capacity(Header::LENGTH);

                    let token = Token(self.currently_accepting.insert((MioSocket::from(socket), read_buffer)));

                    registry.register(&mut self.currently_accepting[token.into()].0, token, Interest::READABLE)?;
                }
                Err(err) if would_block(&err) => {
                    // No more connections are ready to be accepted
                    break;
                }
                Err(ref err) if interrupted(err) => continue,
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn handle_connection_readable(&mut self, token: Token, ev: &Event) -> io::Result<ConnectionResult> {
        let (socket, buffer) = &mut self.currently_accepting[token.into()];

        if ev.is_readable() {
            loop {
                let currently_read = buffer.len();

                match socket.read(&mut buffer[currently_read..]) {
                    Ok(0) => {
                        return Ok(ConnectionResult::ConnectionBroken);
                    }
                    Ok(n) => {
                        if buffer.len() == Header::LENGTH {

                            // we are passing the correct length, safe to use unwrap()
                            let header = Header::deserialize_from(&buffer[..]).unwrap();

                            // extract peer id
                            let peer_id = match WireMessage::from_parts(header, Bytes::new()) {
                                // drop connections from other clis if we are a cli
                                Ok(wm) if wm.header().from() >= self.first_cli && self.my_id >= self.first_cli => return Ok(ConnectionResult::ConnectionBroken),
                                // drop connections to the wrong dest
                                Ok(wm) if wm.header().to() != self.my_id => return Ok(ConnectionResult::ConnectionBroken),
                                // accept all other conns
                                Ok(wm) => wm.header().from(),
                                // drop connections with invalid headers
                                Err(_) => return Ok(ConnectionResult::ConnectionBroken),
                            };

                            return Ok(ConnectionResult::Connected(peer_id));
                        }
                    }
                    // Would block "errors" are the OS's way of saying that the
                    // connection is not actually ready to perform this I/O operation.
                    Err(ref err) if would_block(err) => break,
                    Err(ref err) if interrupted(err) => continue,
                    Err(err) => {
                        return Err(err);
                    }
                };
            }
        }

        Ok(ConnectionResult::Working)
    }
}

impl ConnectionHandler {
    /// Register that we are currently attempting to connect to a node.
    /// Returns true if we can attempt to connect to this node, false otherwise
    /// We may not be able to connect to a given node if the amount of connections
    /// being established already overtakes the limit of concurrent connections
    fn register_connecting_to_node(&self, peer_id: NodeId) -> bool {
        let mut connecting_guard = self.currently_connecting.lock().unwrap();

        let value = connecting_guard.entry(peer_id).or_insert(0);

        *value += 1;

        if *value > self.concurrent_conn.get_connections_to_node(self.my_id(), peer_id, self.first_cli) * 2 {
            *value -= 1;

            false
        } else {
            true
        }
    }

    /// Register that we are done connecting to a given node (The connection was either successful or failed)
    fn done_connecting_to_node(&self, peer_id: &NodeId) {
        let mut connection_guard = self.currently_connecting.lock().unwrap();

        connection_guard.entry(peer_id.clone()).and_modify(|value| { *value -= 1 });

        if let Some(connection_count) = connection_guard.get(peer_id) {
            if *connection_count <= 0 {
                connection_guard.remove(peer_id);
            }
        }
    }

    pub fn connect_to_node<M: Serializable + 'static>(self: &Arc<Self>,
                                                      connections: Arc<Connections<M>>,
                                                      peer_id: NodeId, addr: PeerAddr) -> OneShotRx<Result<()>> {
        
        let (tx, rx) = channel::new_oneshot_channel();

        debug!(" {:?} // Connecting to node {:?} at {:?}", self.my_id(), peer_id, addr);

        let conn_handler = Arc::clone(self);

        if !self.register_connecting_to_node(peer_id) {
            warn!("{:?} // Tried to connect to node that I'm already connecting to {:?}",
                conn_handler.my_id(), peer_id);

            let _ = tx.send(Err(Error::simple_with_msg(ErrorKind::Communication, "Already connecting to node")));

            return rx;
        }

        std::thread::Builder::new()
            .name(format!("Connecting to Node {:?}", peer_id))
            .spawn(move || {

                //Get the correct IP for us to address the node
                //If I'm a client I will always use the client facing addr
                //While if I'm a replica I'll connect to the replica addr (clients only have this addr)
                let addr = if conn_handler.my_id() >= conn_handler.first_cli() {
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
                            conn_handler.my_id(), peer_id);

                                let _ = tx.send(Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to find IP address for peer")));
                                return;
                            }
                        }.clone()
                    }
                };

                const SECS: u64 = 1;
                const RETRY: usize = 3 * 60;

                let mut rng = prng::State::new();

                let nonce = rng.next_state();

                let my_id = conn_handler.my_id();

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
                    debug!("Attempting to connect to node {:?} with addr {:?} for the {} time", peer_id, addr, _try);

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
                                error!("{:?} // Failed to connect to the node {:?} {:?} ", conn_handler.my_id(), peer_id, err);
                                break;
                            }

                            if let Err(err) = sock.flush() {
                                // errors flushing -> faulty connection;
                                // drop this socket
                                error!("{:?} // Failed to connect to the node {:?} {:?} ", conn_handler.my_id(), peer_id, err);
                                break;
                            }

                            // TLS handshake; drop connection if it fails
                            let sock = if peer_id >= conn_handler.first_cli() || conn_handler.my_id() >= conn_handler.first_cli() {
                                debug!("{:?} // Connecting with plain text to node {:?}",my_id, peer_id);
                                SecureSocketSync::new_plain(sock)
                            } else {
                                SecureSocketSync::new_plain(sock)
                                /*let dns_ref = match ServerName::try_from(addr.1.as_str()) {
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
                                }*/
                            };

                            info!("{:?} // Established connection to node {:?}", my_id, peer_id);

                            connections.handle_connection_established(peer_id, SecureSocket::Sync(sock));

                            conn_handler.done_connecting_to_node(&peer_id);

                            let _ = tx.send(Ok(()));

                            return;
                        }
                        Err(err) => {
                            error!("{:?} // Error on connecting to {:?} addr {:?}: {:?}", 
                                conn_handler.my_id(), peer_id, addr, err);
                        }
                    }

                    // sleep for `SECS` seconds and retry
                    std::thread::sleep(Duration::from_secs(SECS));
                }

                conn_handler.done_connecting_to_node(&peer_id);

                // announce we have failed to connect to the peer node
                //if we fail to connect, then just ignore
                error!("{:?} // Failed to connect to the node {:?} ", conn_handler.my_id(), peer_id);

                let _ = tx.send(Err(Error::simple_with_msg(ErrorKind::Communication, "Failed to establish connection")));
            }).expect("Failed to allocate thread to establish connection");

        rx
    }

    pub fn my_id(&self) -> NodeId {
        self.my_id
    }

    pub fn first_cli(&self) -> NodeId {
        self.first_cli
    }
}