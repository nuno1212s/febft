use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::sync::{Arc, Mutex};
use bytes::{Bytes, BytesMut};
use log::{warn};
use mio::{Events, Interest, Poll, Registry, Token};
use mio::event::Event;
use slab::Slab;
use febft_common::node_id::NodeId;
use febft_common::socket::{MioListener, MioSocket};
use crate::message::{Header, WireMessage};
use crate::mio_tcp::connections::epoll_workers::{interrupted, would_block};
use crate::tcpip::connections::ConnCounts;

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

    fn done_connecting_to_node(&self, peer_id: &NodeId) {
        let mut connection_guard = self.currently_connecting.lock().unwrap();

        connection_guard.entry(peer_id.clone()).and_modify(|value| { *value -= 1 });

        if let Some(connection_count) = connection_guard.get(peer_id) {
            if *connection_count <= 0 {
                connection_guard.remove(peer_id);
            }
        }
    }

    pub fn my_id(&self) -> NodeId {
        self.my_id
    }

    pub fn first_cli(&self) -> NodeId {
        self.first_cli
    }
}