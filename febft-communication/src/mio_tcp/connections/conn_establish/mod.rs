use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::sync::{Arc, Mutex};
use bytes::{Bytes, BytesMut};
use mio::{Events, Interest, Poll, Registry, Token};
use mio::event::Event;
use slab::Slab;
use febft_common::node_id::NodeId;
use febft_common::socket::{MioListener, MioSocket};
use crate::message::{Header, WireMessage};
use crate::mio_tcp::connections::epoll_workers::{interrupted, would_block};
use crate::tcpip::connections::ConnCounts;


const SERVER_TOKEN: Token = Token(0);

pub struct ConnectionHandler {
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
    fn event_loop(mut self) -> io::Result<()> {
        let mut poll = Poll::new()?;

        poll.registry()
            .register(&mut self.listener, SERVER_TOKEN, Interest::READABLE)?;

        let mut events = Events::with_capacity(128);

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
                            }
                            ConnectionResult::ConnectionBroken => {
                                // Discard of the connection since it has been broken
                                if let Some((conn, _)) = self.currently_accepting.remove(token.into()) {
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
                    let read_buffer = BytesMut::with_capacity(Header::LENGTH);

                    let token = Token(self.currently_accepting.insert((MioSocket::from(socket), read_buffer)));

                    registry.register(&mut self.currently_accepting[token.into()], token, Interest::READABLE)?;
                }
                Err(err) if would_block(&err) => {
                    // No more connections are ready to be accepted
                    break;
                }
                Err(ref err) if interrupted(err) => continue,
                Err(err) => {
                    Err(err)
                }
            }
        }

        Ok(())
    }

    fn handle_connection_readable(&mut self, token: Token, ev: &Event) -> io::Result<ConnectionResult> {
        let (socket, buffer) = &mut self.currently_accepting[token.into()];

        if ev.is_readable() {

            loop {
                match socket.read(&mut buffer[buffer.len()..]) {
                    Ok(0) => {
                        return Ok(ConnectionResult::ConnectionBroken)
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
                        return Err(err)
                    }
                };
            }
        }

        Ok(ConnectionResult::Working)
    }
}