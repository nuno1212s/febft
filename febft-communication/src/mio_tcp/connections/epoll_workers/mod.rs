use std::io;
use std::sync::Arc;
use std::time::Duration;
use bytes::BytesMut;
use mio::{Events, Interest, Poll, Registry, Token};
use mio::event::Event;
use slab::Slab;
use febft_common::channel::ChannelSyncRx;
use febft_common::node_id::NodeId;
use febft_common::socket::{MioSocket};
use crate::message::Header;
use crate::mio_tcp::connections::{Connections, ConnHandle};
use super::PeerConnection;
use crate::serialize::Serializable;

const EVENT_CAPACITY: usize = 1024;
const WORKER_TIMEOUT: Option<Duration> = Some(Duration::from_micros(50));

pub struct NewConnection<M: Serializable + 'static> {
    conn_id: u32,
    peer_id: NodeId,
    my_id: NodeId,
    socket: MioSocket,
    peer_conn: Arc<PeerConnection<M>>,
}

pub enum EpollWorkerMessage<M: Serializable + 'static> {
    NewConnection(NewConnection<M>),
    CloseConnection(Token),
}

type ConnectionRegister = ChannelSyncRx<MioSocket>;

pub type EpollWorkerId = u32;

/// The information for this worker thread.
struct EpollWorker<M: Serializable + 'static> {
    // The id of this worker
    worker_id: EpollWorkerId,
    // A reference to our parent connections, so we can update it in case anything goes wrong
    // With any connections
    global_connections: Arc<Connections<M>>,
    // This slab stores the connections that are currently being handled by this worker
    connections: Slab<SocketConnection<M>>,
    // register new connections
    connection_register: ChannelSyncRx<EpollWorkerMessage<M>>,
}

/// All information related to a given connection
struct SocketConnection<M: Serializable + 'static> {
    // The handle of this connection
    handle: ConnHandle,
    // The mio socket that this connection refers to
    socket: MioSocket,
    // The header of the message we are currently reading (if applicable)
    current_header: Option<Header>,
    // The buffer for reading data from the socket.
    read_buffer: BytesMut,
    // The connection to the peer this connection is a part of
    connection: Arc<PeerConnection<M>>,
}

impl<M> EpollWorker<M> where M: Serializable + 'static {
    fn epoll_worker_loop(mut self) -> io::Result<()> {
        let mut epoll = Poll::new()?;

        let mut event_queue = Events::with_capacity(EVENT_CAPACITY);

        loop {
            if let Err(e) = epoll.poll(&mut event_queue, WORKER_TIMEOUT) {
                if e.kind() == io::ErrorKind::Interrupted {
                    // spurious wakeup
                    continue;
                } else if e.kind() == io::ErrorKind::TimedOut {
                    // *should* be handled by mio and return Ok() with no events
                    continue;
                } else {
                    return Err(e);
                }
            }

            for event in event_queue.iter() {
                match event.token() {
                    token => {
                        self.handle_connection_event(token, &event);
                    }
                }
            }

            self.register_connections(epoll.registry())?;
        }
    }

    /// Receive connections from the connection register and register them with the epoll instance
    fn register_connections(&mut self, registry: &Registry) -> io::Result<()> {
        loop {
            match self.connection_register.try_recv() {
                Ok(message) => {
                    match message {
                        EpollWorkerMessage::NewConnection(conn) => {
                            let NewConnection {
                                conn_id, peer_id,
                                my_id, socket,
                                peer_conn
                            } = conn;

                            let entry = self.connections.vacant_entry();

                            let token = Token(entry.key());

                            let handle = ConnHandle::new(
                                conn_id, my_id, peer_id, token, registry.try_clone()?,
                            );

                            let socket_conn = SocketConnection {
                                handle: handle.clone(),
                                socket,
                                current_header: None,
                                read_buffer: BytesMut::with_capacity(1024),
                                connection: peer_conn,
                            };

                            entry.insert(socket_conn);

                            registry.register(&mut self.connections[token.into()].socket,
                                              token, Interest::READABLE)?;
                        }
                        EpollWorkerMessage::CloseConnection(token) => {

                            if let Some(conn) = self.connections.remove(token.into()) {
                                registry.deregister(&mut conn.socket)?;
                            }

                        }
                    }
                }
                Err(err) => {
                    // No more connections are ready to be accepted
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_connection_event(&mut self, token: Token, event: &Event) {
        let connection = &mut self.connections[token.into()];

        if event.is_readable() {
            loop {



            }
        }

        loop {
        }
    }
}


pub(super) fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub(super) fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
