use std::fs::read;
use std::io;
use std::io::Read;
use std::net::Shutdown;
use std::sync::Arc;
use std::time::Duration;
use bytes::{Buf, BytesMut};
use mio::{Events, Interest, Poll, Registry, Token, Waker};
use mio::event::Event;
use slab::Slab;
use febft_common::channel::ChannelSyncRx;
use febft_common::error::{Error, ErrorKind, ResultWrappedExt};
use febft_common::node_id::NodeId;
use febft_common::socket::{MioSocket};
use crate::cpu_workers;
use crate::message::Header;
use crate::mio_tcp::connections::{Connections, ConnHandle};
use super::PeerConnection;
use crate::serialize::Serializable;

const EVENT_CAPACITY: usize = 1024;
const DEFAULT_SOCKET_CAPACITY: usize = 1024;
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

enum ConnectionWorkResult {
    Working,
    ConnectionBroken,
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
    // Epoll worker waker
    waker: Arc<Waker>,
    // The poll instance of this worker
    poll: Poll,
}

/// All information related to a given connection
enum SocketConnection<M: Serializable + 'static> {
    PeerConn {
        // The handle of this connection
        handle: ConnHandle,
        // The mio socket that this connection refers to
        socket: MioSocket,
        read_info: ReadingInformation,
        // The connection to the peer this connection is a part of
        connection: Arc<PeerConnection<M>>,
    },
    Waker,
}

struct ReadingInformation {
    read_bytes: usize,
    // The header of the message we are currently reading (if applicable)
    current_header: Option<Header>,
    // The buffer for reading data from the socket.
    read_buffer: BytesMut,
}

impl<M> EpollWorker<M> where M: Serializable + 'static {
    pub fn new(worker_id: EpollWorkerId, connections: Arc<Connections<M>>,
               register: ChannelSyncRx<EpollWorkerMessage<M>>) -> febft_common::error::Result<Self> {
        let poll = Poll::new().wrapped_msg(ErrorKind::Communication, "Failed to initialize poll")?;

        let mut conn_slab = Slab::with_capacity(DEFAULT_SOCKET_CAPACITY);

        let entry = conn_slab.vacant_entry();

        let waker = Arc::new(Waker::new(poll.registry(), Token(entry.key()))
            .wrapped_msg(ErrorKind::Communication, "Failed to create waker")?);

        entry.insert(SocketConnection::Waker);

        Ok(Self {
            worker_id,
            global_connections: connections,
            connections: conn_slab,
            connection_register: register,
            waker,
            poll,
        })
    }

    fn epoll_worker_loop(mut self) -> io::Result<()> {
        let mut event_queue = Events::with_capacity(EVENT_CAPACITY);


        loop {
            if let Err(e) = self.poll.poll(&mut event_queue, WORKER_TIMEOUT) {
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

            self.register_connections()?;
        }
    }

    fn handle_connection_event(&mut self, token: Token, event: &Event) -> febft_common::error::Result<ConnectionWorkResult> {
        match &self.connections[token.into()] {
            SocketConnection::PeerConn { .. } => {
                if event.is_readable() {
                    self.read_until_block(token)?;
                }

                if event.is_writable() {}
            }
            SocketConnection::Waker => {
                // Indicates that we should try to write from the connections

                for (index, connection) in &self.connections {}
            }
        }

        Ok(ConnectionWorkResult::Working)
    }

    fn try_write_until_block(&mut self, token: Token) -> febft_common::error::Result<ConnectionWorkResult> {
        let connection = &mut self.connections[token.into()];

        match connection {
            SocketConnection::PeerConn {
                handle,
                socket,
                read_info,
                connection
            } => {
                loop {

                    // We have something to write
                    if let Some(to_write) = connection.try_take_from_send()? {}
                }
            }
            _ => unreachable!()
        }

        Ok(())
    }

    fn read_until_block(&mut self, token: Token) -> febft_common::error::Result<ConnectionWorkResult> {
        let connection = &mut self.connections[token.into()];

        match connection {
            SocketConnection::PeerConn {
                handle,
                socket,
                read_info,
                connection
            } => {
                loop {
                    if let Some(header) = &read_info.current_header {
                        // We are currently reading a message
                        let currently_read = read_info.read_bytes;
                        let bytes_to_read = header.payload_length() - currently_read;

                        let read = if bytes_to_read > 0 {
                            match socket.read(&mut read_info.read_buffer[currently_read..]) {
                                Ok(0) => {
                                    // Connection closed
                                    return Ok(ConnectionWorkResult::ConnectionBroken);
                                }
                                Ok(n) => {
                                    // We still have more to read
                                    n
                                }
                                Err(err) if would_block(&err) => break,
                                Err(err) if interrupted(&err) => continue,
                                Err(err) => { return Err(Error::wrapped(ErrorKind::Communication, err)); }
                            }
                        } else {
                            // Only read if we need to read from the socket.
                            // If not, keep parsing the messages that are in the read buffer
                            0
                        };

                        if read >= bytes_to_read {
                            let header = std::mem::replace(&mut read_info.current_header, None).unwrap();

                            let message = read_info.read_buffer.split_to(header.payload_length());

                            // We have read the message, send it to be verified and then put into the client pool
                            cpu_workers::deserialize_and_push_message(header, message, connection.client.clone());

                            read_info.read_bytes = read_info.read_buffer.len();

                            read_info.read_buffer.reserve(Header::LENGTH);
                            read_info.read_buffer.resize(Header::LENGTH, 0);
                        } else {
                            read_info.read_bytes += read;
                        }
                    } else {
                        // We are currently reading a header
                        let currently_read_bytes = read_info.read_bytes;
                        let bytes_to_read = Header::LENGTH - currently_read_bytes;

                        let n = if bytes_to_read > 0 {
                            match socket.read(&mut read_info.read_buffer[currently_read_bytes..]) {
                                Ok(0) => {
                                    // Connection closed
                                    return Ok(ConnectionWorkResult::ConnectionBroken);
                                }
                                Ok(n) => {
                                    // We still have to more to read
                                    n
                                }
                                Err(err) if would_block(&err) => break,
                                Err(err) if interrupted(&err) => continue,
                                Err(err) => { return Err(Error::wrapped(ErrorKind::Communication, err)); }
                            }
                        } else {
                            // Only read if we need to read from the socket. (As we are missing bytes)
                            // If not, keep parsing the messages that are in the read buffer
                            0
                        };

                        if n >= bytes_to_read {
                            let header = Header::deserialize_from(&read_info.read_buffer[..Header::LENGTH])?;

                            *(&mut read_info.current_header) = Some(header);

                            if n > bytes_to_read {
                                // We have read more than we should for the current message,
                                // so we can't clear the buffer
                                read_info.read_buffer.advance(Header::LENGTH);

                                read_info.read_bytes = read_info.read_buffer.len();

                                read_info.read_buffer.reserve(header.payload_length());
                                read_info.read_buffer.resize(header.payload_length(), 0);
                            } else {
                                // We have read the header
                                read_info.read_buffer.clear();
                                read_info.read_buffer.reserve(header.payload_length());
                                read_info.read_buffer.resize(header.payload_length(), 0);
                                read_info.read_bytes = 0;
                            }
                        }
                    }
                }

                // We don't have any more
            }
            _ => unreachable!()
        }

        Ok(ConnectionWorkResult::Working)
    }

    /// Receive connections from the connection register and register them with the epoll instance
    fn register_connections(&mut self) -> io::Result<()> {
        loop {
            match self.connection_register.try_recv() {
                Ok(message) => {
                    match message {
                        EpollWorkerMessage::NewConnection(conn) => {
                            let NewConnection {
                                conn_id, peer_id,
                                my_id, mut socket,
                                peer_conn
                            } = conn;

                            let entry = self.connections.vacant_entry();

                            let token = Token(entry.key());

                            let handle = ConnHandle::new(
                                conn_id, my_id, peer_id, self.waker.clone(),
                            );

                            self.poll.registry().register(&mut socket,
                                                          token, Interest::READABLE)?;

                            let socket_conn = SocketConnection::PeerConn {
                                handle: handle.clone(),
                                socket,
                                read_info: ReadingInformation::new(),
                                connection: peer_conn,
                            };

                            entry.insert(socket_conn);
                        }
                        EpollWorkerMessage::CloseConnection(token) => {
                            if let SocketConnection::Waker = &self.connections[token.into()] {
                                // We can't close the waker, wdym?
                                continue;
                            }

                            if let Some(conn) = self.connections.try_remove(token.into()) {
                                match conn {
                                    SocketConnection::PeerConn { mut socket, .. } => {
                                        self.poll.registry().deregister(&mut socket)?;

                                        socket.shutdown(Shutdown::Both)?;
                                    }
                                    _ => unreachable!("Only peer connections can be removed from the connection slab")
                                }
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
}

impl ReadingInformation {
    fn new() -> Self {
        Self {
            current_header: None,
            read_buffer: BytesMut::with_capacity(Header::LENGTH),
        }
    }
}

pub(super) fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub(super) fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
