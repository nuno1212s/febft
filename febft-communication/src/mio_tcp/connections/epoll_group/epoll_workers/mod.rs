use std::io;
use std::io::{Read, Write};
use std::net::Shutdown;
use std::sync::Arc;
use std::time::Duration;
use bytes::{Buf, Bytes, BytesMut};
use log::{debug, error, info, trace};
use mio::{Events, Interest, Poll, Token, Waker};
use mio::event::Event;
use slab::Slab;
use febft_common::channel::{ChannelSyncRx};
use febft_common::error::{Error, ErrorKind, ResultWrappedExt};
use febft_common::node_id::NodeId;
use febft_common::socket::{MioSocket};
use crate::cpu_workers;
use crate::message::{Header, WireMessage};
use crate::mio_tcp::connections::{Connections, ConnHandle};
use crate::mio_tcp::connections::epoll_group::{EpollWorkerId, EpollWorkerMessage, NewConnection};
use super::PeerConnection;
use crate::serialize::Serializable;

const EVENT_CAPACITY: usize = 1024;
const DEFAULT_SOCKET_CAPACITY: usize = 1024;
const WORKER_TIMEOUT: Option<Duration> = Some(Duration::from_micros(50));

enum ConnectionWorkResult {
    Working,
    ConnectionBroken,
}

type ConnectionRegister = ChannelSyncRx<MioSocket>;


/// The information for this worker thread.
pub(super) struct EpollWorker<M: Serializable + 'static> {
    // The id of this worker
    worker_id: EpollWorkerId,
    // A reference to our parent connections, so we can update it in case anything goes wrong
    // With any connections
    global_connections: Arc<Connections<M>>,
    // This slab stores the connections that are currently being handled by this worker
    connections: Slab<SocketConnection<M>>,
    // register new connections
    connection_register: ChannelSyncRx<EpollWorkerMessage<M>>,
    // The poll instance of this worker
    poll: Poll,
    // Waker
    waker: Arc<Waker>,
}

/// All information related to a given connection
enum SocketConnection<M: Serializable + 'static> {
    PeerConn {
        // The handle of this connection
        handle: ConnHandle,
        // The mio socket that this connection refers to
        socket: MioSocket,
        // Information and buffers for the read end of this connection
        read_info: ReadingInformation,
        // Information and buffers for the write end of this connection
        // Option since we may not be writing anything at the moment
        writing_info: Option<WritingInformation>,
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

struct WritingInformation {
    written_bytes: usize,
    currently_writing_header: Option<Bytes>,
    currently_writing: Bytes,
}

impl<M> EpollWorker<M> where M: Serializable + 'static {
    /// Initializing a worker thread for the worker group
    pub fn new(worker_id: EpollWorkerId, connections: Arc<Connections<M>>,
               register: ChannelSyncRx<EpollWorkerMessage<M>>) -> febft_common::error::Result<Self> {
        let poll = Poll::new().wrapped_msg(ErrorKind::Communication, "Failed to initialize poll")?;

        let mut conn_slab = Slab::with_capacity(DEFAULT_SOCKET_CAPACITY);

        let entry = conn_slab.vacant_entry();

        let waker_token = Token(entry.key());
        let waker = Arc::new(Waker::new(poll.registry(), waker_token)
            .wrapped_msg(ErrorKind::Communication, "Failed to create waker")?);

        entry.insert(SocketConnection::Waker);

        info!("{:?} // Initialized Epoll Worker where Waker is token {:?}", connections.id, waker_token);

        Ok(Self {
            worker_id,
            global_connections: connections,
            connections: conn_slab,
            connection_register: register,
            poll,
            waker,
        })
    }

    pub(super) fn epoll_worker_loop(mut self) -> io::Result<()> {
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
                trace!("{:?} // Handling connection event for ev: {:?}", self.global_connections.id, event);

                match event.token() {
                    token => {
                        match self.handle_connection_event(token, &event) {
                            Ok(ConnectionWorkResult::ConnectionBroken) => {
                                self.delete_connection(token, true)?;
                            }
                            Ok(_) => {}
                            Err(err) => {
                                let connection = &self.connections[token.into()];

                                match connection {
                                    SocketConnection::PeerConn { handle, .. } => {
                                        error!("{:?} // Error handling connection event: {:?} for token {:?} (corresponding to conn id {:?})",
                                            self.global_connections.id, err, token, handle.peer_id());
                                    }
                                    _ => unreachable!()
                                }
                            }
                        }
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
                    if let ConnectionWorkResult::ConnectionBroken = self.read_until_block(token)? {
                        return Ok(ConnectionWorkResult::ConnectionBroken);
                    }
                }

                if event.is_writable() {
                    if let ConnectionWorkResult::ConnectionBroken = self.try_write_until_block(token)? {
                        return Ok(ConnectionWorkResult::ConnectionBroken);
                    }
                }
            }
            SocketConnection::Waker => {
                // Indicates that we should try to write from the connections

                let connections_to_analyse: Vec<Token> = self.connections.iter()
                    .filter_map(|(key, conn)|
                        if let SocketConnection::PeerConn { .. } = conn {
                            Some(Token(key))
                        } else {
                            None
                        })
                    .collect();

                for x in connections_to_analyse {
                    self.try_write_until_block(x)?;
                }
            }
        }

        Ok(ConnectionWorkResult::Working)
    }

    fn try_write_until_block(&mut self, token: Token) -> febft_common::error::Result<ConnectionWorkResult> {
        let connection = &mut self.connections[token.into()];

        match connection {
            SocketConnection::PeerConn {
                socket,
                connection,
                writing_info,
                ..
            } => {
                let was_waiting_for_write = writing_info.is_some();
                let mut wrote = false;

                loop {
                    let writing = if let Some(writing_info) = writing_info {
                        wrote = true;

                        //We are writing something
                        writing_info
                    } else {
                        // We are not currently writing anything

                        if let Some(to_write) = connection.try_take_from_send()? {
                            trace!("{:?} // Writing message {:?}", self.global_connections.id, to_write);
                            wrote = true;

                            // We have something to write
                            *writing_info = Some(WritingInformation::from_message(to_write)?);

                            writing_info.as_mut().unwrap()
                        } else {
                            // Nothing to write
                            trace!("{:?} // Nothing left to write, wrote? {}", self.global_connections.id, wrote);

                            // If we have written something in this loop but we have not written until
                            // Would block then we should flush the connection
                            if wrote {
                                match socket.flush() {
                                    Ok(_) => {}
                                    Err(ref err) if would_block(err) => break,
                                    Err(ref err) if interrupted(err) => continue,
                                    Err(err) => { return Err(Error::wrapped(ErrorKind::Communication, err)); }
                                };
                            }

                            break;
                        }
                    };

                    if let Some(header) = writing.currently_writing_header.as_ref() {
                        match socket.write(&header[writing.written_bytes..]) {
                            Ok(0) => return Ok(ConnectionWorkResult::ConnectionBroken),
                            Ok(n) => {
                                // We have successfully written n bytes
                                if n < header.len() {
                                    writing.written_bytes += n;

                                    continue;
                                } else {
                                    writing.written_bytes = 0;
                                    writing.currently_writing_header = None;
                                }
                            }
                            Err(err) if would_block(&err) => {
                                trace!("{:?} // Would block writing header", self.global_connections.id);
                                break;
                            }
                            Err(err) if interrupted(&err) => continue,
                            Err(err) => { return Err(Error::wrapped(ErrorKind::Communication, err)); }
                        }
                    } else {
                        match socket.write(&writing.currently_writing[writing.written_bytes..]) {
                            Ok(0) => return Ok(ConnectionWorkResult::ConnectionBroken),
                            Ok(n) => {
                                // We have successfully written n bytes
                                if n + writing.written_bytes < writing.currently_writing.len() {
                                    writing.written_bytes += n;

                                    continue;
                                } else {
                                    // We have written all that we have to write.
                                    *writing_info = None;
                                }
                            }
                            Err(err) if would_block(&err) => {
                                trace!("{:?} // Would block writing body", self.global_connections.id);
                                break;
                            }
                            Err(err) if interrupted(&err) => continue,
                            Err(err) => { return Err(Error::wrapped(ErrorKind::Communication, err)); }
                        }
                    }
                }

                if writing_info.is_none() && was_waiting_for_write {
                    // We have nothing more to write, so we no longer need to be notified of writability
                    self.poll.registry().reregister(socket, token, Interest::READABLE)
                        .wrapped_msg(ErrorKind::Communication, "Failed to reregister socket")?;
                } else if writing_info.is_some() && !was_waiting_for_write {
                    // We still have something to write but we reached a would block state,
                    // so we need to be notified of writability.
                    self.poll.registry().reregister(socket, token, Interest::READABLE.add(Interest::WRITABLE))
                        .wrapped_msg(ErrorKind::Communication, "Failed to reregister socket")?;
                } else {
                    // We have nothing to write and we were not waiting for writability, so we
                    // Don't need to re register
                    // Or we have something to write and we were already waiting for writability,
                    // So we also don't have to re register
                }
            }
            _ => unreachable!()
        }

        Ok(ConnectionWorkResult::Working)
    }

    fn read_until_block(&mut self, token: Token) -> febft_common::error::Result<ConnectionWorkResult> {
        let connection = &mut self.connections[token.into()];

        match connection {
            SocketConnection::PeerConn {
                handle,
                socket,
                read_info,
                connection,
                ..
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
                                Err(err) if would_block(&err) => { break; }
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
                            trace!("{:?} // Reading message header with {} left to read", self.global_connections.id, bytes_to_read);

                            match socket.read(&mut read_info.read_buffer[currently_read_bytes..]) {
                                Ok(0) => {
                                    // Connection closed
                                    trace!("{:?} // Connection closed", self.global_connections.id);
                                    return Ok(ConnectionWorkResult::ConnectionBroken);
                                }
                                Ok(n) => {
                                    // We still have to more to read
                                    n
                                }
                                Err(err) if would_block(&err) => {
                                    break;
                                }
                                Err(err) if interrupted(&err) => {
                                    continue;
                                }
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
                        } else {
                            read_info.read_bytes += n;
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
                            self.create_connection(conn)?;
                        }
                        EpollWorkerMessage::CloseConnection(token) => {
                            if let SocketConnection::Waker = &self.connections[token.into()] {
                                // We can't close the waker, wdym?
                                continue;
                            }

                            self.delete_connection(token, false)?;
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

    fn create_connection(&mut self, conn: NewConnection<M>) -> io::Result<()> {
        let NewConnection {
            conn_id, peer_id,
            my_id, mut socket,
            peer_conn
        } = conn;

        let entry = self.connections.vacant_entry();

        let token = Token(entry.key());

        let handle = ConnHandle::new(
            conn_id, my_id, peer_id,
            self.worker_id, token,
            self.waker.clone(),
        );

        peer_conn.register_peer_conn(handle.clone());

        self.poll.registry().register(&mut socket,
                                      token, Interest::READABLE)?;

        let socket_conn = SocketConnection::PeerConn {
            handle: handle.clone(),
            socket,
            read_info: ReadingInformation::new(),
            writing_info: None,
            connection: peer_conn,
        };

        entry.insert(socket_conn);

        let _ = self.read_until_block(token);
        let _ = self.try_write_until_block(token);

        Ok(())
    }

    fn delete_connection(&mut self, token: Token, is_failure: bool) -> io::Result<()> {
        if let Some(conn) = self.connections.try_remove(token.into()) {
            match conn {
                SocketConnection::PeerConn {
                    mut socket,
                    connection,
                    handle, ..
                } => {
                    self.poll.registry().deregister(&mut socket)?;

                    socket.shutdown(Shutdown::Both)?;

                    if is_failure {
                        self.global_connections.handle_connection_failed(handle.peer_id, handle.id);
                    } else {
                        connection.delete_connection(handle.id);
                    }
                }
                _ => unreachable!("Only peer connections can be removed from the connection slab")
            }
        }

        Ok(())
    }
}

impl ReadingInformation {
    fn new() -> Self {
        let mut bytes_mut = BytesMut::with_capacity(Header::LENGTH);

        bytes_mut.resize(Header::LENGTH, 0);

        Self {
            read_bytes: 0,
            current_header: None,
            read_buffer: bytes_mut,
        }
    }
}

impl WritingInformation {
    fn new() -> Self {
        Self {
            written_bytes: 0,
            currently_writing_header: None,
            currently_writing: Bytes::new(),
        }
    }

    // Initialize this struct from a wiremessage
    fn from_message(message: WireMessage) -> febft_common::error::Result<Self> {
        let (header, payload) = message.into_inner();

        let mut header_bytes = BytesMut::with_capacity(Header::LENGTH);

        header_bytes.resize(Header::LENGTH, 0);

        header.serialize_into(&mut header_bytes[..Header::LENGTH])?;

        let header_bytes = header_bytes.freeze();

        Ok(Self {
            written_bytes: 0,
            currently_writing_header: Some(header_bytes),
            currently_writing: payload,
        })
    }
}

pub fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

impl<M> NewConnection<M> where M: Serializable + 'static {
    pub fn new(conn_id: u32, peer_id: NodeId, my_id: NodeId, socket: MioSocket, peer_conn: Arc<PeerConnection<M>>) -> Self {
        Self { conn_id, peer_id, my_id, socket, peer_conn }
    }
}