//! Abstractions over different socket types of crates in the Rust ecosystem.

use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::io;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_tls::{
    client::TlsStream as TlsStreamCli,
    server::TlsStream as TlsStreamSrv,
};

use futures::io::{
    AsyncRead,
    AsyncWrite,
    BufReader,
    BufWriter,
};

use rustls::{ClientSession, ServerSession, Session, Stream, StreamOwned};

use crate::bft::error;

#[cfg(feature = "socket_tokio_tcp")]
mod tokio_tcp;

#[cfg(feature = "socket_async_std_tcp")]
mod async_std_tcp;

#[cfg(feature = "socket_rio_tcp")]
mod rio_tcp;

mod std_tcp;

/// A `Listener` represents a socket listening on new communications
/// initiated by peer nodes in the BFT system.
pub struct Listener {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Listener,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Listener,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Listener,
}

///A listener. Differs from the other listeners as this is a synchronous listener
///meant to be used as communication between replicas
pub struct ReplicaListener {
    inner: std_tcp::Listener,
}

/// A `Socket` represents a connection between two peer processes
/// in the BFT system.
pub struct Socket {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Socket,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Socket,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Socket,
}

///A ReplicaSocket represents a connection between two replicas in the BFT system.
/// This is a synchronous socket
pub struct ReplicaSocket {
    inner: std_tcp::Socket,
}

/// Initialize the sockets module.
pub unsafe fn init() -> error::Result<()> {
    #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::init()?; }

    Ok(())
}

/// Drops the global data associated with sockets.
pub unsafe fn drop() -> error::Result<()> {
    #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::drop()?; }

    Ok(())
}

/// Creates a new `Listener` socket, bound to the address `addr`.
pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
            { tokio_tcp::bind(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
            { async_std_tcp::bind(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
            { rio_tcp::bind(addr).await }
    }.and_then(|inner| set_listener_options(Listener { inner }))
}

pub fn bind_replica_server<A: Into<SocketAddr>>(addr: A) -> io::Result<ReplicaListener> {
    { std_tcp::bind(addr) }.and_then(|inner| set_listener_options_replica(ReplicaListener { inner }))
}

/// Connects to the remote node pointed to by the address `addr`.
pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
            { tokio_tcp::connect(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
            { async_std_tcp::connect(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
            { rio_tcp::connect(addr).await }
    }.and_then(|inner| set_sockstream_options(Socket { inner }))
}

pub fn connect_replica<A: Into<SocketAddr>>(addr: A) -> io::Result<ReplicaSocket> {
    { std_tcp::connect(addr) }
        .and_then(|inner| set_sockstream_options_sync(ReplicaSocket { inner }))
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Socket> {
        self.inner.accept()
            .await
            .and_then(|inner| set_sockstream_options(Socket { inner }))
    }
}

impl ReplicaListener {
    pub fn accept(&self) -> io::Result<ReplicaSocket> {
        self.inner.accept()
            .and_then(|inner| set_sockstream_options_sync(ReplicaSocket { inner }))
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>
    {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>
    {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

pub enum SecureSocketRecvClient {
    Plain(BufReader<Socket>),
    Tls(TlsStreamSrv<Socket>),
}

pub enum SecureSocketSendClient {
    Plain(BufWriter<Socket>),
    Tls(TlsStreamCli<Socket>),
}

pub enum SecureSocketRecvReplica {
    Plain(ReplicaSocket),
    Tls(StreamOwned<rustls::ServerSession, ReplicaSocket>),
}

pub enum SecureSocketSendReplica {
    Plain(ReplicaSocket),
    Tls(StreamOwned<rustls::ClientSession, ReplicaSocket>),
}

impl SecureSocketRecvReplica {
    pub fn new_tls(session: rustls::ServerSession, socket: ReplicaSocket) -> Self {
        SecureSocketRecvReplica::Tls(StreamOwned::new(session, socket))
    }
}

impl SecureSocketSendReplica {
    pub fn new_tls(session: rustls::ClientSession, socket: ReplicaSocket) -> Self {
        SecureSocketSendReplica::Tls(StreamOwned::new(session, socket))
    }
}

#[derive(Clone)]
///Client stores asynchronous socket references (Client->replica, replica -> client)
///Replicas stores synchronous socket references (Replica -> Replica)
pub enum SecureSocketSend {
    Client(Arc<futures::lock::Mutex<SecureSocketSendClient>>),
    Replica(Arc<parking_lot::Mutex<SecureSocketSendReplica>>),
}

pub enum SecureSocketRecv {
    Client(SecureSocketRecvClient),
    Replica(SecureSocketRecvReplica),
}

impl Write for ReplicaSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&mut self.inner).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&mut self.inner).flush()
    }
}

impl Read for ReplicaSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_exact(buf)
    }
}

impl Write for SecureSocketSendReplica {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SecureSocketSendReplica::Plain(socket) => {
                socket.write(buf)
            }
            SecureSocketSendReplica::Tls(stream) => {
                stream.write(buf)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SecureSocketSendReplica::Plain(socket) => {
                socket.flush()
            }
            SecureSocketSendReplica::Tls(stream) => {
                stream.flush()
            }
        }
    }
}

impl Read for SecureSocketRecvReplica {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SecureSocketRecvReplica::Plain(socket) => {
                socket.read(buf)
            }
            SecureSocketRecvReplica::Tls(stream) => {
                stream.read(buf)
            }
        }
    }
}

impl AsyncRead for SecureSocketRecvClient {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketRecvClient::Plain(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
            SecureSocketRecvClient::Tls(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for SecureSocketSendClient {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketSendClient::Plain(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            }
            SecureSocketSendClient::Tls(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>
    {
        match &mut *self {
            SecureSocketSendClient::Plain(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
            SecureSocketSendClient::Tls(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>>
    {
        match &mut *self {
            SecureSocketSendClient::Plain(inner) => {
                Pin::new(inner).poll_close(cx)
            }
            SecureSocketSendClient::Tls(inner) => {
                Pin::new(inner).poll_close(cx)
            }
        }
    }
}

// set listener socket options; translated from BFT-SMaRt
#[inline]
fn set_listener_options(listener: Listener) -> io::Result<Listener> {
    let sock = socket2::SockRef::from(&listener.inner);
    sock.set_send_buffer_size(8 * 10240 * 1024)?;
    sock.set_recv_buffer_size(8 * 10240 * 1024)?;
    sock.set_reuse_address(true)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    // ChannelOption.SO_BACKLOG ??
    Ok(listener)
}


// set listener socket options; translated from BFT-SMaRt
#[inline]
fn set_listener_options_replica(listener: ReplicaListener) -> io::Result<ReplicaListener> {
    let sock = socket2::SockRef::from(&listener.inner);
    sock.set_send_buffer_size(8 * 10240 * 1024)?;
    sock.set_recv_buffer_size(8 * 10240 * 1024)?;
    sock.set_reuse_address(true)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    // ChannelOption.SO_BACKLOG ??
    Ok(listener)
}


// set connection socket options; translated from BFT-SMaRt
#[inline]
fn set_sockstream_options(connection: Socket) -> io::Result<Socket> {
    let sock = socket2::SockRef::from(&connection.inner);
    sock.set_send_buffer_size(8 * 10240 * 1024)?;
    sock.set_recv_buffer_size(8 * 10240 * 1024)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}


#[inline]
fn set_sockstream_options_sync(connection: ReplicaSocket) -> io::Result<ReplicaSocket> {
    let sock = socket2::SockRef::from(&connection.inner);
    sock.set_send_buffer_size(8 * 10240 * 1024)?;
    sock.set_recv_buffer_size(8 * 10240 * 1024)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}
