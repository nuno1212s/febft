//! Abstractions over different socket types of crates in the Rust ecosystem.

use std::io;
use std::io::{BufRead, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_tls::{
    client::TlsStream as TlsStreamCli,
    server::TlsStream as TlsStreamSrv,
};
use async_tls::server::TlsStream;

use futures::io::{
    AsyncRead,
    AsyncWrite,
};

use rustls::{ClientConnection, ServerConnection, StreamOwned};

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
pub struct AsyncListener {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Listener,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Listener,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Listener,
}

///A listener. Differs from the other Listener as this is a synchronous listener and does not rely
///On async runtimes
pub struct SyncListener {
    inner: std_tcp::Listener,
}

/// A `Socket` represents a connection between two peer processes
/// in the BFT system.
/// This is an asynchronous socket
pub struct AsyncSocket {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Socket,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Socket,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Socket,
}

///A SyncSocket represents a connection between two peers in the BFT system.
/// This is a synchronous socket
pub struct SyncSocket {
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
pub async fn bind_async_server<A: Into<SocketAddr>>(addr: A) -> io::Result<AsyncListener> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
        { tokio_tcp::bind(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
        { async_std_tcp::bind(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::bind(addr).await }
    }.and_then(|inner| set_listener_options(AsyncListener { inner }))
}

pub fn bind_sync_server<A: Into<SocketAddr>>(addr: A) -> io::Result<SyncListener> {
    { std_tcp::bind(addr) }.and_then(|inner| set_listener_options_replica(SyncListener { inner }))
}

/// Connects to the remote node pointed to by the address `addr`.
pub async fn connect_async<A: Into<SocketAddr>>(addr: A) -> io::Result<AsyncSocket> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
        { tokio_tcp::connect(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
        { async_std_tcp::connect(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::connect(addr).await }
    }.and_then(|inner| set_sockstream_options(AsyncSocket { inner }))
}

pub fn connect_sync<A: Into<SocketAddr>>(addr: A) -> io::Result<SyncSocket> {
    { std_tcp::connect(addr) }
        .and_then(|inner| set_sockstream_options_sync(SyncSocket { inner }))
}

impl AsyncListener {
    pub async fn accept(&self) -> io::Result<AsyncSocket> {
        self.inner.accept()
            .await
            .and_then(|inner| set_sockstream_options(AsyncSocket { inner }))
    }
}

impl SyncListener {
    pub fn accept(&self) -> io::Result<SyncSocket> {
        self.inner.accept()
            .and_then(|inner| set_sockstream_options_sync(SyncSocket { inner }))
    }
}

impl AsyncRead for AsyncSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for AsyncSocket {
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

pub enum SecureSocketRecvAsync {
    Plain(futures::io::BufReader<AsyncSocket>),
    Tls(futures::io::BufReader<TlsStreamSrv<AsyncSocket>>),
}

pub enum SecureSocketSendAsync {
    Plain(futures::io::BufWriter<AsyncSocket>),
    Tls(futures::io::BufWriter<TlsStreamCli<AsyncSocket>>),
}

pub enum SecureSocketRecvSync {
    Plain(std::io::BufReader<SyncSocket>),
    Tls(std::io::BufReader<StreamOwned<ServerConnection, SyncSocket>>),
}

pub enum SecureSocketSendSync {
    Plain(std::io::BufWriter<SyncSocket>),
    Tls(std::io::BufWriter<StreamOwned<ClientConnection, SyncSocket>>),
}

impl SecureSocketRecvAsync {
    pub fn new_plain(socket: AsyncSocket) -> Self {
        SecureSocketRecvAsync::Plain(futures::io::BufReader::new(socket))
    }

    pub fn new_tls(session: TlsStream<AsyncSocket>) -> Self {
        SecureSocketRecvAsync::Tls(futures::io::BufReader::new(session))
    }
}

impl SecureSocketSendAsync {
    pub fn new_plain(socket: AsyncSocket) -> Self {
        SecureSocketSendAsync::Plain(futures::io::BufWriter::new(socket))
    }

    pub fn new_tls(session: TlsStreamCli<AsyncSocket>) -> Self {
        SecureSocketSendAsync::Tls(futures::io::BufWriter::new(session))
    }
}

impl SecureSocketRecvSync {
    pub fn new_plain(socket: SyncSocket) -> Self {
        SecureSocketRecvSync::Plain(std::io::BufReader::new(socket))
    }
    
    pub fn new_tls(session: ServerConnection, socket: SyncSocket) -> Self {
        SecureSocketRecvSync::Tls(std::io::BufReader::new(StreamOwned::new(session, socket)))

    }
}

impl SecureSocketSendSync {
    pub fn new_plain(socket: SyncSocket) -> Self {
        SecureSocketSendSync::Plain(std::io::BufWriter::new(socket))
    }
    
    pub fn new_tls(session: ClientConnection, socket: SyncSocket) -> Self {
        SecureSocketSendSync::Tls(std::io::BufWriter::new(StreamOwned::new(session, socket)))

    }
}

///Client stores asynchronous socket references (Client->replica, replica -> client)
///Replicas stores synchronous socket references (Replica -> Replica)
pub enum SecureSocketSend {
    Async(SocketSendAsync),
    Sync(SocketSendSync),
}

///A socket abstraction to use synchronously
pub struct SocketSendSync {
    socket: SecureSocketSendSync,
}

///A socket abstraction to use asynchronously
pub struct SocketSendAsync(SecureSocketSendAsync);

impl SocketSendSync {
    pub fn new(socket: SecureSocketSendSync) -> Self {
        Self {
            socket,
        }
    }

    pub fn socket(&self) -> &SecureSocketSendSync {
        &self.socket
    }

    pub fn mut_socket(&mut self) -> &mut SecureSocketSendSync {
        &mut self.socket
    }
}

impl SocketSendAsync {
    pub fn new(socket: SecureSocketSendAsync) -> Self {
        Self(socket)
    }

    pub fn socket(&self) -> &SecureSocketSendAsync {
        &self.0
    }

    pub fn mut_socket(&mut self) -> &mut SecureSocketSendAsync
    {
        &mut self.0
    }
}

pub enum SecureSocketRecv {
    Async(SecureSocketRecvAsync),
    Sync(SecureSocketRecvSync),
}

impl Write for SyncSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&mut self.inner).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&mut self.inner).flush()
    }
}

impl Read for SyncSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.inner.read_exact(buf)
    }
}

impl Write for SecureSocketSendSync {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SecureSocketSendSync::Plain(socket) => {
                socket.write(buf)
            }
            SecureSocketSendSync::Tls(stream) => {
                stream.write(buf)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SecureSocketSendSync::Plain(socket) => {
                socket.flush()
            }
            SecureSocketSendSync::Tls(stream) => {
                stream.flush()
            }
        }
    }
}

impl Read for SecureSocketRecvSync {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SecureSocketRecvSync::Plain(socket) => {
                socket.read(buf)
            }
            SecureSocketRecvSync::Tls(stream) => {
                stream.read(buf)
            }
        }
    }
}

impl AsyncRead for SecureSocketRecvAsync {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketRecvAsync::Plain(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
            SecureSocketRecvAsync::Tls(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
        }
    }
}

impl AsyncWrite for SecureSocketSendAsync {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketSendAsync::Plain(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            }
            SecureSocketSendAsync::Tls(inner) => {
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
            SecureSocketSendAsync::Plain(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
            SecureSocketSendAsync::Tls(inner) => {
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
            SecureSocketSendAsync::Plain(inner) => {
                Pin::new(inner).poll_close(cx)
            }
            SecureSocketSendAsync::Tls(inner) => {
                Pin::new(inner).poll_close(cx)
            }
        }
    }
}

// set listener socket options; translated from BFT-SMaRt
#[inline]
fn set_listener_options(listener: AsyncListener) -> io::Result<AsyncListener> {
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
fn set_listener_options_replica(listener: SyncListener) -> io::Result<SyncListener> {
    let sock = socket2::SockRef::from(&listener.inner);
    sock.set_send_buffer_size(8 * 1024 * 1024)?;
    sock.set_recv_buffer_size(8 * 1024 * 1024)?;
    sock.set_reuse_address(true)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    // ChannelOption.SO_BACKLOG ??
    Ok(listener)
}


// set connection socket options; translated from BFT-SMaRt
#[inline]
fn set_sockstream_options(connection: AsyncSocket) -> io::Result<AsyncSocket> {
    let sock = socket2::SockRef::from(&connection.inner);
    sock.set_send_buffer_size(8 * 1024 * 1024)?;
    sock.set_recv_buffer_size(8 * 1024 * 1024)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}


#[inline]
fn set_sockstream_options_sync(connection: SyncSocket) -> io::Result<SyncSocket> {
    let sock = socket2::SockRef::from(&connection.inner);
    sock.set_send_buffer_size(8 * 1024 * 1024)?;
    sock.set_recv_buffer_size(8 * 1024 * 1024)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}
