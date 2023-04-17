//! Abstractions over different socket types of crates in the Rust ecosystem.

use std::fs::read;
use std::io;
use std::io::{BufRead, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use either::Either;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite};

use futures::io::{BufReader, BufWriter};
use log::error;

use rustls::{ClientConnection, Error, IoState, ServerConnection, StreamOwned};
use tokio::io::ReadBuf;
use tokio_rustls::TlsStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt, FuturesAsyncReadCompatExt};

use crate::error;

#[cfg(feature = "socket_tokio_tcp")]
mod tokio_tcp;

#[cfg(feature = "socket_async_std_tcp")]
mod async_std_tcp;

#[cfg(feature = "socket_rio_tcp")]
mod rio_tcp;

mod std_tcp;

const WRITE_BUFFER_SIZE: usize = 8 * 1024;
const READ_BUFFER_SIZE: usize = 8 * 1024;

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

impl AsyncSocket {
    pub fn compat_layer(self) -> Compat<Self> {
        self.compat()
    }

    pub(super) fn split(self) -> (WriteHalfAsync, ReadHalfAsync) {
        #[cfg(feature = "socket_tokio_tcp")]
            let (write, read) = tokio_tcp::split_socket(self.inner);
        #[cfg(feature = "socket_async_std_tcp")]
            let (write, read) = async_std_tcp::split_socket(self.inner);

        //Buffer both the connections
        let write_buffered = BufWriter::with_capacity(WRITE_BUFFER_SIZE, write);
        let read_buffered = BufReader::with_capacity(READ_BUFFER_SIZE, read);

        (WriteHalfAsync {
            inner: write_buffered,
        }, ReadHalfAsync {
            inner: read_buffered
        })
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

pub enum SecureWriteHalf {
    Async(SecureWriteHalfAsync),
    Sync(SecureWriteHalfSync),
}

pub enum SecureReadHalf {
    Async(SecureReadHalfAsync),
    Sync(SecureReadHalfSync),
}

pub enum SecureSocketSync {
    Plain(SyncSocket),
    Tls(Either<ClientConnection, ServerConnection>, SyncSocket),
}

impl SecureSocketSync {
    pub fn new_plain(socket: SyncSocket) -> Self {
        Self::Plain(socket)
    }

    pub fn new_tls_server(tls_conn: ServerConnection, socket: SyncSocket) -> Self {
        Self::Tls(Either::Right(tls_conn), socket)
    }

    pub fn new_tls_client(tls_conn: ClientConnection, socket: SyncSocket) -> Self {
        Self::Tls(Either::Left(tls_conn), socket)
    }

    pub fn split(self) -> (SecureWriteHalfSync, SecureReadHalfSync) {
        match self {
            SecureSocketSync::Plain(socket) => {
                let (write_half, read_half) = socket.split();

                (SecureWriteHalfSync::Plain(write_half),
                 SecureReadHalfSync::Plain(read_half))
            }
            SecureSocketSync::Tls(connection, socket) => {
                let (write, read) = socket.split();

                let shared_conn = Arc::new(Mutex::new(connection));

                (SecureWriteHalfSync::Tls(shared_conn.clone(), write),
                 SecureReadHalfSync::Tls(shared_conn, read))
            }
        }
    }
}

pub enum SecureWriteHalfSync {
    Plain(WriteHalfSync),
    Tls(Arc<Mutex<Either<ClientConnection, ServerConnection>>>, WriteHalfSync),
}

pub enum SecureReadHalfSync {
    Plain(ReadHalfSync),
    Tls(Arc<Mutex<Either<ClientConnection, ServerConnection>>>, ReadHalfSync),
}

impl Read for SecureReadHalfSync {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            SecureReadHalfSync::Plain(plain) => {
                std::io::Read::read(plain, buf)
            }
            SecureReadHalfSync::Tls(tls_conn, sock) => {
                match &mut *tls_conn.lock().unwrap() {
                    Either::Left(tls_conn) => {
                        tls_conn.read_tls(sock)?;

                        match tls_conn.process_new_packets() {
                            Ok(state) => {
                                //FIXME: Is this correct, since we can read a different
                                // Amount of packets as in the read tls one above
                                if state.plaintext_bytes_to_read() > 0 {
                                    tls_conn.reader().read(buf)
                                } else {
                                    Ok(0)
                                }
                            }
                            Err(err) => {
                                error!("Failed to process new tls packets {:?}", err);

                                Err(io::Error::new(ErrorKind::BrokenPipe, ""))
                            }
                        }
                    }
                    Either::Right(tls_conn) => {
                        tls_conn.read_tls(sock)?;

                        match tls_conn.process_new_packets() {
                            Ok(state) => {
                                //FIXME: Is this correct, since we can read a different
                                // Amount of packets as in the read tls one above
                                if state.plaintext_bytes_to_read() > 0 {
                                    tls_conn.reader().read(buf)
                                } else {
                                    Ok(0)
                                }
                            }
                            Err(err) => {
                                error!("Failed to process new tls packets {:?}", err);

                                Err(io::Error::new(ErrorKind::BrokenPipe, ""))
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Write for SecureWriteHalfSync {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            SecureWriteHalfSync::Plain(write_half) => {
                write_half.write(buf)
            }
            SecureWriteHalfSync::Tls(tls_conn, socket) => {
                match &mut *tls_conn.lock().unwrap() {
                    Either::Left(tls_conn) => {
                        tls_conn.writer().write(buf)?;

                        //FIXME: Is this even correct? Will it write the correct amount of bytes?
                        // Since we are returning a different result that can write a different
                        // Amount of bytes, this can get confused and miss count it?
                        match tls_conn.process_new_packets() {
                            Ok(state) => {
                                //FIXME: Is this correct, since we can read a different
                                // Amount of packets as in the read tls one above

                                if state.tls_bytes_to_write() > 0 {
                                    tls_conn.write_tls(socket)
                                } else {
                                    Ok(0)
                                }
                            }
                            Err(err) => {
                                error!("Failed to process new tls packets {:?}", err);

                                Err(io::Error::new(ErrorKind::BrokenPipe, ""))
                            }
                        }
                    }
                    Either::Right(tls_conn) => {
                        tls_conn.writer().write(buf)?;

                        //FIXME: Is this even correct? Will it write the correct amount of bytes?
                        // Since we are returning a different result that can write a different
                        // Amount of bytes, this can get confused and miss count it?
                        match tls_conn.process_new_packets() {
                            Ok(state) => {
                                //FIXME: Is this correct, since we can read a different
                                // Amount of packets as in the read tls one above

                                if state.tls_bytes_to_write() > 0 {
                                    tls_conn.write_tls(socket)
                                } else {
                                    Ok(0)
                                }
                            }
                            Err(err) => {
                                error!("Failed to process new tls packets {:?}", err);

                                Err(io::Error::new(ErrorKind::BrokenPipe, ""))
                            }
                        }
                    }
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            SecureWriteHalfSync::Plain(socket) => {
                socket.flush()
            }
            SecureWriteHalfSync::Tls(tls_conn, socket2) => {
                match &mut *tls_conn.lock().unwrap() {
                    Either::Left(tls_conn) => {
                        while tls_conn.wants_write() {
                            tls_conn.write_tls(socket2)?;
                        }

                        socket2.flush()
                    }
                    Either::Right(tls_conn) => {
                        while tls_conn.wants_write() {
                            tls_conn.write_tls(socket2)?;
                        }

                        socket2.flush()
                    }
                }
            }
        }
    }
}

pub enum SecureSocketAsync {
    Plain(AsyncSocket),
    Tls(TlsStream<Compat<AsyncSocket>>),
}

impl SecureSocketAsync {
    pub fn new_plain(socket: AsyncSocket) -> Self {
        Self::Plain(socket)
    }

    pub fn new_tls(socket: TlsStream<Compat<AsyncSocket>>) -> Self {
        Self::Tls(socket)
    }

    pub fn split(self) -> (SecureWriteHalfAsync, SecureReadHalfAsync) {
        match self {
            SecureSocketAsync::Plain(socket) => {
                let (write, read) = socket.split();

                (SecureWriteHalfAsync::Plain(write),
                 SecureReadHalfAsync::Plain(read))
            }
            SecureSocketAsync::Tls(tls_stream) => {
                //FIXME: We have found that TlsStreams really don't like to be used in duplex mode
                // especially with async-tls, which is what we were using to maintain compatibility
                // between async-std and tokio. This means that we currently have a pretty hard time
                // implementing this.
                // https://github.com/tokio-rs/tls/issues/40

                //Unfortunately in this situation I don't think we can use async socket's efficient OS level split
                // Since the stream requires duplex access. So we must wrap this in a bilock from futures

                //Wrap in bilock
                let (read, write) = tokio::io::split(tls_stream);

                //We have to wrap these at this point instead of at the lower level
                // since we can't use the same method but that's fine I guess
                let read_buffered = BufReader::with_capacity(READ_BUFFER_SIZE, read.compat());
                let write_buffered = BufWriter::with_capacity(WRITE_BUFFER_SIZE, write.compat_write());

                (SecureWriteHalfAsync::Tls(write_buffered),
                 SecureReadHalfAsync::Tls(read_buffered))
            }
        }
    }
}

pub enum SecureWriteHalfAsync {
    Plain(WriteHalfAsync),
    Tls(BufWriter<Compat<tokio::io::WriteHalf<TlsStream<Compat<AsyncSocket>>>>>),
}

pub enum SecureReadHalfAsync {
    Plain(ReadHalfAsync),
    Tls(BufReader<Compat<tokio::io::ReadHalf<TlsStream<Compat<AsyncSocket>>>>>),
}

impl AsyncWrite for SecureWriteHalfAsync {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match &mut *self {
            SecureWriteHalfAsync::Plain(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            }
            SecureWriteHalfAsync::Tls(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            SecureWriteHalfAsync::Plain(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
            SecureWriteHalfAsync::Tls(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            SecureWriteHalfAsync::Plain(inner) => {
                Pin::new(inner).poll_close(cx)
            }
            SecureWriteHalfAsync::Tls(inner) => {
                Pin::new(inner).poll_flush(cx)
            }
        }
    }
}

impl AsyncRead for SecureReadHalfAsync {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match &mut *self {
            SecureReadHalfAsync::Plain(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
            SecureReadHalfAsync::Tls(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            }
        }
    }
}

pub enum WriteHalf {
    Async(WriteHalfAsync),
    Sync(WriteHalfSync),
}

pub struct WriteHalfAsync {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: BufWriter<tokio_tcp::WriteHalf>,
    #[cfg(feature = "socket_async_tcp")]
    inner: BufWriter<async_std_tcp::WriteHalf>,
}

pub struct WriteHalfSync {
    inner: io::BufWriter<std_tcp::WriteHalf>,
}

pub enum ReadHalf {
    Async(ReadHalfAsync),
    Sync(ReadHalfSync),
}

pub struct ReadHalfAsync {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: BufReader<tokio_tcp::ReadHalf>,
    #[cfg(feature = "socket_async_tcp")]
    inner: BufReader<async_std_tcp::ReadHalf>,
}

pub struct ReadHalfSync {
    inner: io::BufReader<std_tcp::ReadHalf>,
}

impl AsyncRead for ReadHalfAsync {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WriteHalfAsync {
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

impl Read for ReadHalfSync {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for WriteHalfSync {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl SyncSocket {
    pub fn split(self) -> (WriteHalfSync, ReadHalfSync) {
        let (write, read) = std_tcp::split(self.inner);

        let write_buffered = io::BufWriter::with_capacity(WRITE_BUFFER_SIZE, write);

        let read_buffered = io::BufReader::with_capacity(READ_BUFFER_SIZE, read);

        (WriteHalfSync { inner: write_buffered },
         ReadHalfSync { inner: read_buffered })
    }
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
        std::io::Read::read(&mut self.inner, buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        std::io::Read::read_exact(&mut self.inner, buf)
    }
}

// set listener socket options; translated from BFT-SMaRt
#[inline]
fn set_listener_options(listener: AsyncListener) -> io::Result<AsyncListener> {
    let sock = socket2::SockRef::from(&listener.inner);
    sock.set_send_buffer_size(WRITE_BUFFER_SIZE)?;
    sock.set_recv_buffer_size(READ_BUFFER_SIZE)?;
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
    sock.set_send_buffer_size(WRITE_BUFFER_SIZE)?;
    sock.set_recv_buffer_size(READ_BUFFER_SIZE)?;
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
    sock.set_send_buffer_size(WRITE_BUFFER_SIZE)?;
    sock.set_recv_buffer_size(READ_BUFFER_SIZE)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}


#[inline]
fn set_sockstream_options_sync(connection: SyncSocket) -> io::Result<SyncSocket> {
    let sock = socket2::SockRef::from(&connection.inner);
    sock.set_send_buffer_size(WRITE_BUFFER_SIZE)?;
    sock.set_recv_buffer_size(READ_BUFFER_SIZE)?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    // ChannelOption.CONNECT_TIMEOUT_MILLIS ??
    Ok(connection)
}
