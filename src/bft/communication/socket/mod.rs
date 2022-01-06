//! Abstractions over different socket types of crates in the Rust ecosystem.

#[cfg(feature = "socket_tokio_tcp")]
mod tokio_tcp;

#[cfg(feature = "socket_async_std_tcp")]
mod async_std_tcp;

#[cfg(feature = "socket_nuclei_tcp")]
mod nuclei_tcp;

#[cfg(feature = "socket_rio_tcp")]
mod rio_tcp;

use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use futures::io::{
    AsyncRead,
    AsyncWrite,
    BufWriter,
    BufReader,
};
use async_tls::{
    server::TlsStream as TlsStreamSrv,
    client::TlsStream as TlsStreamCli,
};

use crate::bft::error;

/// A `Listener` represents a socket listening on new communications
/// initiated by peer nodes in the BFT system.
pub struct Listener {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Listener,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Listener,

    #[cfg(feature = "socket_nuclei_tcp")]
    inner: nuclei_tcp::Listener,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Listener,
}

/// A `Socket` represents a connection between two peer processes
/// in the BFT system.
pub struct Socket {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Socket,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Socket,

    #[cfg(feature = "socket_nuclei_tcp")]
    inner: nuclei_tcp::Socket,

    #[cfg(feature = "socket_rio_tcp")]
    inner: rio_tcp::Socket,
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

        #[cfg(feature = "socket_nuclei_tcp")]
        { nuclei_tcp::bind(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::bind(addr).await }
    }.and_then(|inner| set_listener_options(Listener { inner }))
}

/// Connects to the remote node pointed to by the address `addr`.
pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
        { tokio_tcp::connect(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
        { async_std_tcp::connect(addr).await }

        #[cfg(feature = "socket_nuclei_tcp")]
        { nuclei_tcp::connect(addr).await }

        #[cfg(feature = "socket_rio_tcp")]
        { rio_tcp::connect(addr).await }
    }.and_then(|inner| set_sockstream_options(Socket { inner }))
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Socket> {
        self.inner.accept()
            .await
            .and_then(|inner| set_sockstream_options(Socket { inner }))
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &mut [u8]
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &[u8]
    ) -> Poll<io::Result<usize>>
    {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

pub enum SecureSocketRecv {
    Plain(BufReader<Socket>),
    Tls(TlsStreamSrv<Socket>),
}

pub enum SecureSocketSend {
    Plain(BufWriter<Socket>),
    Tls(TlsStreamCli<Socket>),
}

impl AsyncRead for SecureSocketRecv {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8]
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketRecv::Plain(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            },
            SecureSocketRecv::Tls(inner) => {
                Pin::new(inner).poll_read(cx, buf)
            },
        }
    }
}

impl AsyncWrite for SecureSocketSend {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8]
    ) -> Poll<io::Result<usize>>
    {
        match &mut *self {
            SecureSocketSend::Plain(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            },
            SecureSocketSend::Tls(inner) => {
                Pin::new(inner).poll_write(cx, buf)
            },
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        match &mut *self {
            SecureSocketSend::Plain(inner) => {
                Pin::new(inner).poll_flush(cx)
            },
            SecureSocketSend::Tls(inner) => {
                Pin::new(inner).poll_flush(cx)
            },
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        match &mut *self {
            SecureSocketSend::Plain(inner) => {
                Pin::new(inner).poll_close(cx)
            },
            SecureSocketSend::Tls(inner) => {
                Pin::new(inner).poll_close(cx)
            },
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
