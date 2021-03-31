//! Abstractions over different socket types of crates in the Rust ecosystem.

#[cfg(feature = "socket_tokio_tcp")]
mod tokio_tcp;

#[cfg(feature = "socket_async_std_tcp")]
mod async_std_tcp;

#[cfg(feature = "socket_rio_tcp")]
mod rio_tcp;

use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use futures::io::{AsyncRead, AsyncWrite};

use crate::bft::error;

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

/// Initialize the sockets module.
pub fn init() -> error::Result<()> {
    #[cfg(feature = "socket_rio_tcp")]
    { rio_tcp::init()?; }

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
    }.map(|inner| Listener { inner })
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
    }.map(|inner| Socket { inner })
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Socket> {
        self.inner.accept()
            .await
            .map(|inner| Socket { inner })
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
