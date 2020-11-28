#[cfg(feature = "socket_tokio_tcp")]
mod tokio_tcp;

#[cfg(feature = "socket_async_std_tcp")]
mod async_std_tcp;

use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use futures::io::{AsyncRead, AsyncWrite};

pub struct Listener {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Listener,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Listener,
}

pub struct Socket {
    #[cfg(feature = "socket_tokio_tcp")]
    inner: tokio_tcp::Socket,

    #[cfg(feature = "socket_async_std_tcp")]
    inner: async_std_tcp::Socket,
}

pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
        { tokio_tcp::bind(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
        { async_std_tcp::bind(addr).await }
    }.map(|inner| Listener { inner })
}

pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    {
        #[cfg(feature = "socket_tokio_tcp")]
        { tokio_tcp::connect(addr).await }

        #[cfg(feature = "socket_async_std_tcp")]
        { async_std_tcp::connect(addr).await }
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
