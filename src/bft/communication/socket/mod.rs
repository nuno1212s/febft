#[cfg(feature = "tokio_tcp")]
mod tokio_tcp;

use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use futures::io::{AsyncRead, AsyncWrite};

#[derive(Copy, Clone)]
pub enum Kind {
    #[cfg(feature = "tokio_tcp")]
    TokioTcp,
}

enum ListenerBackend {
    #[cfg(feature = "tokio_tcp")]
    TokioTcp(tokio_tcp::Listener),
}

enum SocketBackend {
    #[cfg(feature = "tokio_tcp")]
    TokioTcp(tokio_tcp::Socket),
}

pub struct Listener {
    inner: ListenerBackend,
}

pub struct Socket {
    inner: SocketBackend,
}

pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    #[cfg(feature = "tokio_tcp")]
    Kind::TokioTcp.bind(addr).await
}

pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    #[cfg(feature = "tokio_tcp")]
    Kind::TokioTcp.connect(addr).await
}

impl Kind {
    pub async fn bind<A: Into<SocketAddr>>(self, addr: A) -> io::Result<Listener> {
        match self {
            #[cfg(feature = "tokio_tcp")]
            Kind::TokioTcp => tokio_tcp::bind(addr)
                .await
                .map(ListenerBackend::TokioTcp)
                .map(Listener::new),
        }
    }

    pub async fn connect<A: Into<SocketAddr>>(self, addr: A) -> io::Result<Socket> {
        match self {
            #[cfg(feature = "tokio_tcp")]
            Kind::TokioTcp => tokio_tcp::connect(addr)
                .await
                .map(SocketBackend::TokioTcp)
                .map(Socket::new),
        }
    }
}

impl Listener {
    fn new(inner: ListenerBackend) -> Self {
        Listener { inner }
    }

    pub async fn accept(&self) -> io::Result<Socket> {
        match &self.inner {
            #[cfg(feature = "tokio_tcp")]
            ListenerBackend::TokioTcp(ref s) => s.accept()
                .await
                .map(SocketBackend::TokioTcp)
                .map(Socket::new),
        }
    }
}

impl Socket {
    fn new(inner: SocketBackend) -> Self {
        Socket { inner }
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &mut [u8]
    ) -> Poll<io::Result<usize>>
    {
        match &mut self.inner {
            #[cfg(feature = "tokio_tcp")]
            SocketBackend::TokioTcp(ref mut s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &[u8]
    ) -> Poll<io::Result<usize>>
    {
        match &mut self.inner {
            #[cfg(feature = "tokio_tcp")]
            SocketBackend::TokioTcp(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        match &mut self.inner {
            #[cfg(feature = "tokio_tcp")]
            SocketBackend::TokioTcp(ref mut s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        match &mut self.inner {
            #[cfg(feature = "tokio_tcp")]
            SocketBackend::TokioTcp(ref mut s) => Pin::new(s).poll_close(cx),
        }
    }
}
