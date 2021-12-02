use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use tokio::net::{TcpStream, TcpListener};
use futures::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{
    Compat,
    TokioAsyncReadCompatExt,
};

pub struct Socket {
    inner: Compat<TcpStream>,
}

pub struct Listener {
    inner: TcpListener,
}

#[tracing::instrument(skip_all)] pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    TcpListener::bind(addr.into())
        .await
        .map(Listener::new)
}

#[tracing::instrument(skip_all)] pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    TcpStream::connect(addr.into())
        .await
        .map(|s| Socket::new(s.compat()))
}

impl Listener {
    fn new(inner: TcpListener) -> Self {
        Listener { inner }
    }

    #[tracing::instrument(skip_all)] pub async fn accept(&self) -> io::Result<Socket> {
        self.inner
            .accept()
            .await
            .map(|(s, _)| Socket::new(s.compat()))
    }
}

impl Socket {
    fn new(inner: Compat<TcpStream>) -> Self {
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
