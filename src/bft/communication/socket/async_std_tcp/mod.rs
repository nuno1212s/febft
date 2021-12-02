use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::task::{Poll, Context};

use futures::io::{AsyncRead, AsyncWrite};
use ::async_std::net::{TcpListener, TcpStream};

pub struct Listener {
    inner: TcpListener,
}

pub struct Socket {
    inner: TcpStream,
}

#[tracing::instrument(skip_all)] pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    let inner = TcpListener::bind(addr.into()).await?;
    Ok(Listener { inner })
}

#[tracing::instrument(skip_all)] pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    TcpStream::connect(addr.into())
        .await
        .map(|inner| Socket { inner })
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

impl Listener {
    #[tracing::instrument(skip_all)] pub async fn accept(&self) -> io::Result<Socket> {
        self.inner
            .accept()
            .await
            .map(|(inner, _)| Socket { inner })
    }
}
