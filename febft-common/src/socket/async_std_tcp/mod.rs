use std::io;
use std::pin::Pin;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::task::{Poll, Context};
use futures::AsyncReadExt;

use futures::io::{AsyncRead, AsyncWrite};
use async_std::net::{TcpListener, TcpStream};

pub struct Listener {
    inner: TcpListener,
}

pub struct Socket {
    inner: TcpStream,
}

pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    let inner = TcpListener::bind(addr.into()).await?;
    Ok(Listener { inner })
}

pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    TcpStream::connect(addr.into())
        .await
        .map(|inner| Socket { inner })
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

impl Listener {
    pub async fn accept(&self) -> io::Result<Socket> {
        self.inner
            .accept()
            .await
            .map(|(inner, _)| Socket { inner })
    }
}

/// The write half of a socket
pub struct WriteHalf {
    inner: futures::io::WriteHalf<TcpStream>,
}

/// The read half of a socket
pub struct ReadHalf {
    inner: futures::io::ReadHalf<TcpStream>,
}

impl Deref for WriteHalf {
    type Target = futures::io::WriteHalf<TcpStream>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WriteHalf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for ReadHalf {
    type Target = futures::io::ReadHalf<TcpStream>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(super) fn split_socket(sock: Socket) -> (WriteHalf, ReadHalf) {
    let (read, write) = sock.split();

    (WriteHalf { inner: write },
     ReadHalf { inner: read })
}

#[cfg(windows)]
mod sys {
    use std::os::windows::io::{RawSocket, AsRawSocket};

    impl AsRawSocket for super::Socket {
        fn as_raw_socket(&self) -> RawSocket {
            self.inner.as_raw_socket()
        }
    }

    impl AsRawSocket for super::Listener {
        fn as_raw_socket(&self) -> RawSocket {
            self.inner.as_raw_socket()
        }
    }
}

#[cfg(unix)]
mod sys {
    use std::os::unix::io::{RawFd, AsRawFd};

    impl AsRawFd for super::Socket {
        fn as_raw_fd(&self) -> RawFd {
            self.inner.as_raw_fd()
        }
    }

    impl AsRawFd for super::Listener {
        fn as_raw_fd(&self) -> RawFd {
            self.inner.as_raw_fd()
        }
    }
}
