use std::io;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::net::{SocketAddr, TcpStream, TcpListener};

use futures::io::{AsyncRead, AsyncWrite};
use ::nuclei::Handle;

pub struct Listener {
    inner: Handle<TcpListener>,
}

pub struct Socket {
    inner: Handle<TcpStream>,
}

pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    let inner = Handle::<TcpListener>::bind(addr.into())?;
    Ok(Listener { inner })
}

pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    let inner = Handle::<TcpStream>::connect(addr.into()).await?;
    Ok(Socket { inner })
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
    pub async fn accept(&self) -> io::Result<Socket> {
        self.inner
            .accept()
            .await
            .map(|(inner, _)| Socket { inner })
    }
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
