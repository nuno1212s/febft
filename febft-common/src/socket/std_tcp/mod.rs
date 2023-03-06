use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};

pub struct Socket {
    inner: TcpStream,
}

pub struct Listener {
    inner: TcpListener,
}

pub struct WriteSocket {
    inner: TcpStream
}

pub struct ReadSocket {
    inner: TcpStream
}

pub fn split(socket: Socket) -> (ReadSocket, WriteSocket) {
    let stream = socket.inner;

    let stream_clone = stream.try_clone().expect("Failed to split socket");

    (ReadSocket { inner: stream }, WriteSocket { inner: stream_clone })
}

pub fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    TcpListener::bind(addr.into())
        .map(Listener::new)
}

pub fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    TcpStream::connect(addr.into())
        .map(|s| {Socket::new(s)})
}

impl Listener {
    fn new(inner: TcpListener) -> Self {
        Listener { inner }
    }

    pub fn accept(&self) -> io::Result<Socket> {
        self.inner
            .accept()
            .map(|(s, _)| Socket::new(s))
    }
}

impl Socket {
    fn new(inner: TcpStream) -> Self {
        Socket { inner }
    }
}

impl Read for ReadSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Write for WriteSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Deref for Socket {
    type Target = std::net::TcpStream;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Socket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(windows)]
mod sys {
    compile_error!("Sorry Windows users! Switch to the `async-std` socket backend.");
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
