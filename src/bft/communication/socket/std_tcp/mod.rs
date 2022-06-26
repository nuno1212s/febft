use std::io;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::os::unix::io::FromRawFd;
use std::str::FromStr;
use nix::sys::socket::{AddressFamily, SockaddrIn, SockFlag, SockType};
use tokio::net::TcpSocket;

pub struct Socket {
    inner: TcpStream,
}

pub struct Listener {
    inner: TcpListener,
}

pub fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    TcpListener::bind(addr.into())
        .map(Listener::new)
}

pub fn bind_and_connect<A: Into<SocketAddr>>(addr: A, bind_ip: IpAddr) -> io::Result<Socket> {

    let host = SockaddrIn::from_str(match bind_ip {
        IpAddr::V4(ipv4) => {
            format!("{}", ipv4)
        }
        IpAddr::V6(_) => {
            return Err(std::io::Error::from(ErrorKind::Unsupported));
        }
    }.as_str()).unwrap();

    let raw_fd = match nix::sys::socket::socket(AddressFamily::Inet, SockType::Stream, SockFlag::empty(), None) {
        Ok(raw_fd) => {
            raw_fd
        }
        Err(errno) => {
            return Err(io::Error::from(errno));
        }
    };

    match nix::sys::socket::bind(raw_fd, &host) {
        Err(errno) => {
            return Err(io::Error::from(errno));
        }
        _ => {}
    };

    let connecting_ip = SockaddrIn::from(match addr.into() {
        SocketAddr::V4(v4) => {v4}
        SocketAddr::V6(_) => {
            return Err(std::io::Error::from(ErrorKind::Unsupported));
        }
    });

    match nix::sys::socket::connect(raw_fd, &connecting_ip) {
        Ok(_) => {}
        Err(errno) => {
            return Err(std::io::Error::from(errno));
        }
    };

    let tcp_stream = unsafe {
        TcpStream::from_raw_fd(raw_fd)
    };

    Ok(Socket::new(tcp_stream))
}

pub fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    TcpStream::connect(addr.into())
        .map(|s| Socket::new(s))
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
