#[cfg(feature = "tokio_tcp")]
mod tokio_tcp;

use std::io;
use std::net::SocketAddr;

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

impl Kind {
    pub async fn bind<A: Into<SocketAddr>>(self, addr: A) -> io::Result<Listener> {
        match self {
            #[cfg(feature = "tokio_tcp")]
            Kind::TokioTcp => tokio_tcp::bind(addr).await
                .map(ListenerBackend::TokioTcp)
                .map(Listener::new)
        }
    }

    pub async fn connect<A: Into<SocketAddr>>(self, addr: A) -> io::Result<Socket> {
        match self {
            #[cfg(feature = "tokio_tcp")]
            Kind::TokioTcp => tokio_tcp::connect(addr).await
                .map(SocketBackend::TokioTcp)
                .map(Socket::new)
        }
    }
}

impl Listener {
    fn new(inner: ListenerBackend) -> Self {
        ListenerBackend { inner }
    }
}

impl Socket {
    fn new(inner: SocketBackend) -> Self {
        SocketBackend { inner }
    }
}
