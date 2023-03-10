// FIXME: not working
// https://unixism.net/2020/04/io-uring-by-example-part-1-introduction/
//
// XXX: try this as an alternative, and purge rio backend:
// https://docs.rs/nuclei/0.1.3/nuclei/index.html
// https://github.com/vertexclique/nuclei

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use std::task::{Poll, Context};
use std::net::{SocketAddr, TcpListener, TcpStream};

use rio::{Uring, Completion};
use futures::io::{AsyncRead, AsyncWrite};
use socket2::{Protocol, Socket as SSocket, Domain, Type};

use crate::bft::globals::Global;
use crate::bft::error::{
    self,
    ErrorKind,
    ResultWrappedExt,
    ResultSimpleExt,
};

// the same type used by rio 0.9
struct Rio(Arc<Uring>);

// global `Uring` instance
static mut RIO: Global<Uring> = Global::new();

// ordering of uring operations; link will wait for previous
// ops to finish before executing the requested op
const ORD: rio::Ordering = rio::Ordering::Link;

// initialize the global `Uring` instance
pub unsafe fn init() -> error::Result<()> {
    let ring = rio::new()
        .wrapped(ErrorKind::CommunicationSocketRioTcp)?;
    let ring = {
        // remove `Arc` wrapping because direct access to
        // the `Uring` is faster than going through another
        // layer of indirection; the `Arc` is not necessary
        // because we are using a global `Uring`, wrapped in
        // a `OnceCell`
        let ring: Rio = std::mem::transmute(ring);
        Arc::try_unwrap(ring.0).unwrap()
    };
    RIO.set(ring);
    Ok(())
}

// drop the global `Uring` instance
pub unsafe fn drop() -> error::Result<()> {
    RIO.drop();
    Ok(())
}

pub struct Socket {
    inner: TcpStream,
    reading: Option<Completion<'static, usize>>,
    writing: Option<Completion<'static, usize>>,
}

pub struct Listener {
    inner: TcpListener,
}

// bind won't actually be asynchronous, but we'll only call it once
// throughout the library, anyway
pub async fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Listener> {
    TcpListener::bind(addr.into())
        .map(|inner| Listener { inner })
}

pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Socket> {
    let addr = addr.into();
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let protocol = Some(Protocol::TCP);
    let ttype = Type::STREAM;
    let socket = SSocket::new(domain, ttype, protocol)?;
    ring().connect(&socket, &addr, ORD).await?;
    let inner: TcpStream = socket.into();
    Ok(Socket { inner, reading: None, writing: None })
}

impl Listener {
    pub async fn accept(&self) -> io::Result<Socket> {
        ring()
            .accept(&self.inner)
            .await
            .map(|inner| Socket { inner, reading: None, writing: None })
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &mut [u8]
    ) -> Poll<io::Result<usize>>
    {
        let this = &mut *self;
        match &mut this.reading {
            Some(ref mut c) => match Pin::new(c).poll(cx) {
                p @ Poll::Ready(_) => {
                    this.reading = None;
                    p
                },
                Poll::Pending => Poll::Pending,
            },
            None => {
                let mut c = ring()
                    .recv_ordered(&self.inner, &mut buf, ORD);
                match Pin::new(&mut c).poll(cx) {
                    p @ Poll::Ready(_) => p,
                    Poll::Pending => {
                        this.reading = Some(c);
                        Poll::Pending
                    },
                }
            },
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>, 
        cx: &mut Context<'_>, 
        buf: &[u8]
    ) -> Poll<io::Result<usize>>
    {
        let mut completion = ring()
            .send_ordered(&self.inner, &buf, ORD);
        Pin::new(&mut completion).poll(cx)
    }

    fn poll_flush(
        self: Pin<&mut Self>, 
        cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>, 
        _cx: &mut Context<'_>
    ) -> Poll<io::Result<()>>
    {
        Poll::Ready(Ok(()))
    }
}

#[inline(always)]
fn ring() -> &'static Uring {
    match unsafe { RIO.get() } {
        Some(ref ring) => ring,
        None => panic!("Linux io_uring wasn't initialized"),
    }
}
