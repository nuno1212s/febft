use std::io;
use std::time::Duration;
use mio::{Events, Interest, Poll, Registry, Token};
use slab::Slab;
use febft_common::channel::ChannelSyncRx;
use febft_common::node_id::NodeId;
use febft_common::socket::{MioListener, MioSocket};

const EVENT_CAPACITY: usize = 1024;
const WORKER_TIMEOUT: Option<Duration> = Some(Duration::from_millis(1));

const SERVER_TOKEN : Token = Token(0);

pub enum EpollWorkerMessage {
    NewConnection(NodeId, MioSocket),
    CloseConnection(Token)
}

type ConnectionRegister = ChannelSyncRx<MioSocket>;

struct EpollWorker {
    // This slab stores the connections that are currently being handled by this worker
    connections: Slab<MioSocket>,
    // register new connections
    connection_register: ChannelSyncRx<MioSocket>
}

impl EpollWorker {

    fn epoll_worker_loop(mut self) -> io::Result<()> {

        let mut epoll = Poll::new()?;

        let mut event_queue = Events::with_capacity(EVENT_CAPACITY);

        loop {

            epoll.poll(&mut event_queue, WORKER_TIMEOUT)?;

            for event in event_queue.iter() {
                match event.token() {
                    SERVER_TOKEN => {
                        
                    }
                    token => {

                    }
                }
            }

            self.register_connections(epoll.registry())?;
        }

    }

    /// Receive connections from the connection register and register them with the epoll instance
    fn register_connections(&mut self, registry: &Registry) -> io::Result<()> {
        loop {

            match self.connection_register.try_recv() {
                Ok(socket) => {
                    let id = self.connections.insert(socket);

                    let token_id = Token(id);

                    registry.register(&mut self.connections[id], token_id, Interest::READABLE)?;
                }
                Err(err) => {
                    // No more connections are ready to be accepted
                    break;
                }
            }
        }

        Ok(())
    }
}


pub(super) fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

pub(super) fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}
