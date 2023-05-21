use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use log::error;
use mio::Token;
use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};
use febft_common::error::*;
use febft_common::node_id::NodeId;
use febft_common::socket::MioSocket;
use crate::mio_tcp::connections::{Connections, PeerConnection};
use crate::mio_tcp::connections::epoll_group::epoll_workers::EpollWorker;
use crate::serialize::Serializable;

pub mod epoll_workers;

pub type EpollWorkerId = u32;

// This will just handle creating and deleting connections so it can be small
pub const DEFAULT_WORKER_CHANNEL: usize = 128;

pub fn init_worker_group_handle<M: Serializable + 'static>(worker_count: u32) -> (EpollWorkerGroupHandle<M>, Vec<ChannelSyncRx<EpollWorkerMessage<M>>>) {
    let mut workers = Vec::with_capacity(worker_count as usize);

    let mut receivers = Vec::with_capacity(worker_count as usize);

    for _ in 0..worker_count {
        let (tx, rx) = channel::new_bounded_sync(DEFAULT_WORKER_CHANNEL);

        workers.push(tx);
        receivers.push(rx);
    }

    (EpollWorkerGroupHandle {
        workers,
        round_robin: AtomicUsize::new(0),
    }, receivers)
}

pub fn initialize_worker_group<M: Serializable + 'static>(connections: Arc<Connections<M>>, receivers: Vec<ChannelSyncRx<EpollWorkerMessage<M>>>) -> Result<()> {
    for (worker_id, rx) in receivers.into_iter().enumerate() {
        let worker = EpollWorker::new(worker_id as u32, connections.clone(), rx)?;

        std::thread::Builder::new().name(format!("Epoll Worker {}", worker_id))
            .spawn(move || {
                if let Err(err) = worker.epoll_worker_loop() {
                    error!("Epoll worker {} failed with error: {:?}", worker_id,err);
                }
            }).expect("Failed to launch worker thread");
    }

    Ok(())
}

/// A handle to the worker group that handles the epoll events
/// Allows us to register new connections to the epoll workers
pub struct EpollWorkerGroupHandle<M: Serializable + 'static> {
    workers: Vec<ChannelSyncTx<EpollWorkerMessage<M>>>,
    round_robin: AtomicUsize,
}

pub struct NewConnection<M: Serializable + 'static> {
    conn_id: u32,
    peer_id: NodeId,
    my_id: NodeId,
    socket: MioSocket,
    peer_conn: Arc<PeerConnection<M>>,
}

pub enum EpollWorkerMessage<M: Serializable + 'static> {
    NewConnection(NewConnection<M>),
    CloseConnection(Token),
}

impl<M: Serializable + 'static> EpollWorkerGroupHandle<M> {
    /// Assigns a socket to any given worker
    pub fn assign_socket_to_worker(&self, conn_details: NewConnection<M>) -> febft_common::error::Result<()> {
        let round_robin = self.round_robin.fetch_add(1, Ordering::Relaxed);

        let worker = self.workers.get(round_robin % self.workers.len())
            .ok_or(Error::simple_with_msg(ErrorKind::Communication, "Failed to get worker for connection?"))?;

        worker.send(EpollWorkerMessage::NewConnection(conn_details)).wrapped(ErrorKind::Communication)?;

        Ok(())
    }

    /// Order a disconnection of a given connection from a worker
    pub fn disconnect_connection_from_worker(&self, epoll_worker: EpollWorkerId, conn_id: Token) -> febft_common::error::Result<()> {
        let worker = self.workers.get(epoll_worker as usize)
            .ok_or(Error::simple_with_msg(ErrorKind::Communication, "Failed to get worker for connection?"))?;

        worker.send(EpollWorkerMessage::CloseConnection(conn_id)).wrapped(ErrorKind::Communication)?;

        Ok(())
    }
}

impl<M: Serializable + 'static> Clone for EpollWorkerGroupHandle<M> {
    fn clone(&self) -> Self {
        Self {
            workers: self.workers.clone(),
            round_robin: AtomicUsize::new(0),
        }
    }
}
