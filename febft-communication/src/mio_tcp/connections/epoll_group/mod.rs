use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use log::error;
use mio::Token;
use febft_common::channel;
use febft_common::channel::ChannelSyncTx;
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

pub fn initialize_worker_group<M: Serializable + 'static>(connections: Arc<Connections<M>>, worker_count: u32) -> Result<EpollWorkerGroupHandle<M>> {
    let mut workers = Vec::with_capacity(worker_count as usize);

    for workerId in 0..worker_count {
        let (tx, rx) = channel::new_bounded_sync(DEFAULT_WORKER_CHANNEL);

        workers.push(tx);

        let worker = EpollWorker::new(workerId, connections.clone(), rx)?;

        std::thread::Builder::new().name(format!("Epoll Worker {}", workerId))
            .spawn(move || {
                loop {
                    if let Err(err) = worker.epoll_worker_loop() {
                        error!("Epoll worker {} failed with error: {:?}", workerId,err);
                    }
                }
            }).expect("Failed to launch worker thread");
    }

    Ok(EpollWorkerGroupHandle {
        workers,
        round_robin: AtomicUsize::new(0),
    })
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
