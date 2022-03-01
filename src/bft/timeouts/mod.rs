//! Module to manage timeouts in `febft`.
//!
//! This includes on-going client requests, as well as CST and
//! view change messages exchanged between replicas.

use std::time::Duration;
use std::marker::PhantomData;
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;

use intmap::IntMap;
use parking_lot::Mutex;
use futures_timer::Delay;

use crate::bft::ordering;
use crate::bft::async_runtime as rt;
use crate::bft::communication::message::Message;
use crate::bft::communication::peer_handling::ConnectedPeer;
use crate::bft::communication::serialize::SharedData;
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

type SeqNo = u64;
type AtomicSeqNo = AtomicU64;

pub enum TimeoutKind {
    /// Timeout pertaining toe the `CST` protocol.
    Cst(ordering::SeqNo),
    /// Timeout pertaining to a group of client requests
    /// awaiting to be decided.
    ClientRequests(ordering::SeqNo),
    // TODO: add the rest of the timeout kinds,
    // e.g. client requests
}

struct TimeoutsHandleShared {
    current_seq_no: AtomicSeqNo,
    canceled: Mutex<IntMap<()>>,
}

pub struct TimeoutsHandle<S: Service> {
    shared: Arc<TimeoutsHandleShared>,
    system_tx: Arc<ConnectedPeer<Message<State<S>, Request<S>, Reply<S>>>>,
}

pub struct TimeoutHandle {
    seq: SeqNo,
    shared: Arc<TimeoutsHandleShared>,
}

pub struct Timeouts<S: Service> {
    _marker: PhantomData<S>,
}

impl TimeoutHandle {
    /// Cancels the timeout associated with this handle.
    pub fn cancel(self) {
        self.shared.cancel(self.seq);
    }
}

impl TimeoutsHandleShared {
    fn gen_seq_no(&self) -> SeqNo {
        self.current_seq_no.fetch_add(1, atomic::Ordering::Relaxed)
    }

    fn cancel(&self, seq: SeqNo) {
        let mut canceled = self.canceled.lock();
        canceled.insert(seq, ());
    }

    fn was_canceled(&self, seq: SeqNo) -> bool {
        let mut canceled = self.canceled.lock();
        canceled.remove(seq).is_some()
    }
}

impl<S: Service> Clone for TimeoutsHandle<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            system_tx: self.system_tx.clone(),
        }
    }
}

impl<S: Service> TimeoutsHandle<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Creates a new timeout event, that will fire after a duration of `dur`.
    pub fn timeout(&self, dur: Duration, kind: TimeoutKind) {
        let mut system_tx = self.system_tx.clone();
        rt::spawn(async move {
            Delay::new(dur).await;
            system_tx.push_request(Message::Timeout(kind)).await;
        });
    }

    /// Creates a new timeout event, that will fire after a duration of `dur`.
    ///
    /// Different from `timeout()`, this method returns a handle that allows the user
    /// to cancel the timeout before it is triggered.
    pub fn timeout_with_cancel(&self, dur: Duration, kind: TimeoutKind) -> TimeoutHandle {
        let mut system_tx = self.system_tx.clone();
        let seq = self.shared.gen_seq_no();

        let shared = Arc::clone(&self.shared);

        rt::spawn(async move {
            Delay::new(dur).await;
            if !shared.was_canceled(seq) {
                system_tx.push_request(Message::Timeout(kind)).await;
            }
        });

        let shared = Arc::clone(&self.shared);
        TimeoutHandle { shared, seq }
    }
}

impl<S: Service> Timeouts<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    pub fn new(
        system_tx: Arc<ConnectedPeer<Message<State<S>, Request<S>, Reply<S>>>>,
    ) -> Arc<TimeoutsHandle<S>> {
        let shared = Arc::new(TimeoutsHandleShared {
            canceled: Mutex::new(IntMap::new()),
            current_seq_no: AtomicSeqNo::new(0),
        });
        Arc::new(TimeoutsHandle { system_tx, shared })
    }
}
