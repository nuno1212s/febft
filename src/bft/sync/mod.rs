//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::time::{Instant, Duration};

//use either::{
//    Left,
//    Right,
//};

use crate::bft::crypto::hash::Digest;
use crate::bft::core::server::ViewInfo;
use crate::bft::communication::{
    Node,
    NodeId
};
use crate::bft::ordering::{
    SeqNo,
    Orderable,
    tbo_pop_message,
    tbo_queue_message,
    tbo_advance_message_queue,
};
use crate::bft::log::{
    Log,
    StoredMessage,
};
use crate::bft::timeouts::{
    TimeoutKind,
    TimeoutsHandle,
};
use crate::bft::collections::{
    self,
    HashMap,
};
use crate::bft::communication::message::{
    Header,
    SystemMessage,
    RequestMessage,
    ViewChangeMessage,
    ViewChangeMessageKind,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

/// Represents a queue of view change messages that arrive out of context into a node.
pub struct TboQueue<O> {
    // the current view
    view: ViewInfo,
    // probe messages from this queue instead of
    // fetching them from the network
    get_queue: bool,
    // stores all STOP messages for the next view
    stop: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
    // stores all STOP-DATA messages for the next view
    stop_data: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
    // stores all SYNC messages for the next view
    sync: VecDeque<VecDeque<StoredMessage<ViewChangeMessage<O>>>>,
}

impl<O> TboQueue<O> {
    fn new(view: ViewInfo) -> Self {
        Self {
            view,
            get_queue: false,
            stop: VecDeque::new(),
            stop_data: VecDeque::new(),
            sync: VecDeque::new(),
        }
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// view change messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    fn next_instance_queue(&mut self) {
        self.view = self.view.next_view();
        tbo_advance_message_queue(&mut self.stop);
        tbo_advance_message_queue(&mut self.stop_data);
        tbo_advance_message_queue(&mut self.sync);
    }

    /// Queues a view change message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    pub fn queue(&mut self, h: Header, m: ViewChangeMessage<O>) {
        match m.kind() {
            ViewChangeMessageKind::Stop(_) => self.queue_stop(h, m),
            ViewChangeMessageKind::StopData(_) => self.queue_stop_data(h, m),
            ViewChangeMessageKind::Sync(_) => self.queue_sync(h, m),
        }
    }

    /// Queues a `STOP` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop(&mut self, h: Header, m: ViewChangeMessage<O>) {
        // NOTE: we use next() because we want to retrieve messages
        // for v+1, as we haven't started installing the new view yet
        let seq = self.view.sequence_number().next();
        tbo_queue_message(seq, &mut self.stop, StoredMessage::new(h, m))
    }

    /// Queues a `STOP-DATA` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_stop_data(&mut self, h: Header, m: ViewChangeMessage<O>) {
        let seq = self.view.sequence_number();
        tbo_queue_message(seq, &mut self.stop_data, StoredMessage::new(h, m))
    }

    /// Queues a `SYNC` message for later processing, or drops it
    /// immediately if it pertains to an older view change instance.
    fn queue_sync(&mut self, h: Header, m: ViewChangeMessage<O>) {
        let seq = self.view.sequence_number();
        tbo_queue_message(seq, &mut self.sync, StoredMessage::new(h, m))
    }
}

enum TimeoutPhase {
    // we have never received a timeout
    Init(Instant),
    // we received a second timeout for the same request;
    // start view change protocol
    TimedOutOnce(Instant),
}

enum ProtoPhase {
    // the view change protocol isn't running;
    // we are watching pending client requests for
    // any potential timeouts
    Init,
    // we are running the stopping phase of the
    // Mod-SMaRt protocol
    Stopping(usize),
    // we are still running the stopping phase of
    // Mod-SMaRt, but we have either locally triggered
    // a view change, or received at least f+1 STOP msgs,
    // so we don't need to broadcast a new STOP
    Stopping2(usize),
    // we are running the STOP-DATA phase of Mod-SMaRt
    StoppingData(usize),
    // we are running the SYNC phase of Mod-SMaRt
    Syncing(usize),
}

// TODO: finish statuses returned from `process_message`
pub enum SynchronizerStatus {
    /// We are not running the view change protocol.
    Nil,
    /// The view change protocol is currently running.
    Running,
    /// We installed a new view, resulted from running the
    /// view change protocol.
    NewView(ViewInfo),
    /// The following set of client requests timed out.
    ///
    /// We need to invoke the leader change protocol if
    /// we have a non empty set of stopped messages.
    RequestsTimedOut { forwarded: Vec<Digest>, stopped: Vec<Digest> },
}

/// Represents the status of calling `poll()` on a `Synchronizer`.
pub enum SynchronizerPollStatus<O> {
    /// The `Replica` associated with this `Synchronizer` should
    /// poll its main channel for more messages.
    Recv,
    /// A new view change message is available to be processed.
    NextMessage(Header, ViewChangeMessage<O>),
}

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages?
pub struct Synchronizer<S: Service> {
    watching_timeouts: bool,
    phase: ProtoPhase,
    timeout_seq: SeqNo,
    timeout_dur: Duration,
    watching: HashMap<Digest, TimeoutPhase>,
    tbo: TboQueue<Request<S>>,
    // NOTE: remembers whose replies we have
    // received already, to avoid replays
    //voted: HashSet<NodeId>,
}

macro_rules! extract_msg {
    ($t:ty => $g:expr, $q:expr) => {
        extract_msg!($t => {}, $g, $q)
    };

    ($t:ty => $opt:block, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ViewChangeMessage<O>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            ConsensusPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            ConsensusPollStatus::Recv
        }
    };
}

impl<S> Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + Clone + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    pub fn new(timeout_dur: Duration, view: ViewInfo) -> Self {
        Self {
            timeout_dur,
            phase: ProtoPhase::Init,
            watching_timeouts: false,
            timeout_seq: SeqNo::from(0),
            watching: collections::hash_map(),
            tbo: TboQueue::new(view),
        }
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(
        &mut self,
        digest: Digest,
        timeouts: &TimeoutsHandle<S>,
    ) {
        if !self.watching_timeouts {
            let seq = self.next_timeout();
            timeouts.timeout(self.timeout_dur, TimeoutKind::ClientRequests(seq));
            self.watching_timeouts = true;
        }

        self.watching
            .entry(digest)
            .and_modify(|phase| *phase = TimeoutPhase::TimedOutOnce(Instant::now()))
            .or_insert_with(|| TimeoutPhase::Init(Instant::now()));
    }

    /// Remove a client request with digest `digest` from the watched list
    /// of requests.
    pub fn unwatch_request(&mut self, digest: &Digest) {
        self.watching.remove(digest);
        self.watching_timeouts = !self.watching.is_empty();
    }

    /// Stop watching all pending client requests.
    pub fn unwatch_all(&mut self) {
        // since we will be on a different seq no,
        // the time out will do nothing
        self.next_timeout();
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    pub fn install_view(&mut self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        //self.phase = ProtoPhase::Init;
        self.tbo.view = view;
    }

    /// Advances the state of the view change state machine.
    pub fn process_message(
        &mut self,
        header: Header,
        message: ViewChangeMessage<O>,
        _log: &mut Log<State<S>, Request<S>, Reply<S>>,
        _node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        match self.phase {
            ProtoPhase::Init => {
                match message.kind() {
                    ViewChangeMessageKind::Stop(_) => self.queue_stop(header, message),
                    ViewChangeMessageKind::StopData(_) => self.queue_stop_data(header, message),
                    ViewChangeMessageKind::Sync(_) => self.queue_sync(header, message),
                }
                SynchronizerStatus::
            },
        }
    }

    /// Handle a timeout received from the timeouts layer.
    ///
    /// This timeout pertains to a group of client requests awaiting to be decided.
    pub fn client_requests_timed_out(&mut self, seq: SeqNo) -> SynchronizerStatus {
        let ignore_timeout = !self.watching_timeouts
            //
            // FIXME: maybe we should continue even after we have
            // already stopped, since we may need to forward new requests...
            //
            // tl;dr remove the `|| self.sent_stop()` line
            //
            || self.sent_stop()
            || seq.next() != self.timeout_seq;

        if ignore_timeout {
            return SynchronizerStatus::Nil;
        }

        // iterate over list of watched pending requests,
        // and select the ones to be stopped or forwarded
        // to peer nodes
        let mut forwarded = Vec::new();
        let mut stopped = Vec::new();
        let now = Instant::now();

        for (digest, timeout_phase) in self.watching.iter_mut() {
            // NOTE:
            // =====================================================
            // - on the first timeout we forward pending requests to
            //   the leader
            // - on the second timeout, we start a view change by
            //   broadcasting a STOP message
            match timeout_phase {
                TimeoutPhase::Init(i) if now.duration_since(*i) > self.timeout_dur => {
                    forwarded.push(digest.clone());
                    *timeout_phase = TimeoutPhase::TimedOutOnce(now);
                },
                TimeoutPhase::TimedOutOnce(i) if now.duration_since(*i) > self.timeout_dur => {
                    stopped.push(digest.clone());
                },
                _ => (),
            }
        }

        for digest in &stopped {
            // remove stopped requests
            self.watching.remove(digest);
        }

        // TODO: include stopped messages we have received in the forwarded messages;
        // if we add the stopped messages into our queue of requests as soon as we
        // receive them, this should be done automagically
        SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Trigger a view change locally.
    pub fn begin_view_change(
        &mut self,
        requests: Vec<StoredMessage<RequestMessage<Request<S>>>>,
        node: &mut Node<S::Data>,
    ) {
        match self.phase {
            // we have timed out, and sent a stop msg
            ProtoPhase::Init => self.phase = ProtoPhase::Stopping2(0),
            // we have received stop messages from peer nodes,
            // but haven't sent our own stop
            ProtoPhase::Stopping(n) => self.phase = ProtoPhase::Stopping2(n),
            // we are already running the view change proto, and sent a stop
            _ => return,
        }

        let message = SystemMessage::ViewChange(ViewChangeMessage::new(
            self.view().sequence_number().next(),
            ViewChangeMessageKind::Stop(requests),
        ));
        let targets = NodeId::targets(0..self.view().params().n());
        node.broadcast(message, targets);
    }

    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    pub fn view(&self) -> &ViewInfo {
        &self.tbo.view
    }

    fn next_timeout(&mut self) -> SeqNo {
        let next = self.timeout_seq;
        self.timeout_seq = self.timeout_seq.next();
        next
    }

    fn sent_stop(&self) -> bool {
        match self.phase {
            ProtoPhase::Init | ProtoPhase::Stopping(_) => false,
            _ => true,
        }
    }
}

impl<S> Deref for Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    type Target = TboQueue<Request<S>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tbo
    }
}

impl<S> DerefMut for Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tbo
    }
}
