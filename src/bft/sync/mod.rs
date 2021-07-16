//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::time::{Instant, Duration};

use crate::bft::ordering::SeqNo;
use crate::bft::communication::Node;
use crate::bft::crypto::hash::Digest;
use crate::bft::core::server::ViewInfo;
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
    ViewChangeMessage,
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

struct TboQueue<O> {
    // the current view
    view: ViewInfo,
    // probe messages from this queue instead of
    // fetching them from the network
    get_queue: bool,
    // stores all STOP messages for the next view
    stop: Vec<StoredMessage<ViewChangeMessage<O>>>,
    // stores all STOP-DATA messages for the next view
    stop_data: Vec<StoredMessage<ViewChangeMessage<O>>>,
    // stores all SYNC messages for the next view
    sync: Vec<StoredMessage<ViewChangeMessage<O>>>,
}

impl<O> TboQueue<O> {
    fn new(view: ViewInfo) -> Self {
        Self {
            view,
            get_queue: false,
            stop: Vec::new(),
            stop_data: Vec::new(),
            sync: Vec::new(),
        }
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
    // so we don't need to broadcast a new STOP msgs
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

impl<S> Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
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
        _header: Header,
        _message: () /*ViewChangeMessage*/,
        _log: &mut Log<State<S>, Request<S>, Reply<S>>,
        _node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        unimplemented!()
    }

    /// Handle a timeout received from the timeouts layer.
    ///
    /// This timeout pertains to a group of client requests awaiting to be decided.
    pub fn client_requests_timed_out(&mut self, seq: SeqNo) -> SynchronizerStatus {
        let ignore_timeout = seq.next() != self.timeout_seq
            || self.watching.is_empty()
            || self.running_view_change();

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

        SynchronizerStatus::RequestsTimedOut { forwarded, stopped }
    }

    /// Trigger a view change locally.
    pub fn begin_view_change(
        &mut self,
        _requests: (),
        _node: &mut Node<S::Data>,
    ) {
        if self.running_view_change() {
            return;
        }
        self.phase = ProtoPhase::Stopping2(0);

        // TODO: send STOP msgs
        unimplemented!()
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

    fn running_view_change(&self) -> bool {
        match self.phase {
            ProtoPhase::Init
                | ProtoPhase::Stopping(_)
                | ProtoPhase::Stopping2(_) => false,
            _ => true,
        }
    }
}
