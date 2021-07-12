//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use std::marker::PhantomData;

use crate::bft::log::Log;
use crate::bft::ordering::SeqNo;
use crate::bft::communication::Node;
use crate::bft::crypto::hash::Digest;
use crate::bft::core::server::ViewInfo;
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
};
use crate::bft::executable::{
    Service,
    Request,
    Reply,
    State,
};

enum TimeoutPhase {
    // we have never received a timeout
    Init,
    // we received a second timeout for the same request;
    // start view change protocol
    TimedOutOnce,
}

enum ProtoPhase {
    // nothing is happening, there are no client
    // requests to be ordered
    Init,
    // we are watching the timers of a batch
    // of client requests (not the same batch
    // used during consensus)
    WatchingTimeout,
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
}

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages?
pub struct Synchronizer<S: Service> {
    phase: ProtoPhase,
    timeout_seq: SeqNo,
    view: ViewInfo,
    watching: HashMap<Digest, TimeoutPhase>,
    _phantom: PhantomData<S>,
}

impl<S> Synchronizer<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    pub fn new(view: ViewInfo) -> Self {
        Self {
            view,
            _phantom: PhantomData,
            phase: ProtoPhase::Init,
            timeout_seq: SeqNo::from(0),
            watching: collections::hash_map(),
        }
    }

    /// Watch a client request with the digest `digest`.
    pub fn watch_request(
        &mut self,
        digest: Digest,
        timeouts: &TimeoutsHandle<S>,
    ) {
        if matches!(self.phase, ProtoPhase::Init) {
            self.phase = ProtoPhase::WatchingTimeout;
        }

        // TODO: watch request
        drop((digest, timeouts));
    }

    /// Remove a client request with digest `digest` from the watched list
    /// of requests.
    pub fn unwatch_request(
        &mut self,
        digest: Digest,
    ) {
        // TODO: unwatch request
        drop(digest);

        match self.phase {
            ProtoPhase::WatchingTimeout if self.watching.is_empty() => {
                self.phase = ProtoPhase::Init;
            },
            _ => (),
        }
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    pub fn install_view(&mut self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        //self.phase = ProtoPhase::Init;
        self.view = view;
    }

    /// Advances the state of the view change state machine.
    pub fn process_message(
        &mut self,
        header: Header,
        message: () /*ViewChangeMessage*/,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        unimplemented!()
    }

    /// Handle a timeout received from the timeouts layer.
    pub fn timed_out(&mut self) -> SynchronizerStatus {
        // TODO:
        // - on the first timeout we forward pending requests to
        //   the leader
        // - on the second timeout, we start a view change by
        //   broadcasting a STOP message
        unimplemented!()
    }

    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    pub fn view(&self) -> &ViewInfo {
        &self.view
    }
}
