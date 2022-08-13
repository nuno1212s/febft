use crate::bft::communication::message::{
    ConsensusMessageKind, Header, RequestMessage, StoredMessage, ViewChangeMessage,
    ViewChangeMessageKind,
};
use crate::bft::communication::Node;
use crate::bft::consensus::follower_consensus::ConsensusFollower;
use crate::bft::consensus::log::{CollectData, MemLog, Proof};
use crate::bft::core::server::ViewInfo;
use crate::bft::executable::{Reply, Request, Service, State};
use crate::bft::ordering::{tbo_pop_message, Orderable, SeqNo};
use crate::bft::sync::{
    FinalizeState, FinalizeStatus, ProtoPhase, SynchronizerPollStatus, TboQueue,
};
use intmap::IntMap;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use super::{collect_data, highest_proof, normalized_collects, signed_collects, sound, AbstractSynchronizer};

macro_rules! extract_msg {
    ($t:ty => $g:expr, $q:expr) => {
        extract_msg!($t => {}, $g, $q)
    };

    ($t:ty => $opt:block, $g:expr, $q:expr) => {
        if let Some(stored) = tbo_pop_message::<ViewChangeMessage<$t>>($q) {
            $opt
            let (header, message) = stored.into_inner();
            SynchronizerPollStatus::NextMessage(header, message)
        } else {
            *$g = false;
            SynchronizerPollStatus::Recv
        }
    };
}

macro_rules! stop_status {
    ($self:expr, $i:expr) => {{
        let f = $self.view().params().f();
        if $i > f {
            FollowerSynchronizerStatus::Running
        } else {
            FollowerSynchronizerStatus::Nil
        }
    }};
}

macro_rules! finalize_view_change {
    (
        $self:expr,
        $state:expr,
        $proof:expr,
        $normalized_collects:expr,
        $log:expr,
        $consensus:expr,
        $node:expr $(,)?
    ) => {{
        match $self.pre_finalize($state, $proof, $normalized_collects, $log) {
            // wait for next timeout
            FinalizeStatus::NoValue => FollowerSynchronizerStatus::Running,
            // we need to run cst before proceeding with view change
            FinalizeStatus::RunCst(state) => {
                $self.finalize_state.replace(Some(state));
                $self.phase.replace(ProtoPhase::SyncingState);
                FollowerSynchronizerStatus::RunCst
            }
            // we may finish the view change proto
            FinalizeStatus::Commit(state) => $self.finalize(state, $log, $consensus, $node),
        }
    }};
}

pub enum FollowerProtoPhase {
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
    // so we don't need to broadcast a new STOP;
    // this is effectively an implementation detail,
    // and not a real phase of Mod-SMaRt!
    Stopping2(usize),
    // we are running the SYNC phase of Mod-SMaRt
    Syncing,
    // we are running the SYNC phase of Mod-SMaRt,
    // but are paused while waiting for the state
    // transfer protocol to finish
    SyncingState,
}

pub enum FollowerSynchronizerStatus {
    /// We are not running the view change protocol.
    Nil,
    /// The view change protocol is currently running.
    Running,
    /// The view change protocol just finished running.
    NewView,
    /// Before we finish the view change protocol, we need
    /// to run the CST protocol.
    RunCst,
}

// TODO:
// - the fields in this struct
// - TboQueue for sync phase messages
// This synchronizer will only move forward on replica messages
// Does not actually partake in the quorum, just receives copies of the messages
// exchanged and follows along
pub struct FollowerSynchronizer<S: Service> {
    phase: RefCell<ProtoPhase>,
    stopped: RefCell<IntMap<Vec<StoredMessage<RequestMessage<Request<S>>>>>>,
    collects: Mutex<IntMap<StoredMessage<ViewChangeMessage<Request<S>>>>>,
    tbo: Mutex<TboQueue<Request<S>>>,
    finalize_state: RefCell<Option<FinalizeState<Request<S>>>>,
}

impl<S: Service + 'static> AbstractSynchronizer<S> for FollowerSynchronizer<S> {


    /// Returns some information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo {
        self.tbo.lock().unwrap().view.clone()
    }

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn install_view(&self, view: ViewInfo) {
        // FIXME: is the following line necessary?
        //self.phase = ProtoPhase::Init;
        let mut guard = self.tbo.lock().unwrap();

        guard.install_view(view);
    }

    fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>) {
        self.tbo.lock().unwrap().queue(header, message)
    }
}

impl<S: Service> FollowerSynchronizer<S> {
    pub fn new(view: ViewInfo) -> Arc<Self> {
        Arc::new(Self {
            phase: RefCell::new(ProtoPhase::Init),
            stopped: RefCell::new(Default::default()),
            collects: Mutex::new(Default::default()),
            tbo: Mutex::new(TboQueue::new(view)),
            finalize_state: RefCell::new(None),
        })
    }

    pub fn signal(&self) {
        self.tbo.lock().unwrap().signal()
    }
    pub fn can_process_stops(&self) -> bool {
        self.tbo.lock().unwrap().can_process_stops()
    }

    /// Check if we can process new view change messages.
    pub fn poll(&self) -> SynchronizerPollStatus<Request<S>> {
        let mut tbo_guard = self.tbo.lock().unwrap();
        match *self.phase.borrow() {
            _ if !tbo_guard.get_queue => SynchronizerPollStatus::Recv,
            ProtoPhase::Init => {
                extract_msg!(Request<S> =>
                    { self.phase.replace(ProtoPhase::Stopping(0)); },
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                )
            }
            ProtoPhase::Stopping(_) | ProtoPhase::Stopping2(_) => {
                extract_msg!(Request<S> =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop
                )
            }
            ProtoPhase::StoppingData(_) => {
                extract_msg!(Request<S> =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.stop_data
                )
            }
            ProtoPhase::Syncing => {
                extract_msg!(Request<S> =>
                    &mut tbo_guard.get_queue,
                    &mut tbo_guard.sync
                )
            }
            ProtoPhase::SyncingState => SynchronizerPollStatus::ResumeViewChange,
        }
    }

    /// Advances the state of the view change state machine.
    //
    // TODO: retransmit STOP msgs
    pub fn process_message(
        &self,
        header: Header,
        message: ViewChangeMessage<Request<S>>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        consensus: &mut ConsensusFollower<S>,
        node: &Node<S::Data>,
    ) -> FollowerSynchronizerStatus {
        match *self.phase.borrow() {
            ProtoPhase::Init => {
                match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return FollowerSynchronizerStatus::Nil;
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        //Ignore stop data messages

                        return FollowerSynchronizerStatus::Nil;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return FollowerSynchronizerStatus::Nil;
                    }
                }
            }
            ProtoPhase::Stopping(i) | ProtoPhase::Stopping2(i) => {
                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let next_seq = current_view.sequence_number().next();

                let i = match message.kind() {
                    ViewChangeMessageKind::Stop(_) if msg_seq != next_seq => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return stop_status!(self, i);
                    }
                    ViewChangeMessageKind::Stop(_)
                        if self.stopped.borrow().contains_key(header.from().into()) =>
                    {
                        // drop attempts to vote twice
                        return stop_status!(self, i);
                    }
                    ViewChangeMessageKind::Stop(_) => i + 1,
                    ViewChangeMessageKind::StopData(_) => {
                        //Ignore stop data messages
                        return stop_status!(self, i);
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return stop_status!(self, i);
                    }
                };

                // store pending requests from this STOP
                let stopped = match message.into_kind() {
                    ViewChangeMessageKind::Stop(stopped) => stopped,
                    _ => unreachable!(),
                };

                self.stopped
                    .borrow_mut()
                    .insert(header.from().into(), stopped);

                // NOTE: we only take this branch of the code before
                // we have sent our own STOP message
                if let ProtoPhase::Stopping(_) = *self.phase.borrow() {
                    return if i > current_view.params().f() {
                        self.begin_view_change(None, node, log);
                        FollowerSynchronizerStatus::Running
                    } else {
                        self.phase.replace(ProtoPhase::Stopping(i));
                        FollowerSynchronizerStatus::Nil
                    };
                }

                if i == current_view.params().quorum() {
                    self.install_view(current_view.next_view());

                    self.phase.replace(ProtoPhase::Syncing);
                } else {
                    self.phase.replace(ProtoPhase::Stopping2(i));
                }

                FollowerSynchronizerStatus::Running
            }
            ProtoPhase::StoppingData(_) => {
                //Since a follower can never be a leader (as he isn't a part of the
                // quorum, he can never be in this state)
                unreachable!()
            }
            ProtoPhase::Syncing => {
                let msg_seq = message.sequence_number();
                let current_view = self.view();
                let seq = current_view.sequence_number();

                // reject SYNC messages if these were not sent by the leader
                let (proposed, collects) = match message.kind() {
                    ViewChangeMessageKind::Stop(_) => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_stop(header, message);

                        return FollowerSynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::StopData(_) => {
                        //Ignore stop data messages

                        return FollowerSynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) if msg_seq != seq => {
                        let mut guard = self.tbo.lock().unwrap();

                        guard.queue_sync(header, message);

                        return FollowerSynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) if header.from() != current_view.leader() => {
                        return FollowerSynchronizerStatus::Running;
                    }
                    ViewChangeMessageKind::Sync(_) => {
                        let mut message = message;

                        message.take_collects().unwrap().into_inner()
                    }
                };

                // leader has already performed this computation in the
                // STOP-DATA phase of Mod-SMaRt
                let signed: Vec<_> = signed_collects::<S>(node, collects);

                let proof = highest_proof::<S, _>(current_view, node, signed.iter());

                let curr_cid = proof
                    .map(|p| p.pre_prepare().message().sequence_number())
                    .map(|seq| SeqNo::from(u32::from(seq) + 1))
                    .unwrap_or(SeqNo::ZERO);

                let normalized_collects: Vec<_> =
                    { normalized_collects(curr_cid, collect_data(signed.iter())).collect() };

                let sound = sound(current_view, &normalized_collects);

                if !sound.test() {
                    // FIXME: BFT-SMaRt doesn't do anything if `sound`
                    // evaluates to false; do we keep the same behavior,
                    // and wait for a new time out? but then, no other
                    // consensus messages have been processed... this
                    // may be a point of contention on the lib!
                    return FollowerSynchronizerStatus::Running;
                }

                let state = FinalizeState {
                    curr_cid,
                    sound,
                    proposed,
                };

                finalize_view_change!(
                    self,
                    state,
                    proof,
                    normalized_collects,
                    log,
                    consensus,
                    node,
                )
            }

            // handled by `resume_view_change()`
            ProtoPhase::SyncingState => unreachable!(),
        }
    }


    /// Trigger a view change locally.
    ///
    /// The value `timed_out` corresponds to a list of client requests
    /// that have timed out on the current replica.
    pub fn begin_view_change(
        &self,
        timed_out: Option<Vec<StoredMessage<RequestMessage<Request<S>>>>>,
        node: &Node<S::Data>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) {
        match (&*self.phase.borrow(), &timed_out) {
            // we have received STOP messages from peer nodes,
            // but haven't sent our own STOP, yet; (And in the case of followers we will never send it)
            //
            // when `timed_out` is `None`, we were called from `process_message`,
            // so we need to update our phase with a new received message
            (ProtoPhase::Stopping(i), None) => {
                self.phase.replace(ProtoPhase::Stopping2(*i + 1));
            }
            (ProtoPhase::Stopping(i), _) => {
                self.phase.replace(ProtoPhase::Stopping2(*i));
            }
            // we have timed out, therefore we should send a STOP msg;
            //
            // note that we might have already been running the view change proto,
            // and started another view because we timed out again (e.g. because of
            // a faulty leader during the view change)
            _ => {
                // clear state from previous views
                self.stopped.borrow_mut().clear();
                self.collects.lock().unwrap().clear();
                self.phase.replace(ProtoPhase::Stopping2(0));
            }
        };
    }

    // this function mostly serves the purpose of consuming
    // values with immutable references, to allow borrowing data mutably
    fn pre_finalize(
        &self,
        state: FinalizeState<Request<S>>,
        _proof: Option<&Proof<Request<S>>>,
        _normalized_collects: Vec<Option<&CollectData<Request<S>>>>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
    ) -> FinalizeStatus<Request<S>> {
        if let ProtoPhase::Syncing = *self.phase.borrow() {
            //
            // NOTE: this code will not run when we resume
            // the view change protocol after running CST
            //
            if log.decision_log().borrow().executing() != state.curr_cid {
                return FinalizeStatus::RunCst(state);
            }
        }

        let rqs = match state.proposed.consensus().kind() {
            ConsensusMessageKind::PrePrepare(rqs) => rqs,
            _ => {
                panic!("Can only have pre prepare messages");
            }
        };

        if rqs.is_empty() && !state.sound.test() {
            return FinalizeStatus::NoValue;
        }

        FinalizeStatus::Commit(state)
    }

    fn finalize(
        &self,
        state: FinalizeState<Request<S>>,
        log: &MemLog<State<S>, Request<S>, Reply<S>>,
        consensus: &mut ConsensusFollower<S>,
        node: &Node<S::Data>,
    ) -> FollowerSynchronizerStatus {
        let FinalizeState {
            curr_cid,
            proposed,
            sound,
        } = state;

        // we will get some value to be proposed because of the
        // check we did in `pre_finalize()`, guarding against no values
        let (header, message) = log
            .decision_log()
            .borrow_mut()
            .clear_last_occurrences(curr_cid, sound.value())
            .and_then(|stored| Some(stored.into_inner()))
            .unwrap_or(proposed.into_inner());

        // finalize view change by broadcasting a PREPARE msg
        consensus.finalize_view_change((header, message), self, log, node);

        // skip queued messages from the current view change
        // and update proto phase
        self.tbo.lock().unwrap().next_instance_queue();
        self.phase.replace(ProtoPhase::Init);

        // resume normal phase
        FollowerSynchronizerStatus::NewView
    }
}
