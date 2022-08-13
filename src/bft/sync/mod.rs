use std::{time::Instant, cmp::Ordering, collections::VecDeque};

use super::{executable::{Service, Request, State, Reply}, core::server::ViewInfo, communication::{message::{ViewChangeMessage, Header, StoredMessage, ViewChangeMessageKind, FwdConsensusMessage, WireMessage}, Node}, crypto::hash::Digest, ordering::{tbo_queue_message, tbo_advance_message_queue, SeqNo, Orderable}, consensus::log::{CollectData, ViewDecisionPair, Proof}, collections};

pub mod replica_sync;
pub mod follower_sync;

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

//use either::{
//    Left,
//    Right,
//};

/// Contains the `COLLECT` structures the leader received in the `STOP-DATA` phase
/// of the view change protocol, as well as a value to be proposed in the `SYNC` message.
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
#[derive(Clone)]
pub struct LeaderCollects<O> {
    //fwd_propose: FwdConsensusMessage<O>,
    //proposed: Vec<StoredMessage<RequestMessage<O>>>,
    proposed: FwdConsensusMessage<O>,
    collects: Vec<StoredMessage<ViewChangeMessage<O>>>,
}

impl<O> LeaderCollects<O> {
    /// Gives up ownership of the inner values of this `LeaderCollects`.
    pub fn into_inner(self) -> (FwdConsensusMessage<O>, Vec<StoredMessage<ViewChangeMessage<O>>>) {
        (self.proposed, self.collects)
    }
}

pub(super) struct FinalizeState<O> {
    curr_cid: SeqNo,
    sound: Sound,
    proposed: FwdConsensusMessage<O>,
}


pub(super) enum FinalizeStatus<O> {
    NoValue,
    RunCst(FinalizeState<O>),
    Commit(FinalizeState<O>),
}

pub(self) enum Sound {
    Unbound(bool),
    Bound(Digest),
}

impl Sound {
    fn value(&self) -> Option<&Digest> {
        match self {
            Sound::Bound(d) => Some(d),
            _ => None,
        }
    }

    fn test(&self) -> bool {
        match self {
            Sound::Unbound(ok) => *ok,
            _ => true,
        }
    }
}

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

    pub fn install_view(&mut self, view: ViewInfo) {
        self.view = view;
    }

    /// Signal this `TboQueue` that it may be able to extract new
    /// view change messages from its internal storage.
    pub fn signal(&mut self) {
        self.get_queue = true;
    }

    fn next_instance_queue(&mut self) {
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

    /// Verifies if we have new `STOP` messages to be processed for
    /// the next view.
    pub fn can_process_stops(&self) -> bool {
        self.stop
            .get(0)
            .map(|deque| deque.len() > 0)
            .unwrap_or(false)
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

#[derive(Copy, Clone)]
pub(super) enum TimeoutPhase {
    // we have never received a timeout
    Init(Instant),
    // we received a second timeout for the same request;
    // start view change protocol
    TimedOutOnce(Instant),
    // keep requests that timed out stored in memory,
    // for efficienty
    TimedOut,
}

pub(super) enum ProtoPhase {
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
    // we are running the STOP-DATA phase of Mod-SMaRt
    StoppingData(usize),
    // we are running the SYNC phase of Mod-SMaRt
    Syncing,
    // we are running the SYNC phase of Mod-SMaRt,
    // but are paused while waiting for the state
    // transfer protocol to finish
    SyncingState,
}

// TODO: finish statuses returned from `process_message`
pub enum SynchronizerStatus {
    /// We are not running the view change protocol.
    Nil,
    /// The view change protocol is currently running.
    Running,
    /// The view change protocol just finished running.
    NewView,
    /// Before we finish the view change protocol, we need
    /// to run the CST protocol.
    RunCst,
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
    /// We need to resume the view change protocol, after
    /// running the CST protocol.
    ResumeViewChange,
}

///A trait describing some of the necessary methods for the synchronizer
pub trait AbstractSynchronizer<S: Service + 'static> {

    /// Returns information regarding the current view, such as
    /// the number of faulty replicas the system can tolerate.
    fn view(&self) -> ViewInfo;

    /// Install a new view received from the CST protocol, or from
    /// running the view change protocol.
    fn install_view(&self, view: ViewInfo);

    fn queue(&self, header: Header, message: ViewChangeMessage<Request<S>>);

}


////////////////////////////////////////////////////////////////////////////////
//
// NOTE: the predicates below were taken from
// Cachin's 'Yet Another Visit to Paxos' (April 2011), pages 10-11
//
// in this consensus algorithm, the WRITE phase is equivalent to the
// PREPARE phase of PBFT, so we will observe a mismatch of terminology
//
// in the arguments of the predicates below, 'ts' means 'timestamp',
// and it is equivalent to the sequence number of a view
//
////////////////////////////////////////////////////////////////////////////////

pub(super) fn sound<'a, O>(
    curr_view: ViewInfo,
    normalized_collects: &[Option<&'a CollectData<O>>],
) -> Sound {
    // collect timestamps and values
    let mut timestamps = collections::hash_set();
    let mut values = collections::hash_set();

    for maybe_collect in normalized_collects.iter() {
        // NOTE: BFT-SMaRt assumes normalized values start on view 0,
        // if their CID is different from the one in execution;
        // see `LCManager::normalizeCollects` on its code
        let c = match maybe_collect {
            Some(c) => c,
            None => {
                timestamps.insert(SeqNo::ZERO);
                continue;
            }
        };

        // add quorum write timestamp
        timestamps.insert(c
            .incomplete_proof()
            .quorum_writes()
            .map(|ViewDecisionPair(ts, _)| *ts)
            .unwrap_or(SeqNo::ZERO));

        // add writeset timestamps and values
        for ViewDecisionPair(ts, value) in c.incomplete_proof().write_set().iter() {
            timestamps.insert(*ts);
            values.insert(value);
        }
    }

    for ts in timestamps {
        for value in values.iter() {
            if binds(curr_view, ts, value, normalized_collects) {
                return Sound::Bound(**value);
            }
        }
    }

    Sound::Unbound(unbound(curr_view, normalized_collects))
}

pub(super) fn binds<O>(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        false
    } else {
        quorum_highest(curr_view, ts, value, normalized_collects)
            && certified_value(curr_view, ts, value, normalized_collects)
    }
}

pub(super) fn unbound<O>(
    curr_view: ViewInfo,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    if normalized_collects.len() < curr_view.params().quorum() {
        false
    } else {
        let count = normalized_collects
            .iter()
            .filter(move |maybe_collect| {
                maybe_collect
                    .map(|collect| {
                        collect
                            .incomplete_proof()
                            .quorum_writes()
                            .map(|ViewDecisionPair(other_ts, _)| {
                                *other_ts == SeqNo::ZERO
                            })
                            // when there is no quorum write, BFT-SMaRt
                            // assumes replicas are on view 0
                            .unwrap_or(true)
                    })
                    // check NOTE above on the `sound` predicate
                    .unwrap_or(true)
            })
            .count();
        count >= curr_view.params().quorum()
    }
}

// NOTE: `filter_map` on the predicates below filters out
// collects whose cid was different from the one in execution;
//
// in BFT-SMaRt's code, a `TimestampValuePair` is generated in
// `LCManager::normalizeCollects`, containing an empty (zero sized
// byte array) digest, which will always evaluate to false when
// comparing its equality to other digests from collects whose
// cid is the same as the one in execution;
//
// therefore, our code *should* be correct :)

pub(super) fn quorum_highest<O>(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    let appears = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .position(|collect| {
            collect
                .incomplete_proof()
                .quorum_writes()
                .map(|ViewDecisionPair(other_ts, other_value)| {
                    *other_ts == ts && other_value == value
                })
                .unwrap_or(false)
        })
        .is_some();
    let count = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .filter(move |collect| {
            collect
                .incomplete_proof()
                .quorum_writes()
                .map(|ViewDecisionPair(other_ts, other_value)| {
                    match other_ts.cmp(&ts) {
                        Ordering::Less => true,
                        Ordering::Equal if other_value == value => true,
                        _ => false,
                    }
                })
                .unwrap_or(false)
        })
        .count();
    appears && count >= curr_view.params().quorum()
}

pub(super) fn certified_value<O>(
    curr_view: ViewInfo,
    ts: SeqNo,
    value: &Digest,
    normalized_collects: &[Option<&CollectData<O>>],
) -> bool {
    let count: usize = normalized_collects
        .iter()
        .filter_map(Option::as_ref)
        .map(move |collect| {
            collect
                .incomplete_proof()
                .write_set()
                .iter()
                .filter(|ViewDecisionPair(other_ts, other_value)| {
                    *other_ts >= ts && other_value == value
                })
                .count()
        })
        .sum();
    count > curr_view.params().f()
}

pub(super) fn collect_data<'a, O: 'a>(
    collects: impl Iterator<Item=&'a StoredMessage<ViewChangeMessage<O>>>,
) -> impl Iterator<Item=&'a CollectData<O>> {
    collects
        .filter_map(|stored| {
            match stored.message().kind() {
                ViewChangeMessageKind::StopData(collects) => Some(collects),
                _ => None,
            }
        })
}

pub(super) fn normalized_collects<'a, O: 'a>(
    in_exec: SeqNo,
    collects: impl Iterator<Item=&'a CollectData<O>>,
) -> impl Iterator<Item=Option<&'a CollectData<O>>> {
    collects
        .map(move |collect| {
            if collect.incomplete_proof().executing() == in_exec {
                Some(collect)
            } else {
                None
            }
        })
}

pub(super) fn signed_collects<S>(
    node: &Node<S::Data>,
    collects: Vec<StoredMessage<ViewChangeMessage<Request<S>>>>,
) -> Vec<StoredMessage<ViewChangeMessage<Request<S>>>>
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    collects
        .into_iter()
        .filter(|stored| validate_signature::<S, _>(node, stored))
        .collect()
}

pub(super) fn validate_signature<'a, S, M>(
    node: &'a Node<S::Data>,
    stored: &'a StoredMessage<M>,
) -> bool
    where
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    let wm = match WireMessage::from_parts(*stored.header(), vec![]) {
        Ok(wm) => wm,
        _ => return false,
    };
    // check if we even have the public key of the node that claims
    // to have sent this particular message
    let key = match node.get_public_key(stored.header().from()) {
        Some(k) => k,
        None => return false,
    };
    wm.is_valid(Some(key))
}

pub(super) fn highest_proof<'a, S, I>(
    view: ViewInfo,
    node: &Node<S::Data>,
    collects: I,
) -> Option<&'a Proof<Request<S>>>
    where
        I: Iterator<Item=&'a StoredMessage<ViewChangeMessage<Request<S>>>>,
        S: Service + Send + 'static,
        State<S>: Send + Clone + 'static,
        Request<S>: Send + Clone + 'static,
        Reply<S>: Send + 'static,
{
    collect_data(collects)
        // fetch proofs
        .filter_map(|collect| collect.last_proof())
        // check if COMMIT msgs are signed, and all have the same digest
        //
        // TODO: check proofs and digests of PREPAREs as well, eventually,
        // but for now we are replicating the behavior of BFT-SMaRt
        .filter(move |proof| {
            let digest = proof
                .pre_prepare()
                .header()
                .digest();

            proof
                .commits()
                .iter()
                .filter(|stored| {
                    stored
                        .message()
                        .has_proposed_digest(digest)
                        .unwrap_or(false)
                })
                .filter(move |&stored| validate_signature::<S, _>(node, stored))
                .count() >= view.params().quorum()
        })
        .max_by_key(|proof| {
            proof
                .pre_prepare()
                .message()
                .sequence_number()
        })
}