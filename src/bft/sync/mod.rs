//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

use crate::bft::core::server::ViewInfo;

enum ProtoPhase {
    Init,
    Stopping,
    StoppingData,
    Syncing,
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
pub struct Synchronizer;

impl Synchronizer {
/*
    /// Advances the state of the view change state machine.
    pub fn process_message(
        &mut self,
        header: Header,
        message: ViewChangeMessage,
        view: ViewInfo,
        log: &mut Log<State<S>, Request<S>, Reply<S>>,
        node: &mut Node<S::Data>,
    ) -> SynchronizerStatus {
        unimplemented!()
    }
*/

    /// Handle a timeout received from the timeouts layer.
    pub fn timed_out(&mut self) -> SynchronizerStatus {
        // TODO:
        // - on the first timeout we forward pending requests to
        //   the leader
        // - on the second timeout, we start a view change by
        //   broadcasting a STOP message
        unimplemented!()
    }
}
