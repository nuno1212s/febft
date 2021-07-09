//! Implements the synchronization phase from the Mod-SMaRt protocol.
//!
//! This code allows a replica to change its view, where a new
//! leader is elected.

enum ProtoPhase {
    Init,
    Stopping,
    StoppingData,
    Syncing,
}

pub enum SynchronizerStatus {
    // TODO: statuses returned from `process_message`
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
