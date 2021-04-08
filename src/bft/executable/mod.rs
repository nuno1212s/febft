//! User application execution business logic.

use crate::bft::error::*;
use crate::bft::communication::serialize::{
    Marshal,
    Unmarshal,
};

/// A user defined `Service`.
///
/// Application logic is implemented by this trait.
pub trait Service {
    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    type Request: Marshal<Self::Reply> + Unmarshal<Self::Request>;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    type Reply: Marshal<Self::Reply> + Unmarshal<Self::Request>;

    /// The application state, which is mutated by client
    /// requests.
    type State;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<Self::State>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn process(&mut self, state: &mut Self::State, request: Self::Request) -> Result<Self::Reply>;
}

/*
FIXME:

pub struct Replica<S: Service> {
    node: Node<S::Request, S::Reply>,
}

*/
