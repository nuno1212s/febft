//! User application execution business logic.

use crate::bft::error::*;
use crate::bft::communication::serialize::{
    Data,
    SharedData,
};

/// A user defined `Service`.
///
/// Application logic is implemented by this trait.
pub trait Service {
    /// The types used by the application.
    type Data: Data;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<<Self::Data as Data>::State>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn process(
        &mut self,
        state: &mut <Self::Data as Data>::State,
        request: <Self::Data as SharedData>::Request,
    ) -> Result<<Self::Data as SharedData>::Reply>;
}
