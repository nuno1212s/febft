use std::fmt::{Debug, Formatter};
use febft_timeouts::Timeout;
use febft_common::ordering::SeqNo;
use febft_communication::message::{Header, NetworkMessage};
use febft_communication::serialize::Serializable;
use febft_common::error::*;
use febft_messages::messages::SystemMessage;

/// The `Message` type encompasses all the messages traded between different
/// asynchronous tasks in the system.
pub type Teste = ();
/*pub enum Message<T, S> where T: Serializable {
    /// A network message
    NetworkMessage(NetworkMessage<T>),
    /// Same as `Message::ExecutionFinished`, but includes a snapshot of
    /// the application state.
    ///
    /// This is useful for local checkpoints.
    ExecutionFinishedWithAppstate((SeqNo, S)),
    /// We received a timeout from the timeouts layer.
    Timeout(Timeout),
}

impl<T, S> Debug for Message<T, S> where T: Serializable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::NetworkMessage(header) => {
                write!(f, "System message")
            }
            Message::ExecutionFinishedWithAppstate(_) => {
                write!(f, "Execution finished")
            }
            Message::Timeout(_) => {
                write!(f, "timeout")
            }
            _ => {}
        }
    }
}


impl<T: Serializable, S> Message<T, S> {
    /// Returns the `Header` of this message, if it is
    /// a `SystemMessage`.
    pub fn header(&self) -> Result<&Header> {
        match self {
            Message::NetworkMessage(ref h) =>
                Ok(h),
            Message::ExecutionFinishedWithAppstate(_) =>
                Err("Expected System found ExecutionFinishedWithAppstate")
                    .wrapped(ErrorKind::CommunicationMessage),
            Message::Timeout(_) =>
                Err("Expected System found Timeout")
                    .wrapped(ErrorKind::CommunicationMessage),
        }
    }
}*/