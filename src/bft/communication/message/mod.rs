#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ReplicaMessage {
    Dummy(Vec<u8>),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub enum ClientMessage {
    Dummy(Vec<u8>),
}
