#[cfg(feature = "serialize_serde_bincode")]
pub mod bincode;

#[cfg(feature = "serialize_serde_messagepack")]
pub mod messagepack;
