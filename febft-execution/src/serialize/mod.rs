use std::io::{Read, Write};
#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serialize_bincode")]
use bincode::{Encode, Decode, BorrowDecode};

use febft_common::error::*;

/// Marker trait containing the types used by the application,
/// as well as routines to serialize the application data.
///
/// Both clients and replicas should implement this trait,
/// to communicate with each other.
/// This data type must be Send since it will be sent across
/// threads for processing and follow up reception
pub trait SharedData: Send {
    /// The application state, which is mutated by client
    /// requests.
    #[cfg(feature = "serialize_serde")]
    type State: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_bincode")]
    type State: for<'a> Encode + Decode + BorrowDecode<'a> + Send + Clone;

    /// Represents the requests forwarded to replicas by the
    /// clients of the BFT system.
    #[cfg(feature = "serialize_serde")]
    type Request: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_bincode")]
    type Request: for<'a> Encode + Decode + BorrowDecode<'a> + Send + Clone;

    /// Represents the replies forwarded to clients by replicas
    /// in the BFT system.
    #[cfg(feature = "serialize_serde")]
    type Reply: for<'a> Deserialize<'a> + Serialize + Send + Clone;

    #[cfg(feature = "serialize_bincode")]
    type Reply: for<'a> Encode + Decode + BorrowDecode<'a> + Send + Clone;


    ///Serialize a state so it can be utilized by the SMR middleware
    ///  (either for network sending or persistent storing)
    fn serialize_state<W>(w: W, state: &Self::State) -> Result<()> where W: Write;

    ///Deserialize a state generated by the serialize_state function.
    fn deserialize_state<R>(r: R) -> Result<Self::State> where R: Read;

    ///Serialize a request from your service, given the writer to serialize into
    fn serialize_request<W>(w: W, request: &Self::Request) -> Result<()> where W: Write;

    ///Deserialize a request that was generated by the serialize request function above
    fn deserialize_request<R>(r: R) -> Result<Self::Request> where R: Read;

    ///Serialize a reply into a given writer
    fn serialize_reply<W>(w: W, reply: &Self::Reply) -> Result<()> where W: Write;

    ///Deserialize a reply that was generated using the serialize reply function above
    fn deserialize_reply<R>(r: R) -> Result<Self::Reply> where R: Read;
}