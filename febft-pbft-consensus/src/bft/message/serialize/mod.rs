//! This module is responsible for serializing wire messages in `febft`.
//!
//! All relevant types transmitted over the wire are `serde` aware, if
//! this feature is enabled with `serialize_serde`. Slightly more exotic
//! serialization routines, for better throughput, can be utilized, such
//! as [Cap'n'Proto](https://capnproto.org/capnp-tool.html), but these are
//! expected to be implemented by the user.

use std::io::{Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;
use bytes::Bytes;
use atlas_common::error::*;

#[cfg(feature = "serialize_serde")]
use ::serde::{Deserialize, Serialize};
use atlas_common::crypto::hash::{Context, Digest};
use atlas_common::globals::ReadOnly;
use atlas_communication::message::StoredMessage;
use atlas_communication::Node;
use atlas_communication::serialize::Serializable;
use atlas_core::ordering_protocol::{ProtocolMessage, SerProof, SerProofMetadata};
use atlas_core::persistent_log::PersistableOrderProtocol;
use atlas_execution::app::Service;
use atlas_execution::serialize::SharedData;
use atlas_core::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage};
use atlas_core::state_transfer::DecLog;
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage};
use crate::bft::{PBFT};
use crate::bft::msg_log::decisions::{DecisionLog, Proof, ProofMetadata};
use crate::bft::sync::view::ViewInfo;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

#[cfg(feature = "serialize_serde")]
pub mod serde;

/// The buffer type used to serialize messages into.
pub type Buf = Bytes;

pub fn serialize_consensus<W, D>(w: &mut W, message: &ConsensusMessage<D::Request>) -> Result<()>
    where
        W: Write + AsRef<[u8]> + AsMut<[u8]>,
        D: SharedData,
{
    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_consensus::<W, D>(w, message)?;

    #[cfg(feature = "serialize_serde")]
    serde::serialize_consensus::<W, D>(message, w)?;

    Ok(())
}

pub fn deserialize_consensus<R, D>(r: R) -> Result<ConsensusMessage<D::Request>>
    where
        R: Read + AsRef<[u8]>,
        D: SharedData,
{
    #[cfg(feature = "serialize_capnp")]
        let result = capnp::deserialize_consensus::<R, D>(r)?;

    #[cfg(feature = "serialize_serde")]
        let result = serde::deserialize_consensus::<R, D>(r)?;

    Ok(result)
}

/// The serializable type, to be used to appease the compiler and it's requirements
pub struct PBFTConsensus<D: SharedData>(PhantomData<D>);

impl<D> OrderingProtocolMessage for PBFTConsensus<D> where D: SharedData {
    type ViewInfo = ViewInfo;
    type ProtocolMessage = PBFTMessage<D::Request>;
    type Proof = Proof<D::Request>;
    type ProofMetadata = ProofMetadata;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(builder: atlas_capnp::consensus_messages_capnp::protocol_message::Builder, msg: &Self::ProtocolMessage) -> Result<()> {
        capnp::serialize_message::<D>(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(reader: atlas_capnp::consensus_messages_capnp::protocol_message::Reader) -> Result<Self::ProtocolMessage> {
        capnp::deserialize_message::<D>(reader)
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_view_capnp(builder: atlas_capnp::cst_messages_capnp::view_info::Builder, msg: &Self::ViewInfo) -> Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_view_capnp(reader: atlas_capnp::cst_messages_capnp::view_info::Reader) -> Result<Self::ViewInfo> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_proof_capnp(builder: atlas_capnp::cst_messages_capnp::proof::Builder, msg: &Self::Proof) -> Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_proof_capnp(reader: atlas_capnp::cst_messages_capnp::proof::Reader) -> Result<Self::Proof> {
        todo!()
    }
}

impl<D> StatefulOrderProtocolMessage for PBFTConsensus<D> where D: SharedData {
    type DecLog = DecisionLog<D::Request>;

    #[cfg(feature = "serialize_capnp")]
    fn serialize_declog_capnp(builder: atlas_capnp::cst_messages_capnp::dec_log::Builder, msg: &Self::DecLog) -> Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_declog_capnp(reader: atlas_capnp::cst_messages_capnp::dec_log::Reader) -> Result<Self::DecLog> {
        todo!()
    }
}

const CF_PRE_PREPARES: &str = "PRE_PREPARES";
const CF_PREPARES: &str = "PREPARES";
const CF_COMMIT: &str = "COMMITS";


impl<D> PersistableOrderProtocol<Self, Self> for PBFTConsensus<D>
    where D: SharedData, {
    fn message_types() -> Vec<&'static str> {
        vec![
            CF_PRE_PREPARES,
            CF_PREPARES, CF_COMMIT,
        ]
    }

    fn get_type_for_message(msg: &ProtocolMessage<Self>) -> Result<&'static str> {
        match msg {
            PBFTMessage::Consensus(msg) => {
                match msg.kind() {
                    ConsensusMessageKind::PrePrepare(_) => {
                        Ok(CF_PRE_PREPARES)
                    }
                    ConsensusMessageKind::Prepare(_) => {
                        Ok(CF_PREPARES)
                    }
                    ConsensusMessageKind::Commit(_) => {
                        Ok(CF_COMMIT)
                    }
                }
            }
            _ => {
                Err(Error::simple_with_msg(ErrorKind::MsgLogPersistentSerialization, "Invalid message type"))
            }
        }
    }

    fn init_proof_from(metadata: SerProofMetadata<Self>, messages: Vec<StoredMessage<ProtocolMessage<Self>>>) -> SerProof<Self> {
        let mut pre_prepares = Vec::with_capacity(messages.len() / 2);
        let mut prepares = Vec::with_capacity(messages.len() / 2);
        let mut commits = Vec::with_capacity(messages.len() / 2);

        for message in messages {
            match message.message() {
                PBFTMessage::Consensus(cons) => {
                    match cons.kind() {
                        ConsensusMessageKind::PrePrepare(_) => {
                            pre_prepares.push(Arc::new(ReadOnly::new(message)));
                        }
                        ConsensusMessageKind::Prepare(_) => {
                            prepares.push(Arc::new(ReadOnly::new(message)));
                        }
                        ConsensusMessageKind::Commit(_) => {
                            commits.push(Arc::new(ReadOnly::new(message)));
                        }
                    }
                }
                PBFTMessage::ViewChange(_) => { unreachable!() }
                PBFTMessage::ObserverMessage(_) => { unreachable!() }
            }
        }

        Proof::new(metadata, pre_prepares, prepares, commits)
    }

    fn init_dec_log(proofs: Vec<SerProof<Self>>) -> DecLog<Self> {
        DecisionLog::from_proofs(proofs)
    }

    fn decompose_proof(proof: &SerProof<Self>) -> (&SerProofMetadata<Self>, Vec<&StoredMessage<ProtocolMessage<Self>>>) {
        let mut messages = Vec::new();

        for message in proof.pre_prepares() {
            messages.push(&**message.as_ref());
        }

        for message in proof.prepares() {
            messages.push(&**message.as_ref());
        }

        for message in proof.commits() {
            messages.push(&**message.as_ref());
        }

        (proof.metadata(), messages)
    }

    fn decompose_dec_log(proofs: &DecLog<Self>) -> Vec<&SerProof<Self>> {
        proofs.proofs().iter().collect()
    }
}