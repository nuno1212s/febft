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

#[cfg(feature = "serialize_serde")]
use ::serde::{Deserialize, Serialize};
use bytes::Bytes;

use atlas_common::error::*;
use atlas_communication::message::Header;
use atlas_communication::message_signing::NetworkMessageSignatureVerifier;
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_communication::serialize::Serializable;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, StatefulOrderProtocolMessage};
use atlas_core::ordering_protocol::networking::signature_ver::OrderProtocolSignatureVerificationHelper;
use atlas_core::persistent_log::PersistableOrderProtocol;
use atlas_execution::serialize::ApplicationData;

use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, PBFTMessage, ViewChangeMessage, ViewChangeMessageKind};
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
        D: ApplicationData,
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
        D: ApplicationData,
{
    #[cfg(feature = "serialize_capnp")]
        let result = capnp::deserialize_consensus::<R, D>(r)?;

    #[cfg(feature = "serialize_serde")]
        let result = serde::deserialize_consensus::<R, D>(r)?;

    Ok(result)
}

/// The serializable type, to be used to appease the compiler and it's requirements
pub struct PBFTConsensus<D: ApplicationData>(PhantomData<(D)>);

impl<D> PBFTConsensus<D> where D: ApplicationData {
    /// Verify the consensus internal message structure
    fn verify_consensus_message<S, SV, NI>(network_info: &Arc<NI>, header: &Header, msg: &ConsensusMessage<D::Request>) -> Result<bool>
        where S: Serializable,
              NI: NetworkInformationProvider,
              SV: NetworkMessageSignatureVerifier<S, NI>, {
        match msg.kind() {
            ConsensusMessageKind::PrePrepare(requests) => {
                requests.iter().map(|request| {
                    let header = request.header();
                    let client_request = request.message();

                    //TODO: Verify client request signature

                    Ok(false)
                }).reduce(|a, b| a.and(b))
                    .unwrap_or(Ok(true))
            }
            ConsensusMessageKind::Prepare(prepare) => {
                Ok(true)
            }
            ConsensusMessageKind::Commit(commit) => {
                Ok(true)
            }
        }
    }

    /// Verify view change message internal structure
    fn verify_view_change_message<S, SV, NI>(network_info: &Arc<NI>, header: &Header, msg: &ViewChangeMessage<D::Request>) -> Result<bool>
        where S: Serializable,
              NI: NetworkInformationProvider,
              SV: NetworkMessageSignatureVerifier<S, NI>, {
        match msg.kind() {
            ViewChangeMessageKind::Stop(timed_out_rqs) => {
                timed_out_rqs.iter().map(|request| {
                    let header = request.header();
                    let client_request = request.message();

                    Ok(false)
                }).reduce(|a, b| a.and(b)).unwrap_or(Ok(true))
            }
            ViewChangeMessageKind::StopQuorumJoin(joining_node) => {
                Ok(true)
            }
            ViewChangeMessageKind::StopData(data) => {
                if let Some(proof) = &data.last_proof {
                    proof.pre_prepares().iter().map(|pre_prepare| {
                        let header = pre_prepare.header();
                        let consensus_message = pre_prepare.message();

                        Self::verify_consensus_message::<S, SV, NI>(network_info, header, consensus_message)
                    }).reduce(|a, b| a.and(b)).unwrap_or(Ok(true))
                } else {
                    Ok(true)
                }
            }
            ViewChangeMessageKind::Sync(sync_message) => {
                let message = sync_message.message();

                let header = message.header();
                let message = message.consensus();

                Self::verify_consensus_message::<S, SV, NI>(network_info, header, message)
            }
        }
    }
}

impl<D> OrderingProtocolMessage for PBFTConsensus<D>
    where D: ApplicationData, {
    type ViewInfo = ViewInfo;
    type ProtocolMessage = PBFTMessage<D::Request>;
    type LoggableMessage = ConsensusMessage<D::Request>;
    type Proof = Proof<D::Request>;
    type ProofMetadata = ProofMetadata;

    fn verify_order_protocol_message<NI, OPVH, D2>(network_info: &NI, header: &Header, message: Self::ProtocolMessage) -> Result<(bool, Self::ProtocolMessage)> where NI: NetworkInformationProvider, OPVH: OrderProtocolSignatureVerificationHelper<D2, Self, NI>, D2: ApplicationData, Self: Sized {
        todo!()
    }

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

impl<D> StatefulOrderProtocolMessage for PBFTConsensus<D>
    where D: ApplicationData + 'static {
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
