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

use atlas_common::error::*;
use atlas_common::ordering::Orderable;
use atlas_common::serialization_helper::SerMsg;
use atlas_communication::message::Header;
use atlas_communication::reconfiguration::NetworkInformationProvider;
use atlas_core::ordering_protocol::loggable::message::PersistentOrderProtocolTypes;
use atlas_core::ordering_protocol::networking::serialize::{
    OrderProtocolVerificationHelper, OrderingProtocolMessage, PermissionedOrderingProtocolMessage,
};

use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::message::{
    ConsensusMessage, ConsensusMessageKind, PBFTMessage, ViewChangeMessageKind,
};
use crate::bft::sync::view::ViewInfo;

#[cfg(feature = "serialize_capnp")]
pub mod capnp;

#[cfg(feature = "serialize_serde")]
pub mod serde;

pub fn serialize_consensus<W, RQ>(w: &mut W, message: &ConsensusMessage<RQ>) -> Result<()>
where
    RQ: SerMsg,
    W: Write + AsRef<[u8]> + AsMut<[u8]>,
{
    #[cfg(feature = "serialize_capnp")]
    capnp::serialize_consensus::<W, RQ>(w, message)?;

    #[cfg(feature = "serialize_serde")]
    serde::serialize_consensus::<W, RQ>(message, w)?;

    Ok(())
}

pub fn deserialize_consensus<R, RQ>(r: R) -> Result<ConsensusMessage<RQ>>
where
    RQ: SerMsg,
    R: Read + AsRef<[u8]>,
{
    #[cfg(feature = "serialize_capnp")]
    let result = capnp::deserialize_consensus::<R, RQ>(r)?;

    #[cfg(feature = "serialize_serde")]
    let result = serde::deserialize_consensus::<R, RQ>(r)?;

    Ok(result)
}

/// The serializable type, to be used to appease the compiler and it's requirements
pub struct PBFTConsensus<RQ>(PhantomData<fn() -> RQ>);

impl<RQ> OrderingProtocolMessage<RQ> for PBFTConsensus<RQ>
where
    RQ: SerMsg,
{
    type ProtocolMessage = PBFTMessage<RQ>;
    type DecisionMetadata = ProofMetadata;

    type DecisionAdditionalInfo = ();

    fn internally_verify_message<NI, OPVH>(
        network_info: &Arc<NI>,
        _header: &Header,
        message: &Self::ProtocolMessage,
    ) -> Result<()>
    where
        NI: NetworkInformationProvider,
        OPVH: OrderProtocolVerificationHelper<RQ, Self, NI>,
        Self: Sized,
    {
        match message {
            PBFTMessage::Consensus(consensus) => {
                let (_seq, _view) = (consensus.sequence_number(), consensus.view());

                match consensus.kind() {
                    ConsensusMessageKind::PrePrepare(requests) => {
                        let request_iter = requests.iter();

                        for request in request_iter {
                            let (header, message) = (request.header(), request.message());

                            let _ = OPVH::verify_request_message(
                                network_info,
                                header,
                                message.clone(),
                            )?;
                        }

                        Ok(())
                    }
                    ConsensusMessageKind::Prepare(_digest) => Ok(()),
                    ConsensusMessageKind::Commit(_digest) => Ok(()),
                }
            }
            PBFTMessage::ViewChange(view_change) => {
                let _view = view_change.sequence_number();

                match view_change.kind() {
                    ViewChangeMessageKind::Stop(timed_out_req) => {
                        for client_rq in timed_out_req.iter() {
                            let (header, message) = (client_rq.header(), client_rq.message());

                            let _ = OPVH::verify_request_message(
                                network_info,
                                header,
                                message.clone(),
                            )?;
                        }

                        Ok(())
                    }
                    ViewChangeMessageKind::StopQuorumJoin(_node) => Ok(()),
                    ViewChangeMessageKind::StopData(collect_data) => {
                        if let Some(_proof) = &collect_data.last_proof {}

                        Ok(())
                    }
                    ViewChangeMessageKind::Sync(leader_collects) => {
                        let (fwd, collects) =
                            (leader_collects.proposed(), leader_collects.collects());

                        {
                            let (header, message) = (fwd.header(), fwd.consensus_msg());

                            let _ = OPVH::verify_protocol_message(
                                network_info,
                                header,
                                PBFTMessage::Consensus(message.clone()),
                            )?;
                        }

                        for collect in collects {
                            let (header, message) = (collect.header(), collect.message());

                            let _ = OPVH::verify_protocol_message(
                                network_info,
                                header,
                                message.clone(),
                            )?;
                        }

                        Ok(())
                    }
                }
            }
            PBFTMessage::ObserverMessage(_m) => Ok(()),
        }
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_capnp(
        builder: atlas_capnp::consensus_messages_capnp::protocol_message::Builder,
        msg: &Self::ProtocolMessage,
    ) -> Result<()> {
        capnp::serialize_message::<RQ>(builder, msg)
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_capnp(
        reader: atlas_capnp::consensus_messages_capnp::protocol_message::Reader,
    ) -> Result<Self::ProtocolMessage> {
        capnp::deserialize_message::<RQ>(reader)
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_view_capnp(
        builder: atlas_capnp::cst_messages_capnp::view_info::Builder,
        msg: &Self::ViewInfo,
    ) -> Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_view_capnp(
        reader: atlas_capnp::cst_messages_capnp::view_info::Reader,
    ) -> Result<Self::ViewInfo> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn serialize_proof_capnp(
        builder: atlas_capnp::cst_messages_capnp::proof::Builder,
        msg: &Self::Proof,
    ) -> Result<()> {
        todo!()
    }

    #[cfg(feature = "serialize_capnp")]
    fn deserialize_proof_capnp(
        reader: atlas_capnp::cst_messages_capnp::proof::Reader,
    ) -> Result<Self::Proof> {
        todo!()
    }
}

impl<RQ> PermissionedOrderingProtocolMessage for PBFTConsensus<RQ>
where
    RQ: SerMsg,
{
    type ViewInfo = ViewInfo;
}

impl<RQ> PersistentOrderProtocolTypes<RQ, Self> for PBFTConsensus<RQ>
where
    RQ: SerMsg,
{
    type Proof = Proof<RQ>;

    fn verify_proof<NI, OPVH>(network_info: &Arc<NI>, proof: Self::Proof) -> Result<Self::Proof>
    where
        NI: NetworkInformationProvider,
        OPVH: OrderProtocolVerificationHelper<RQ, Self, NI>,
        Self: Sized,
    {
        let (metadata, messages) = proof.into_parts();

        for msg in &messages {
            let _ =
                OPVH::verify_protocol_message(network_info, msg.header(), msg.message().clone())?;
        }

        let proof = Proof::init_from_messages(metadata, messages)?;

        Ok(proof)
    }
}
