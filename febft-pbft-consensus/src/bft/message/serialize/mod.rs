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
use atlas_common::ordering::Orderable;
use atlas_communication::message::{Header, StoredMessage};
use atlas_communication::reconfiguration_node::NetworkInformationProvider;
use atlas_core::ordering_protocol::loggable::PersistentOrderProtocolTypes;
use atlas_core::ordering_protocol::networking::serialize::{OrderingProtocolMessage, PermissionedOrderingProtocolMessage};
use atlas_core::ordering_protocol::networking::signature_ver::OrderProtocolSignatureVerificationHelper;
use atlas_smr_application::serialize::ApplicationData;

use crate::bft::log::decisions::{Proof, ProofMetadata};
use crate::bft::message::{ConsensusMessage, ConsensusMessageKind, FwdConsensusMessage, PBFTMessage, ViewChangeMessage, ViewChangeMessageKind};
use crate::bft::sync::LeaderCollects;
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

impl<D> OrderingProtocolMessage<D> for PBFTConsensus<D>
    where D: ApplicationData, {
    type ProtocolMessage = PBFTMessage<D::Request>;
    type ProofMetadata = ProofMetadata;

    fn verify_order_protocol_message<NI, OPVH>(network_info: &Arc<NI>, header: &Header, message: Self::ProtocolMessage) -> Result<Self::ProtocolMessage>
        where NI: NetworkInformationProvider,
              OPVH: OrderProtocolSignatureVerificationHelper<D, Self, NI>, Self: Sized {
        match message {
            PBFTMessage::Consensus(consensus) => {
                let (seq, view) = (consensus.sequence_number(), consensus.view());

                match consensus.into_kind() {
                    ConsensusMessageKind::PrePrepare(requests) => {
                        let mut request_copy = Vec::with_capacity(requests.len());

                        let request_iter = requests.into_iter();

                        let mut global_res = true;

                        for request in request_iter {
                            let (header, message) = request.into_inner();

                            let message = OPVH::verify_request_message(network_info, &header, message)?;

                            let stored_msg = StoredMessage::new(header, message);

                            request_copy.push(stored_msg);
                        }

                        let consensus = ConsensusMessage::new(seq, view, ConsensusMessageKind::PrePrepare(request_copy));

                        Ok(PBFTMessage::Consensus(consensus))
                    }
                    ConsensusMessageKind::Prepare(digest) => {
                        Ok(PBFTMessage::Consensus(ConsensusMessage::new(seq, view, ConsensusMessageKind::Prepare(digest))))
                    }
                    ConsensusMessageKind::Commit(digest) => {
                        Ok(PBFTMessage::Consensus(ConsensusMessage::new(seq, view, ConsensusMessageKind::Commit(digest))))
                    }
                }
            }
            PBFTMessage::ViewChange(view_change) => {
                let (view) = view_change.sequence_number();

                match view_change.into_kind() {
                    ViewChangeMessageKind::Stop(timed_out_req) => {
                        let mut rq_copy = Vec::with_capacity(timed_out_req.len());

                        let rq_iter = timed_out_req.into_iter();

                        for client_rq in rq_iter {
                            let (header, message) = client_rq.into_inner();

                            let rq_message = OPVH::verify_request_message(network_info, &header, message)?;

                            let stored_rq = StoredMessage::new(header, rq_message);

                            rq_copy.push(stored_rq);
                        }

                        Ok(PBFTMessage::ViewChange(ViewChangeMessage::new(view, ViewChangeMessageKind::Stop(rq_copy))))
                    }
                    ViewChangeMessageKind::StopQuorumJoin(node) => {
                        Ok(PBFTMessage::ViewChange(ViewChangeMessage::new(view, ViewChangeMessageKind::StopQuorumJoin(node))))
                    }
                    ViewChangeMessageKind::StopData(collect_data) => {
                        if let Some(proof) = &collect_data.last_proof {}

                        let vcm = ViewChangeMessage::new(view, ViewChangeMessageKind::StopData(collect_data));
                        Ok(PBFTMessage::ViewChange(vcm))
                    }
                    ViewChangeMessageKind::Sync(leader_collects) => {
                        let (fwd, collects) = leader_collects.into_inner();

                        let res = {
                            let (header, message) = fwd.into_inner();

                            let message = OPVH::verify_protocol_message(network_info, &header, PBFTMessage::Consensus(message))?;

                            let message = FwdConsensusMessage::new(header, message.into_consensus());

                            message
                        };

                        let mut collected_messages = Vec::with_capacity(collects.len());

                        let iter = collects.into_iter();

                        for collect in iter {
                            let (header, message) = collect.into_inner();

                            let message = OPVH::verify_protocol_message(network_info, &header, message)?;

                            collected_messages.push(StoredMessage::new(header, message));
                        }

                        let vc = ViewChangeMessage::new(view, ViewChangeMessageKind::Sync(LeaderCollects::new(res, collected_messages)));

                        Ok(PBFTMessage::ViewChange(vc))
                    }
                }
            }
            PBFTMessage::ObserverMessage(m) => Ok(PBFTMessage::ObserverMessage(m))
        }
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

impl<D> PermissionedOrderingProtocolMessage for PBFTConsensus<D> where D: ApplicationData + 'static {
    type ViewInfo = ViewInfo;
}

impl<D> PersistentOrderProtocolTypes<D, Self> for PBFTConsensus<D>
    where D: ApplicationData + 'static {
    type Proof = Proof<D::Request>;

    fn verify_proof<NI, OPVH>(network_info: &Arc<NI>, proof: Self::Proof) -> Result<Self::Proof>
        where NI: NetworkInformationProvider,
              OPVH: OrderProtocolSignatureVerificationHelper<D, Self, NI>,
              Self: Sized {

        let (metadata, messages) = proof.into_parts();

        for msg in &messages {
            let _ = OPVH::verify_protocol_message(network_info, msg.header(), msg.message().clone())?;
        }

        let proof = Proof::init_from_messages(metadata, messages)?;

        Ok(proof)
    }
}
