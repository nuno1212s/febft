use crate::bft::message::ConsensusMessage;
use anyhow::Context;
use atlas_common::error::*;
use atlas_common::serialization_helper::SerMsg;
use std::io::{Read, Write};

pub fn serialize_consensus<W, RQ>(m: &ConsensusMessage<RQ>, w: &mut W) -> Result<()>
where
    W: Write + AsMut<[u8]>,
    RQ: SerMsg,
{
    bincode::serde::encode_into_std_write(m, w, bincode::config::standard()).context(format!(
        "Failed to serialize message {} bytes len",
        w.as_mut().len()
    ))?;

    Ok(())
}

pub fn deserialize_consensus<R, RQ>(r: R) -> Result<ConsensusMessage<RQ>>
where
    RQ: SerMsg,
    R: Read + AsRef<[u8]>,
{
    let msg = bincode::serde::decode_borrowed_from_slice(r.as_ref(), bincode::config::standard())
        .context("Failed to deserialize message")?;

    Ok(msg)
}
