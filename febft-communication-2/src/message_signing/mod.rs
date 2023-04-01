use std::sync::Arc;
use intmap::IntMap;
use febft_common::crypto::hash::{Context, Digest};
use febft_common::crypto::signature::{KeyPair, PublicKey, Signature};
use febft_common::error::*;
use crate::config::PKConfig;

pub struct NodePKShared {
    my_key: KeyPair,
    peer_keys: IntMap<PublicKey>,
}

pub struct SignDetached {
    shared: Arc<NodePKShared>,
}

impl SignDetached {
    pub fn key_pair(&self) -> &KeyPair {
        &self.shared.my_key
    }
}

impl NodePKShared {

    pub fn from_config(config: PKConfig) -> Arc<Self> {
        Arc::new(Self {
            my_key: config.sk,
            peer_keys: config.pk
        })
    }

}

fn digest_parts(from: u32, to: u32, nonce: u64, payload: &[u8]) -> Digest {
    let mut ctx = Context::new();

    let buf = Self::CURRENT_VERSION.to_le_bytes();
    ctx.update(&buf[..]);

    let buf = from.to_le_bytes();
    ctx.update(&buf[..]);

    let buf = to.to_le_bytes();
    ctx.update(&buf[..]);

    let buf = nonce.to_le_bytes();
    ctx.update(&buf[..]);

    let buf = (payload.len() as u64).to_le_bytes();
    ctx.update(&buf[..]);

    ctx.update(payload);
    ctx.finish()
}

///Sign a given message, with the following passed parameters
/// From is the node that sent the message
/// to is the destination node
/// nonce is the none
/// and the payload is what we actually want to sign (in this case we will
/// sign the digest of the message, instead of the actual entire payload
/// since that would be quite slow)
pub(crate) fn sign_parts(
    sk: &KeyPair,
    from: u32,
    to: u32,
    nonce: u64,
    payload: &[u8],
) -> Signature {
    let digest = digest_parts(from, to, nonce, payload);
    // NOTE: unwrap() should always work, much like heap allocs
    // should always work
    sk.sign(digest.as_ref()).unwrap()
}

///Verify the signature of a given message, which contains
/// the following parameters
/// From is the node that sent the message
/// to is the destination node
/// nonce is the none
/// and the payload is what we actually want to sign (in this case we will
/// sign the digest of the message, instead of the actual entire payload
/// since that would be quite slow)
pub(crate) fn verify_parts(
    pk: &PublicKey,
    sig: &Signature,
    from: u32,
    to: u32,
    nonce: u64,
    payload: &[u8],
) -> Result<()> {
    let digest = digest_parts(from, to, nonce, payload);
    pk.verify(digest.as_ref(), sig)
}