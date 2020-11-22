use ring::{signature as rsig, signature::KeyPair as RKeyPair};

use crate::bft::error::prelude::*;

pub struct KeyPair {
    sk: rsig::Ed25519KeyPair,
    pk: rsig::UnparsedPublicKey<<rsig::Ed25519KeyPair as RKeyPair>::PublicKey>,
}

#[derive(Copy, Clone)]
pub struct Signature {
    value: [u8; MAX_LEN],
    len: usize,
}

// copied from ring's source
const ELEM_MAX_BITS: usize = 384;
const ELEM_MAX_BYTES: usize = (ELEM_MAX_BITS + 7) / 8;
const SCALAR_MAX_BYTES: usize = ELEM_MAX_BYTES;
const MAX_LEN: usize = 1/*tag:SEQUENCE*/ + 2/*len*/ +
    (2 * (1/*tag:INTEGER*/ + 1/*len*/ + 1/*zero*/ + SCALAR_MAX_BYTES));

impl KeyPair {
    pub fn from_bytes(seed_bytes: &[u8]) -> Result<Self> {
        let sk = rsig::Ed25519KeyPair::from_seed_unchecked(seed_bytes)
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid seed for ed25519 key")?;
        let pk = sk.public_key().clone();
        let pk = rsig::UnparsedPublicKey::new(&rsig::ED25519, pk);
        Ok(KeyPair { pk, sk })
    }

    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        Ok(unsafe {
            // safety: safe, because I copied the type from the
            // source code of ring, so they should have identical
            // memory representations
            std::mem::transmute(self.sk.sign(message))
        })
    }

    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.pk.verify(message, signature.as_ref())
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid signature")
    }
}

impl Signature {
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        if raw_bytes.len() > MAX_LEN {
            return Err("Signature is too long")
                .wrapped(ErrorKind::CryptoSignatureRingEd25519);
        }
        let mut sig = Signature {
            value: [0; MAX_LEN],
            len: raw_bytes.len(),
        };
        (&mut sig.value[..raw_bytes.len()]).copy_from_slice(raw_bytes);
        Ok(sig)
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.value[..self.len]
    }
}
