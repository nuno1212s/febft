use ring::{signature as rsig, signature::KeyPair as RKeyPair};

use crate::bft::error::prelude::*;

pub struct KeyPair {
    sk: rsig::Ed25519KeyPair,
    pk: rsig::UnparsedPublicKey<<rsig::Ed25519KeyPair as RKeyPair>::PublicKey>,
}

pub type Signature = ring::signature::Signature;

impl KeyPair {
    pub fn from_bytes(seed_bytes: &[u8]) -> Result<Self> {
        let sk = rsig::Ed25519KeyPair::from_seed_unchecked(seed_bytes)
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid seed for ed25519 key")?;
        let pk = sk.public_key().clone();
        let pk = rsig::UnparsedPublicKey::new(&rsig::ED25519, pk);
        Ok(KeyPair { pk, sk })
    }

    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        Ok(self.sk.sign(message))
    }

    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.pk.verify(message, signature.as_ref())
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid signature")
    }
}
