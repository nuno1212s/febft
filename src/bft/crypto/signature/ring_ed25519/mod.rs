use ring::{signature as rsig, signature::KeyPair as RKeyPair};

use crate::bft::error::*;

pub struct KeyPair {
    sk: rsig::Ed25519KeyPair,
    pk: PublicKey,
}

#[derive(Copy, Clone)]
pub struct PublicKey {
    pk: rsig::UnparsedPublicKey<<rsig::Ed25519KeyPair as RKeyPair>::PublicKey>,
}

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Signature([u8; Signature::LENGTH]);

impl KeyPair {
    pub fn from_bytes(seed_bytes: &[u8]) -> Result<Self> {
        let sk = rsig::Ed25519KeyPair::from_seed_unchecked(seed_bytes)
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid seed for ed25519 key")?;
        let pk = sk.public_key().clone();
        let pk = PublicKey::from_bytes_unchecked(pk);
        Ok(KeyPair { pk, sk })
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.pk
    }

    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        let signature = self.sk.sign(message);
        Ok(Signature::from_bytes_unchecked(signature.as_ref()))
    }
}

impl PublicKey {
    pub fn from_bytes<B: AsRef<[u8]>>(raw_bytes: B) -> Result<Self> {
        Ok(Self::from_bytes_unchecked(raw_bytes))
    }

    fn from_bytes_unchecked<B: AsRef<[u8]>>(raw_bytes: B) -> Result<Self> {
        let pk = rsig::UnparsedPublicKey::new(&rsig::ED25519, raw_bytes);
        PublicKey { pk }
    }

    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.pk.verify(message, signature.as_ref())
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid signature")
    }
}

impl Signature {
    pub const LENGTH: usize = 64;

    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        if raw_bytes.len() < Self::LENGTH {
            return Err("Signature has an invalid length")
                .wrapped(ErrorKind::CryptoSignatureRingEd25519);
        }
        Ok(Self::from_bytes_unchecked(raw_bytes))
    }

    fn from_bytes_unchecked(raw_bytes: &[u8]) -> Self {
        let mut inner = [0; Self::LENGTH];
        inner.copy_from_slice(&raw_bytes[..Self::LENGTH]);
        Self(inner)
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
