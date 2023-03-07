#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

#[cfg(feature = "serialize_serde")]
use serde_big_array::BigArray;

use ring::{
    signature as rsig,
    signature::KeyPair as RKeyPair,
    signature::ED25519_PUBLIC_KEY_LEN,
};

use crate::error::*;

pub struct KeyPair {
    sk: rsig::Ed25519KeyPair,
    pk: PublicKey,
}

type RPubKey = <rsig::Ed25519KeyPair as RKeyPair>::PublicKey;

#[derive(Copy, Clone)]
pub struct PublicKey {
    pk: rsig::UnparsedPublicKey<RPubKey>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Signature(
    #[cfg_attr(feature = "serialize_serde", serde(with = "BigArray"))]
    [u8; Signature::LENGTH]
);

impl KeyPair {
    pub fn from_bytes(seed_bytes: &[u8]) -> Result<Self> {
        let sk = rsig::Ed25519KeyPair::from_seed_unchecked(seed_bytes)
            .simple_msg(ErrorKind::CryptoSignatureRingEd25519, "Invalid seed for ed25519 key")?;
        let pk = sk.public_key().clone();
        let pk = PublicKey::from_bytes_unchecked(pk.as_ref());
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
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        if raw_bytes.len() < ED25519_PUBLIC_KEY_LEN {
            return Err("Public key has an invalid length")
                .wrapped(ErrorKind::CryptoSignatureRingEd25519);
        }
        Ok(Self::from_bytes_unchecked(raw_bytes))
    }

    fn from_bytes_unchecked(raw_bytes: &[u8]) -> Self {
        let mut buf = [0; ED25519_PUBLIC_KEY_LEN];
        buf.copy_from_slice(&raw_bytes[..ED25519_PUBLIC_KEY_LEN]);
        let pk: RPubKey = unsafe {
            // safety remarks: ring represents `RPubKey` as:
            // pub struct PublicKey([u8; ED25519_PUBLIC_KEY_LEN])
            std::mem::transmute(buf)
        };
        let pk = rsig::UnparsedPublicKey::new(&rsig::ED25519, pk);
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

#[cfg(test)]
mod tests {
    use super::{Signature, KeyPair};

    #[test]
    fn test_sign_verify() {
        let k = KeyPair::from_bytes(&[0; 32][..]).expect("Invalid key bytes");

        let message = b"test message";
        let signature = k.sign(message)
            .expect("Signature failed");
        k
            .public_key()
            .verify(message, &signature)
            .expect("Verify failed");
    }
}
