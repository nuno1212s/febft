//! Public key cryptographic operations.

use crate::bft::error::*;

#[cfg(feature = "crypto_signature_ring_ed25519")]
mod ring_ed25519;

/// A `KeyPair` holds both the private and public key components
/// that form a digital identity.
pub struct KeyPair {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::KeyPair,
}

/// A `Signature` is the result of using `KeyPair::sign`. Represents
/// a digital signature with a private key.
#[derive(Copy, Clone)]
pub struct Signature {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::Signature,
}

impl KeyPair {
    /// Constructs a `KeyPair` from a byte buffer of appropriate size.
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_signature_ring_ed25519")]
            { ring_ed25519::KeyPair::from_bytes(raw_bytes)? }
        };
        Ok(KeyPair { inner })
    }

    /// Performs a cryptographic signature of an arbitrary message.
    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        let inner = self.inner.sign(message)?;
        Ok(Signature { inner })
    }

    /// Verifies if a signature is valid, i.e. if this `KeyPair` performed it.
    /// Forged signatures can be verified successfully, so a good public key
    /// crypto algorithm and key size should be picked.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.inner.verify(message, &signature.inner)
    }
}

impl Signature {
    /// Length in bytes required to represent a `Signature` in memory.
    ///
    /// Note: doesn't imply `Signature::LENGTH == std::mem::size_of::<Signature>`.
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    pub const LENGTH: usize = ring_ed25519::Signature::LENGTH;

    /// Constructs a `Signature` from a byte buffer of appropriate size.
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_signature_ring_ed25519")]
            { ring_ed25519::Signature::from_bytes(raw_bytes)? }
        };
        Ok(Signature { inner })
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
