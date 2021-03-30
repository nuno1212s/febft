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

/// The public component of a `KeyPair`.
#[derive(Copy, Clone)]
pub struct PublicKey {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::PublicKey,
}

/// Reference to a `PublicKey`.
pub struct PublicKeyRef<'a> {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: &'a ring_ed25519::PublicKey,
}

/// A `Signature` is the result of using `KeyPair::sign`. Represents
/// a digital signature with a private key.
#[derive(Copy, Clone)]
#[repr(transparent)]
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

    /// Returns a reference to the public component of this `KeyPair`.
    ///
    /// The returned key can be cloned, since it implements `Clone`.
    pub fn public_key<'a>(&'a self) -> PublicKeyRef<'a> {
        let inner = self.inner.public_key();
        PublicKeyRef { inner }
    }

    /// Performs a cryptographic signature of an arbitrary message.
    ///
    /// The hash of the message is calculated by `sign()`, so the users
    /// don't need to perform this step themselves.
    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        let inner = self.inner.sign(message)?;
        Ok(Signature { inner })
    }

}

impl<'a> From<PublicKeyRef<'a>> for PublicKey {
    fn from(pk: PublicKeyRef<'a>) -> PublicKey {
        let inner = pk.inner.clone();
        PublicKey { inner }
    }
}

impl<'a> PublicKeyRef<'a> {
    /// Check the `verify` documentation for `PublicKey`.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.inner.verify(message, &signature.inner)
    }
}

impl PublicKey {
    /// Constructs a `PublicKey` from a byte buffer of appropriate size.
    ///
    /// The buffer should be an owned type, such as a `Vec<u8>` or an
    /// array of bytes.
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_signature_ring_ed25519")]
            { ring_ed25519::PublicKey::from_bytes(raw_bytes)? }
        };
        Ok(PublicKey { inner })
    }

    /// Verifies if a signature is valid, i.e. if this `KeyPair` performed it.
    ///
    /// Forged signatures can be verified successfully, so a good public key
    /// crypto algorithm and key size should be picked.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.inner.verify(message, &signature.inner)
    }
}

impl Signature {
    /// Length in bytes required to represent a `Signature` in memory.
    pub const LENGTH: usize = {
        #[cfg(feature = "crypto_signature_ring_ed25519")]
        { ring_ed25519::Signature::LENGTH }
    };

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

#[cfg(test)]
mod tests {
    use super::{Signature, KeyPair};

    #[test]
    fn test_length() {
        assert_eq!(Signature::LENGTH, std::mem::size_of::<Signature>());
    }

    #[test]
    fn test_sign_verify() {
        #[cfg(feature = "crypto_signature_ring_ed25519")]
        let k = KeyPair::from_bytes(&[0; 32][..]).expect("Invalid key bytes");

        let message = b"test message";
        let signature = k.sign(message)
            .expect("Signature failed");
        k.verify(message, &signature)
            .expect("Verify failed");
    }
}
