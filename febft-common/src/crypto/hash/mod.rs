//! Abstractions over different crypto hash digest algorithms.

use std::fmt::{Debug, Formatter};

#[cfg(feature = "serialize_serde")]
use serde::{Deserialize, Serialize};

use crate::error::*;

#[cfg(feature = "crypto_hash_ring_sha2")]
mod ring_sha2;

#[cfg(feature = "crypto_hash_blake3_blake3")]
mod blake3_blake3;

/// The type `Context` represents an on-going hash digest calculation.
pub struct Context {
    #[cfg(feature = "crypto_hash_ring_sha2")]
    inner: ring_sha2::Context,

    #[cfg(feature = "crypto_hash_blake3_blake3")]
    inner: blake3_blake3::Context,
}

/// Represents a hash digest.
#[derive(Hash, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Digest {
    #[cfg(feature = "crypto_hash_ring_sha2")]
    inner: ring_sha2::Digest,

    #[cfg(feature = "crypto_hash_blake3_blake3")]
    inner: blake3_blake3::Digest,
}

impl Context {
    /// Initializes a new `Context` instance.
    ///
    /// Feed this it data with `Context::update`.
    pub fn new() -> Self {
        let inner = {
            #[cfg(feature = "crypto_hash_ring_sha2")]
                { ring_sha2::Context::new() }

            #[cfg(feature = "crypto_hash_blake3_blake3")]
                { blake3_blake3::Context::new() }
        };
        Context { inner }
    }

    /// Feeds the `Context` some data to be hashed.
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// Extracts the resulting digest of hashing data onto the `Context`.
    pub fn finish(self) -> Digest {
        let inner = self.inner.finish();
        Digest { inner }
    }
}

impl Digest {
    /// The length of the `Digest` in bytes.
    pub const LENGTH: usize = {
        #[cfg(feature = "crypto_hash_ring_sha2")]
            { ring_sha2::Digest::LENGTH }

        #[cfg(feature = "crypto_hash_blake3_blake3")]
            { blake3_blake3::Digest::LENGTH }
    };

    /// Constructs a `Digest` from a byte buffer of appropriate size.
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_hash_ring_sha2")]
                { ring_sha2::Digest::from_bytes(raw_bytes) }

            #[cfg(feature = "crypto_hash_blake3_blake3")]
                { blake3_blake3::Digest::from_bytes(raw_bytes) }
        }?;
        Ok(Digest { inner })
    }

    /// Hashes this `Digest` with the given `nonce`.
    pub fn entropy<B: AsRef<[u8]>>(&self, nonce: B) -> Self {
        let mut ctx = Context::new();
        ctx.update(nonce.as_ref());
        ctx.update(self.as_ref());
        ctx.finish()
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x?}", self.inner.as_ref().chunks(4).next().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::Digest;

    #[test]
    fn test_length() {
        assert_eq!(Digest::LENGTH, std::mem::size_of::<Digest>());
    }
}
