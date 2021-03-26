//! Abstractions over different crypto hash digest algorithms.

#[cfg(feature = "crypto_hash_ring_sha2")]
mod ring_sha2;

use crate::bft::error::*;

/// The type `Context` represents an on-going hash digest calculation.
pub struct Context {
    #[cfg(feature = "crypto_hash_ring_sha2")]
    inner: ring_sha2::Context,
}

/// Represents a hash digest.
#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Digest {
    #[cfg(feature = "crypto_hash_ring_sha2")]
    inner: ring_sha2::Digest,
}

impl Context {
    /// Initializes a new `Context` instance.
    ///
    /// Feed this it data with `Context::update`.
    pub fn new() -> Self {
        let inner = {
            #[cfg(feature = "crypto_hash_ring_sha2")]
            { ring_sha2::Context::new() }
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
    };

    /// Constructs a `Digest` from a byte buffer of appropriate size.
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_hash_ring_sha2")]
            { ring_sha2::Digest::from_bytes(raw_bytes) }
        }?;
        Ok(Digest { inner })
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
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
