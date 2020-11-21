use crate::bft::error::prelude::*;

#[cfg(feature = "crypto_signature_ring_ed25519")]
mod ring_ed25519;

pub struct KeyPair {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::KeyPair,
}

#[derive(Copy, Clone)]
pub struct Signature {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::Signature,
}

impl KeyPair {
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_signature_ring_ed25519")]
            ring_ed25519::KeyPair::from_bytes(raw_bytes)?
        };
        Ok(KeyPair { inner })
    }

    pub fn sign(&self, message: &[u8]) -> Result<Signature> {
        let inner = self.inner.sign(message)?;
        Ok(Signature { inner })
    }

    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<()> {
        self.inner.verify(message, &signature.inner)
    }
}

impl Signature {
    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let inner = {
            #[cfg(feature = "crypto_signature_ring_ed25519")]
            ring_ed25519::Signature::from_bytes(raw_bytes)?
        };
        Ok(Signature { inner })
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}
