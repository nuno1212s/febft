use ring::digest::{
    self,
    SHA256,
    SHA256_OUTPUT_LEN,
};

use crate::bft::error::*;

pub struct Context {
    inner: digest::Context,
}

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Digest([u8; Digest::LENGTH]);

impl Context {
    pub fn new() -> Self {
        let inner = digest::Context::new(&SHA256);
        Context { inner }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finish(self) -> Digest {
        let h = self.inner.finish();
        Digest::from_bytes_unchecked(h.as_ref())
    }
}

impl Digest {
    pub const LENGTH: usize = SHA256_OUTPUT_LEN;

    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        if raw_bytes.len() < Self::LENGTH {
            return Err("Digest has an invalid length")
                .wrapped(ErrorKind::CryptoHashRingSha2);
        }
        Ok(Self::from_bytes_unchecked(raw_bytes))
    }

    fn from_bytes_unchecked(raw_bytes: &[u8]) -> Self {
        let mut inner = [0; Self::LENGTH];
        inner.copy_from_slice(&raw_bytes[..Self::LENGTH]);
        Self(inner)
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
