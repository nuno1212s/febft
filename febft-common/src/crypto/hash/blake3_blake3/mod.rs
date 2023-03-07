use blake3::OUT_LEN;

#[cfg(feature = "serialize_serde")]
use serde::{Serialize, Deserialize};

use crate::error::*;

pub struct Context {
    inner: blake3::Hasher,
}

#[derive(Hash, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(transparent)]
#[cfg_attr(feature = "serialize_serde", derive(Serialize, Deserialize))]
pub struct Digest([u8; Digest::LENGTH]);

impl Context {
    pub fn new() -> Self {
        let inner = blake3::Hasher::new();
        Context { inner }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finish(self) -> Digest {
        let h = self.inner.finalize();
        Digest(h.into())
    }
}

impl Digest {
    pub const LENGTH: usize = OUT_LEN;

    pub fn from_bytes(raw_bytes: &[u8]) -> Result<Self> {
        if raw_bytes.len() < Self::LENGTH {
            return Err("Digest has an invalid length")
                .wrapped(ErrorKind::CryptoHashBlake3Blake3);
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

#[cfg(test)]
mod tests {
    use super::Context;

    #[test]
    fn test_digest() {
        let mut ctx = Context::new();
        ctx.update(b"cool\n");
        let h = ctx.finish();
        assert_eq!(
            h.as_ref(),
            b"\xdf_m\x9b\xd4\xcd\xad\x9d\xe6\xd7w6\x8f\xcet{\x90\x85\xc8\xe1\xf5B\x15\x87\x85\xbey\xa6\x0b<\xdav",
        );
    }
}
