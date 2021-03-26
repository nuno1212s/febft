use ring::digest::{
    self,
    Digest,
    SHA512,
    SHA512_OUTPUT_LEN,
};

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Digest([u8; Digest::LENGTH]);

impl Digest {
    pub const LENGTH: usize = SHA512_OUTPUT_LEN;

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
