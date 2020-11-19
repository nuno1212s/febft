#[cfg(feature = "crypto_signature_ring_ed25519")]
mod ring_ed25519;

pub struct PublicKey {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::PublicKey,
}

pub struct SecretKey {
    #[cfg(feature = "crypto_signature_ring_ed25519")]
    inner: ring_ed25519::SecretKey,
}
