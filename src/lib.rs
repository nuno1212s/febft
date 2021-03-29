//! This crate, `febft`, implements a byzantine fault tolerant state machine
//! replication library in Rust, whilst taking advantage of the feature flags
//! in `Cargo.toml` to provide a super flexible, modular API.
//!
//! # Feature flags
//!
//! At the moment, a user is able to customize:
//!
//! - The asynchronous runtime used by this crate:
//!     + E.g. To use `tokio`, enter the feature flag `async_runtime_tokio`.
//! - The thread pool used to execute CPU intensive tasks:
//!     + E.g. `threadpool_cthpool`.
//! - The sockets library used to communicate with other nodes:
//!     + E.g. `socket_async_std_tcp`.
//! - The framework used to serialize messages sent over the wire:
//!     + E.g. `serialize_serde_messagepack`.
//! - The crypto library used to perform public key crypto operations:
//!     + E.g. `crypto_signature_ring_ed25519`.
//! - The crypto library used to calculate hash digests of messages:
//!     + E.g. `crypto_hash_ring_sha2`.
//!
//! However, for convenience, some sane default feature flags are already
//! configured, which should perform well under any environment. Mind you,
//! the user, that this is a BFT library, so software variation is encouraged;
//! in a typical system setup, you would probably employ different backend
//! libraries performing identical duties.

#[cfg(feature = "expose_impl")]
pub mod bft;

#[cfg(not(feature = "expose_impl"))]
mod bft;

// TODO: re-export some stuff
