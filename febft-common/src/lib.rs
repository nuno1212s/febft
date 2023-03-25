//! This crate, `febft-common` takes advantage of the feature flags
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
//! - If the serialization of wire messages is possible with `serde`:
//!     + With `serialize_serde`.
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
#![feature(type_alias_impl_trait)]

pub mod async_runtime;
pub mod collections;
pub mod crypto;
pub mod error;
pub mod globals;
pub mod ordering;
pub mod prng;
pub mod threadpool;
pub mod persistentdb;
pub mod channel;
pub mod socket;
pub mod mem_pool;