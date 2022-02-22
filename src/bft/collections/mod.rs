//! Wrappers around `std::collections`.

use std::default::Default;
use dashmap::DashMap;

#[cfg(feature = "collections_randomstate_fxhash")]
pub type RandomState = ::std::hash::BuildHasherDefault<::fxhash::FxHasher>;

#[cfg(feature = "collections_randomstate_twox_hash")]
pub type RandomState = ::twox_hash::RandomXxh3HashBuilder64;

#[cfg(feature = "collections_randomstate_std")]
pub type RandomState = ::std::collections::hash_map::RandomState;

/// A map which, as the name suggests, maintains the order of its `(K, V)` pairs.
pub type OrderedMap<K, V> = ::linked_hash_map::LinkedHashMap<K, V, RandomState>;

/// A `HashMap` with a faster hashing function.
pub type HashMap<K, V> = ::std::collections::HashMap<K, V, RandomState>;

/// A `HashSet` with a faster hashing function.
pub type HashSet<T> = ::std::collections::HashSet<T, RandomState>;

pub type ConcurrentHashMap<K, V> = ::dashmap::DashMap<K, V, RandomState>;

/// Creates a new `OrderedMap`.
pub fn ordered_map<K: Eq + ::std::hash::Hash, V>() -> OrderedMap<K, V> {
    OrderedMap::with_hasher(Default::default())
}

/// Creates a new `HashMap`.
pub fn hash_map<K, V>() -> HashMap<K, V> {
    HashMap::with_hasher(Default::default())
}

/// Creates a new `HashSet`.
pub fn hash_set<T>() -> HashSet<T> {
    HashSet::with_hasher(Default::default())
}

/// Creates a new `HashMap`, with a custom capacity.
pub fn hash_map_capacity<K, V>(cap: usize) -> HashMap<K, V> {
    HashMap::with_capacity_and_hasher(cap, Default::default())
}

/// Creates a new `HashSet`, with a custom capacity.
pub fn hash_set_capacity<T>(cap: usize) -> HashSet<T> {
    HashSet::with_capacity_and_hasher(cap, Default::default())
}

pub fn concurrent_hash_map<K, V>() -> ConcurrentHashMap<K, V> { dashmap::DashMap::with_hasher(Default::default()) }

pub fn concurrent_hash_map_with_capacity<K, V> (size: usize) -> ConcurrentHashMap<K, V> {
    DashMap::with_capacity_and_hasher(size, Default::default())
}