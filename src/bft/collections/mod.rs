use std::default::Default;

#[cfg(feature = "collections_randomstate_twox_hash")]
pub type RandomState = ::twox_hash::RandomXxh3HashBuilder64;

#[cfg(feature = "collections_randomstate_std")]
pub type RandomState = ::std::collections::hash_map::RandomState;

/// A `HashMap` with a faster hashing function.
pub type HashMap<K, V> = ::std::collections::HashMap<K, V, RandomState>;

/// A `HashSet` with a faster hashing function.
pub type HashSet<T> = ::std::collections::HashSet<T, RandomState>;

/// Creates a new `HashMap`.
pub fn hash_map<K, V>() -> HashMap<K, V> {
    HashMap::with_hasher(Default::default())
}

/// Creates a new `HashSet`.
pub fn hash_set<T>() -> HashSet<T> {
    HashSet::with_hasher(Default::default())
}
