use std::default::Default;

use twox_hash::RandomXxh3HashBuilder64;

/// A `HashMap` with a faster hashing function.
pub type HashMap<K, V> = ::std::collections::HashMap<K, V, RandomXxh3HashBuilder64>;

/// A `HashSet` with a faster hashing function.
pub type HashSet<T> = ::std::collections::HashSet<T, RandomXxh3HashBuilder64>;

/// Creates a new `HashMap`.
pub fn hash_map<K, V>() -> HashMap<K, V> {
    HashMap::with_hasher(Default::default())
}

/// Creates a new `HashSet`.
pub fn hash_set<T>() -> HashSet<T> {
    HashSet::with_hasher(Default::default())
}
