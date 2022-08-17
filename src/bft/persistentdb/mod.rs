use std::path::Path;
use std::sync::Arc;

use crate::bft::error::*;
use crate::bft::persistentdb::rocksdb::RocksKVDB;

pub mod rocksdb;

#[derive(Clone)]
pub struct KVDB {
    inner: Arc<RocksKVDB>,
}

impl KVDB {
    pub fn new<T>(db_path: T) -> Result<Self>
    where
        T: AsRef<Path>,
    {
        Ok(Self {
            inner: Arc::new(RocksKVDB::new(db_path)?),
        })
    }

    pub fn get<T>(&self, key: T) -> Result<Option<Vec<u8>>>
    where
        T: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn exists<T>(&self, key: T) -> Result<bool>
    where
        T: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn set<T, Y>(&self, key: T, data: Y) -> Result<()>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn set_all<T, Y, Z>(&self, values: T) -> Result<()>
    where
        T: Iterator<Item = (Y, Z)>,
        Y: AsRef<[u8]>,
        Z: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn delete<T>(&self, key: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        todo!()
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, keys: T) -> Result<()>
    where
        T: AsRef<[Y]>,
        Y: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn erase_range<T>(&self, start: T, end: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn compact_range<T>(&self, start: T, end: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        todo!()
    }

    pub fn iter<T, Y>(&self) -> Result<T>
    where
        Y: AsRef<[u8]>,
        T: Iterator<Item = (Y, Y)>,
    {
        todo!()
    }

    pub fn iter_range<T, Y>(&self, start: Option<T>, end: Option<T>) -> Result<Y>
    where
        T: AsRef<[u8]>,
        Y: Iterator<Item = (T, T)>,
    {
        todo!()
    }

    pub fn iter_prefix<T, Y>(&self, prefix: T) -> Y
    where
        T: AsRef<[u8]>,
        Y: Iterator<Item = (T, T)>,
    {
        todo!()
    }
}
