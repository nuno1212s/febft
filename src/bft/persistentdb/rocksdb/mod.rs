use std::path::Path;

use rocksdb::{DB, DBWithThreadMode, Direction, IteratorMode, Options, SingleThreaded, WriteBatch, WriteBatchWithTransaction};

use crate::bft::error::*;

pub(crate) struct RocksKVDB {
    db: DBWithThreadMode<SingleThreaded>,
}

impl RocksKVDB {
    pub fn new<T>(db_location: T) -> Result<Self> where T: AsRef<Path> {
        Ok(RocksKVDB {
            db: DB::open(&Options::default(), db_location).wrapped(ErrorKind::PersistentdbRocksdb)?
        })
    }

    pub fn get<T>(&self, key: T) -> Result<Option<Vec<u8>>> where T: AsRef<[u8]>{
        self.db.get(key).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn set<T>(&self, key: T, data: T) -> Result<()> where T: AsRef<[u8]> {
        self.db.put(key, data).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn set_all<T, Y>(&self, values: T) -> Result<()> where T: Iterator<Item = (Y, Y)>, Y: AsRef<[u8]> {
        let mut batch = WriteBatchWithTransaction::<false>::default();

        for (key, value) in values {
            batch.put(key, value)
        }

        self.db.write(batch).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn erase<T>(&self, key: T) -> Result<()> where T: AsRef<[u8]> {
        self.db.delete(key).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, keys: T) -> Result<()> where T: Iterator<Item=Y>, Y: AsRef<[u8]> {
        let mut batch = WriteBatchWithTransaction::<false>::default();

        for key in keys {
            batch.delete(key)
        }

        self.db.write(batch).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn erase_range<T>(&self, start: T, end: T) -> Result<()> where T: AsRef<[u8]> {
        let option = self.db.cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME);

        if let Some(cf) = option {
            self.db.delete_range_cf(cf, start, end).wrapped(ErrorKind::PersistentdbRocksdb)?;

            Ok(())
        } else {
            Err(Error::simple_with_msg(ErrorKind::PersistentdbRocksdb, "Failed to get default column family"))
        }
    }

    pub fn compact_prefix<T>(&self, prefix: T) -> Result<()> where T: AsRef<[u8]> {
        todo!()
    }

    pub fn compact_range<T>(&self, start: Option<T>, end: Option<T>) -> Result<()> where T: AsRef<[u8]> {
        Ok(self.db.compact_range(start, end))
    }

    pub fn iter_range<T, Y>(&self, start: Option<T>, end: Option<T>) -> Result<Y> where T: AsRef<[u8]>, Y: Iterator<Item=(T, T)> {
        let mut iterator = if let Some(start) = start {
            self.db.iterator(IteratorMode::From(start.as_ref(), Direction::Forward))
        } else {
            self.db.iterator(IteratorMode::Start)
        };

        if let Some(end) = end {
            iterator.set_mode(IteratorMode::From(end.as_ref(), Direction::Reverse));
        }

        Ok(iterator)
    }


    pub fn iter_prefix<T, Y>(&self, prefix: T) -> Result<Y> where T: AsRef<[u8]>, Y: Iterator<Item=Result<(Box<[u8]>, Box<u8>)>> {
        todo!()
    }
}