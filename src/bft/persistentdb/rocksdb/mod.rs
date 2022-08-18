use std::path::Path;

use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, CompactOptions, DBWithThreadMode, Direction,
    IteratorMode, Options, SingleThreaded, WriteBatchWithTransaction, DB,
};

use crate::bft::error::*;

pub(crate) struct RocksKVDB {
    db: DBWithThreadMode<SingleThreaded>,
}

impl RocksKVDB {
    pub fn new<T>(db_location: T, prefixes: Vec<&'static str>) -> Result<Self>
    where
        T: AsRef<Path>,
    {
        let mut cfs = Vec::with_capacity(prefixes.len());

        for cf in prefixes {
            let mut cf_opts = Options::default();

            cfs.push(ColumnFamilyDescriptor::new(cf, cf_opts));
        }

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors(&db_opts, db_location, cfs).unwrap();

        Ok(RocksKVDB { db })
    }

    fn get_handle(&self, prefix: &'static str) -> Result<&ColumnFamily> {
        let handle = self.db.cf_handle(prefix);

        if let Some(handle) = handle {
            Ok(handle)
        } else {
            Err(Error::simple_with_msg(
                ErrorKind::PersistentdbRocksdb,
                "Column family by that name does not exist",
            ))
        }
    }

    pub fn get<T>(&self, prefix: &'static str, key: T) -> Result<Option<Vec<u8>>>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .get_cf(handle, key)
            .wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn set<T>(&self, prefix: &'static str, key: T, data: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .put_cf(handle, key, data)
            .wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn set_all<T, Y>(&self, prefix: &'static str, values: T) -> Result<()>
    where
        T: Iterator<Item = (Y, Y)>,
        Y: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for (key, value) in values {
            batch.put_cf(handle, key, value)
        }

        self.db.write(batch).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn erase<T>(&self, prefix: &'static str, key: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .delete_cf(handle, key)
            .wrapped(ErrorKind::PersistentdbRocksdb)
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, prefix: &'static str, keys: T) -> Result<()>
    where
        T: Iterator<Item = Y>,
        Y: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        let mut batch = WriteBatchWithTransaction::<false>::default();

        for key in keys {
            batch.delete_cf(handle, key)
        }

        self.db.write(batch).wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn erase_range<T>(&self, prefix: &'static str, start: T, end: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        self.db
            .delete_range_cf(handle, start, end)
            .wrapped(ErrorKind::PersistentdbRocksdb)
    }

    pub fn compact_range<T>(
        &self,
        prefix: &'static str,
        start: Option<T>,
        end: Option<T>,
    ) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        let handle = self.get_handle(prefix)?;

        Ok(self
            .db
            .compact_range_cf_opt(handle, start, end, &CompactOptions::default()))
    }

    pub fn iter_range<T, Y>(
        &self,
        prefix: &'static str,
        start: Option<T>,
        end: Option<T>,
    ) -> Result<Y>
    where
        T: AsRef<[u8]>,
        Y: Iterator<Item = (T, T)>,
    {
        let handle = self.get_handle(prefix)?;

        let mut iterator = if let Some(start) = start {
            self.db.iterator_cf(
                handle,
                IteratorMode::From(start.as_ref(), Direction::Forward),
            )
        } else {
            self.db.iterator_cf(handle, IteratorMode::Start)
        };

        if let Some(end) = end {
            iterator.set_mode(IteratorMode::From(end.as_ref(), Direction::Reverse));
        }

        todo!()
        //Ok(iterator)
    }

    pub fn iter_prefix<T, Y>(&self, pset_moderefix: T) -> Result<Y>
    where
        T: AsRef<[u8]>,
        Y: Iterator<Item = Result<(Box<[u8]>, Box<u8>)>>,
    {
        todo!()
    }
}
