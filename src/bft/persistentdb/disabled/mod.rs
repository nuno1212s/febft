use std::path::Path;
use crate::bft::error::*;

#[derive(Clone)]
pub(crate) struct DisabledKV;

impl DisabledKV {
    pub fn new<T>(_db_location: T, _prefixes: Vec<&'static str>) -> Result<Self>
    where
        T: AsRef<Path>,
    {
        Ok(DisabledKV)
    }


    pub fn get<T>(&self, _prefix: &'static str, _key: T) -> Result<Option<Vec<u8>>>
    where
        T: AsRef<[u8]>,
    {
        Ok(None)
    }

    pub fn get_all<T, Y>(&self, _keys: T) -> Result<Vec<Result<Option<Vec<u8>>>>>
    where
        T: Iterator<Item = (&'static str, Y)>,
        Y: AsRef<[u8]>,
    {
        Ok(vec![])
    }

    pub fn exists<T>(&self, _prefix: &'static str, _key: T) -> Result<bool>
    where
        T: AsRef<[u8]>,
    {
        Ok(false)
    }

    pub fn set<T, Y>(&self, _prefix: &'static str, _key: T, _data: Y) -> Result<()>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>,
    {
        Ok(())
    }

    pub fn set_all<T, Y, Z>(&self, _prefix: &'static str, _values: T) -> Result<()>
    where
        T: Iterator<Item = (Y, Z)>,
        Y: AsRef<[u8]>,
        Z: AsRef<[u8]>,
    {
        Ok(())
    }

    pub fn erase<T>(&self, _prefix: &'static str, _key: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        Ok(())
    }

    /// Delete a set of keys
    /// Accepts an [`&[&[u8]]`], in any possible form, as long as it can be dereferenced
    /// all the way to the intended target.
    pub fn erase_keys<T, Y>(&self, _prefix: &'static str, _keys: T) -> Result<()>
    where
        T: Iterator<Item = Y>,
        Y: AsRef<[u8]>,
    {
        Ok(())
    }

    pub fn erase_range<T>(&self, _prefix: &'static str, _start: T, _end: T) -> Result<()>
    where
        T: AsRef<[u8]>,
    {
        Ok(())
    }

    pub fn compact_range<T, Y>(
        &self,
        _prefix: &'static str,
        _start: Option<T>,
        _end: Option<Y>,
    ) -> Result<()>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>,
    {
        Ok(())    
    }

    pub fn iter_range<T, Y>(
        &self,
        _prefix: &'static str,
        _start: Option<T>,
        _end: Option<Y>,
    ) -> Result<Box<dyn Iterator<Item = Result<(Box<[u8]>,Box<[u8]>)>> + '_>>
    where
        T: AsRef<[u8]>,
        Y: AsRef<[u8]>
    {
        //Return an empty iterator
        Ok(Box::new(vec![].into_iter()))
    }
}
