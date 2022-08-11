
use crate::bft::error::*;

pub mod rocksdb;

pub struct KVDB {

}

impl KVDB {

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        todo!()
    }

    pub fn set(&self, key: Vec<u8>, data: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub fn delete(&self, key: &Vec<u8>) {}

    pub fn delete_keys(&self, keys: &Vec<Vec<u8>>) {}

    pub fn erase_range(&self, start: &Vec<u8>, end: &Vec<u8>) {}

    pub fn compact_prefix(&self, prefix: &Vec<u8>) {}

    pub fn compact_range(&self, start: &Vec<u8>, end: &Vec<u8>) {}

}
