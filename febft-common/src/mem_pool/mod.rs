use std::cell::RefCell;
use std::ops::{Deref, DerefMut};

pub struct ClaimedMemory {
    capacity: usize,
    owned_mem: RefCell<Vec<Vec<u8>>>
}

impl ClaimedMemory {

    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            owned_mem: RefCell::new(vec![Vec::with_capacity(capacity)]),
        }
    }

    pub fn take_mem(&self) -> MemoryGrant {

        if self.owned_mem.borrow().is_empty() {
            let cap = self.capacity;

            MemoryGrant { mem_pool: self, bytes : RefCell::new(Vec::with_capacity(cap)) }
        } else {
            let bytes_mut = self.owned_mem.borrow_mut().swap_remove(0);

            MemoryGrant { mem_pool: self, bytes: RefCell::new(bytes_mut) }
        }
    }

    fn reclaim_mem(&self, bytes: Vec<u8>) {
        self.owned_mem.borrow_mut().push(bytes);
    }

}

pub struct MemoryGrant<'a> {
    mem_pool: &'a ClaimedMemory,
    bytes: RefCell<Vec<u8>>
}

impl<'a> MemoryGrant<'a> {
    pub fn buffer(&self) -> &RefCell<Vec<u8>> {
        &self.bytes
    }
}

impl<'a> Drop for MemoryGrant<'a> {
    fn drop(&mut self) {
        self.mem_pool.reclaim_mem(self.bytes.take())
    }
}