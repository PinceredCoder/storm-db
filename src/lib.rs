// TODO: remove
#![allow(unused)]

mod memtable;
mod sstable;

use memtable::MemTable;
use std::borrow::Borrow;

const MEMTABLE_CAPACITY: usize = 2usize.pow(20);

pub struct DBService<K, V> {
    memtable: MemTable<K, V>,
}

impl<K: Ord, V> DBService<K, V> {
    pub fn new() -> Self {
        Self {
            memtable: MemTable::new(MEMTABLE_CAPACITY),
        }
    }
}

impl<K: Ord, V> Default for DBService<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
