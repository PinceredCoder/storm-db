mod memtable;
mod sstable;

use memtable::MemTable;

const MEMTABLE_CAPACITY: usize = 2usize.pow(20);

/*
    TODO:


    1. Implement the service
    1.1. Write tests
    2. Add bloom-filter
    3. Add write-ahead log
    3.1 Write tests
*/

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
