use std::{borrow::Borrow, collections::BTreeMap};

/// Use None as the thubmstone
pub struct MemTable<K, V> {
    data: BTreeMap<K, MemTableEntry<V>>,
    max_size: usize,
}

#[derive(Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum MemTableEntry<V> {
    Value(V),
    Tombstone,
}

impl<K: Ord, V> MemTable<K, V> {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: BTreeMap::new(),
            max_size,
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<(), MemTableError> {
        if self.data.len() >= self.max_size {
            return Err(MemTableError::CapacityExceeded);
        }

        self.data.insert(key, MemTableEntry::Value(value));

        Ok(())
    }

    pub fn get<Q: Ord>(&self, key: &Q) -> Option<&MemTableEntry<V>>
    where
        K: Borrow<Q>,
    {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: K) {
        self.data.insert(key, MemTableEntry::Tombstone);
    }

    pub fn is_full(&self) -> bool {
        self.data.len() >= self.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn into_sorted_vec(self) -> Vec<(K, MemTableEntry<V>)> {
        self.data.into_iter().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemTableError {
    #[error("memtable capacity exceeded")]
    CapacityExceeded,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut memtable = MemTable::new(10);

        assert!(memtable.put(1, "value1".to_string()).is_ok());
        assert_eq!(
            memtable.get(&1),
            Some(&MemTableEntry::Value("value1".to_string()))
        );

        memtable.delete(1);
        assert_eq!(memtable.get(&1), Some(&MemTableEntry::Tombstone));
    }

    #[test]
    fn test_capacity_limit() {
        let mut memtable = MemTable::new(2);

        assert!(memtable.put(1, "value1".to_string()).is_ok());
        assert!(memtable.put(2, "value2".to_string()).is_ok());
        assert!(memtable.put(3, "value3".to_string()).is_err());

        assert!(memtable.is_full());
    }

    #[test]
    fn test_ordered_iteration() {
        let mut memtable = MemTable::new(10);

        memtable
            .put(3, "three".to_string())
            .expect("Failed to put value");
        memtable
            .put(1, "one".to_string())
            .expect("Failed to put value");
        memtable
            .put(2, "two".to_string())
            .expect("Failed to put value");

        let sorted: Vec<_> = memtable.into_sorted_vec();
        assert_eq!(sorted[0].0, 1);
        assert_eq!(sorted[1].0, 2);
        assert_eq!(sorted[2].0, 3);
    }
}
