use std::borrow::Borrow;

// TODO: use some Trait
use basalgo::tree::AvlTree;

pub struct MemTable<K, V> {
    data: AvlTree<K, V>,
    max_size: usize,
}

impl<K: Ord, V> MemTable<K, V> {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: AvlTree::new(),
            max_size,
        }
    }

    pub fn put(&mut self, key: K, value: V) -> Result<(), MemTableError> {
        if self.data.size() >= self.max_size {
            return Err(MemTableError::CapacityExceeded);
        }

        self.data.insert(key, value);

        Ok(())
    }

    pub fn get<Q: Borrow<K>>(&self, key: &Q) -> Option<&V> {
        self.data.get(key)
    }

    pub fn delete<Q: Borrow<K>>(&mut self, key: &Q) {
        self.data.remove(key);
    }

    pub fn is_full(&self) -> bool {
        self.data.size() >= self.max_size
    }

    pub fn size(&self) -> usize {
        self.data.size()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.max_size
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.data.iter()
    }

    pub fn into_sorted_vec(self) -> Vec<(K, V)> {
        self.data.into_iter().collect()
    }

    pub fn clear(&mut self) {
        self.data = AvlTree::new();
    }

    pub fn merge_from(&mut self, other: MemTable<K, V>) -> Result<(), MemTableError> {
        for (k, v) in other.data {
            self.put(k, v)?
        }
        Ok(())
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
        assert_eq!(memtable.get(&1), Some(&"value1".to_string()));
        assert_eq!(memtable.size(), 1);

        memtable.delete(&1);
        assert_eq!(memtable.get(&1), None);
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

        let sorted: Vec<_> = memtable.iter().collect();
        assert_eq!(sorted[0].0, &1);
        assert_eq!(sorted[1].0, &2);
        assert_eq!(sorted[2].0, &3);

        let sorted: Vec<_> = memtable.into_sorted_vec();
        assert_eq!(sorted[0].0, 1);
        assert_eq!(sorted[1].0, 2);
        assert_eq!(sorted[2].0, 3);
    }
}
