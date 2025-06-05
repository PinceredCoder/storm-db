mod error;

#[cfg(test)]
mod tests;

pub(crate) use error::MemTableError;

use std::{borrow::Borrow, collections::BTreeMap};

pub(crate) struct MemTable<K, V> {
    data: BTreeMap<K, MemTableEntry<V>>,
    max_size: usize,
}

impl<K, V> Default for MemTable<K, V> {
    fn default() -> Self {
        Self {
            data: BTreeMap::default(),
            max_size: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub(crate) enum MemTableEntry<V> {
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

    pub fn from_iter<T: IntoIterator<Item = (K, MemTableEntry<V>)>>(
        iter: T,
        max_size: usize,
    ) -> Self {
        Self {
            data: BTreeMap::from_iter(iter),
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
