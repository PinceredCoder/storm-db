use crate::{
    memtable::{MemTable, MemTableEntry},
    sstable::SSTable,
    stormdb::error::DBServiceError,
};
use std::{
    ffi,
    hash::Hash,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU16, Ordering},
};
use tokio::{fs, sync::RwLock};

pub mod error;

#[cfg(test)]
mod tests;

const MEMTABLE_CAPACITY: usize = 2usize.pow(20);
const SSTABLE_PREFIX: &str = "sstable_";
const SSTABLE_EXTENSION: &str = ".db";

/*
    TODO:
    1. Add write-ahead log (associate WAL with memtable?)
    1.1 Write tests
    2. Optimize put and delete (e. g. create SSTable in a separate tokio task)
*/

pub struct DBService<K: Ord + bincode::Encode, V: bincode::Encode> {
    memtable: RwLock<MemTable<K, V>>,
    sstables: RwLock<Vec<SSTable<K, V>>>,
    next_sstable_id: AtomicU16,
    path_to_db: PathBuf,
}

impl<K: Ord + bincode::Encode, V: bincode::Encode> Drop for DBService<K, V> {
    fn drop(&mut self) {
        self.shutdown().expect("failed shutting down");
    }
}

impl<K: Ord + bincode::Encode, V: bincode::Encode> DBService<K, V> {
    pub fn new(path_to_db: PathBuf) -> Self {
        Self {
            memtable: RwLock::new(MemTable::new(MEMTABLE_CAPACITY)),
            sstables: RwLock::new(Vec::new()),
            next_sstable_id: AtomicU16::new(0),
            path_to_db,
        }
    }

    pub async fn read_from_fs<P: AsRef<Path>>(path_to_db: P) -> Result<Self, DBServiceError>
    where
        K: Hash + bincode::Decode<()>,
        V: bincode::Decode<()>,
    {
        if !path_to_db.as_ref().exists() {
            fs::create_dir_all(&path_to_db).await?;
            return Ok(Self::new(path_to_db.as_ref().to_path_buf()));
        }

        let mut sstable_files = Vec::new();
        let mut max_id = 0;

        let mut read_dir = fs::read_dir(&path_to_db).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();

            if let Some(file_name) = path.file_name().and_then(ffi::OsStr::to_str) {
                if file_name.starts_with(SSTABLE_PREFIX) && file_name.ends_with(SSTABLE_EXTENSION) {
                    let id_part =
                        &file_name[SSTABLE_PREFIX.len()..file_name.len() - SSTABLE_EXTENSION.len()];
                    if let Ok(id) = id_part.parse::<u16>() {
                        max_id = max_id.max(id);
                        sstable_files.push((id, path));
                    }
                }
            }
        }

        sstable_files.sort_by_key(|&(id, _)| id);

        let mut sstables = Vec::new();
        for (_id, path) in sstable_files {
            let sstable = SSTable::read_from_file(path).await?;
            sstables.push(sstable);
        }

        let (memtable, next_sstable_id) = if !sstables.is_empty()
            && (sstables.last().unwrap().total_items() as usize) < MEMTABLE_CAPACITY
        {
            (
                sstables
                    .pop()
                    .unwrap()
                    .read_into_memtable(MEMTABLE_CAPACITY)
                    .await?,
                max_id,
            )
        } else {
            (MemTable::new(MEMTABLE_CAPACITY), max_id + 1)
        };

        Ok(Self {
            memtable: RwLock::new(memtable),
            sstables: RwLock::new(sstables),
            next_sstable_id: AtomicU16::new(next_sstable_id),
            path_to_db: path_to_db.as_ref().to_path_buf(),
        })
    }

    fn shutdown(&self) -> Result<(), DBServiceError> {
        let memtable = {
            let mut memtable_guard = self.memtable.try_write()?;
            std::mem::replace(&mut *memtable_guard, MemTable::new(0))
        };

        // Ensure directory exists
        if !self.path_to_db.exists() {
            std::fs::create_dir_all(&self.path_to_db)?;
        }

        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .path_to_db
            .join(format!("{SSTABLE_PREFIX}{sstable_id}{SSTABLE_EXTENSION}"));

        SSTable::flush_memtable_sync(memtable, path)?;

        Ok(())
    }

    pub async fn put(&self, key: K, value: V) -> Result<(), DBServiceError>
    where
        K: Hash + Clone,
    {
        let mut memtable = self.memtable.write().await;

        if memtable.is_full() {
            let to_flush = std::mem::replace(&mut *memtable, MemTable::new(MEMTABLE_CAPACITY));
            self.flush_memtable(to_flush).await?;
        }

        memtable.put(key, value)?;
        Ok(())
    }

    async fn flush_memtable(&self, memtable: MemTable<K, V>) -> Result<(), DBServiceError>
    where
        K: Hash + Clone,
    {
        let sstable = self.write_memtable_to_sstable(memtable).await?;
        let mut sstables = self.sstables.write().await;
        sstables.push(sstable);

        Ok(())
    }

    async fn write_memtable_to_sstable(
        &self,
        memtable: MemTable<K, V>,
    ) -> Result<SSTable<K, V>, DBServiceError>
    where
        K: Hash + Clone,
    {
        // Ensure directory exists
        if !self.path_to_db.exists() {
            fs::create_dir_all(&self.path_to_db).await?;
        }

        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .path_to_db
            .join(format!("{SSTABLE_PREFIX}{sstable_id}{SSTABLE_EXTENSION}"));

        Ok(SSTable::write_from_memtable(memtable, path).await?)
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>, DBServiceError>
    where
        K: Hash + bincode::Decode<()>,
        V: Clone + bincode::Decode<()>,
    {
        {
            let memtable = self.memtable.read().await;
            if let Some(entry) = memtable.get(key) {
                return match entry {
                    MemTableEntry::Value(v) => Ok(Some(v.clone())),
                    MemTableEntry::Tombstone => Ok(None),
                };
            }
        }

        let sstables = self.sstables.read().await;
        for sstable in sstables.iter().rev() {
            if let Some(value) = sstable.get(key).await? {
                return match value {
                    MemTableEntry::Value(v) => Ok(Some(v)),
                    MemTableEntry::Tombstone => Ok(None),
                };
            }
        }

        Ok(None)
    }

    pub async fn delete(&self, key: K) -> Result<(), DBServiceError>
    where
        K: Clone + std::hash::Hash + bincode::Encode,
        V: bincode::Encode,
    {
        let mut memtable = self.memtable.write().await;

        if memtable.is_full() {
            let to_flush = std::mem::replace(&mut *memtable, MemTable::new(MEMTABLE_CAPACITY));
            self.flush_memtable(to_flush).await?;
        }

        memtable.delete(key);
        Ok(())
    }
}
