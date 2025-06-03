mod memtable;
mod sstable;

use memtable::{MemTable, MemTableError};
use sstable::{SSTable, SSTableError};
use std::{
    borrow::Borrow,
    ffi,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU16, Ordering},
};
use tokio::{fs, sync::RwLock};

use crate::memtable::MemTableEntry;

const MEMTABLE_CAPACITY: usize = 2usize.pow(20);
const SSTABLE_PREFIX: &str = "sstable_";
const SSTABLE_EXTENSION: &str = ".db";

/*
    TODO:
    1. Add write-ahead log
    1.1 Write tests
    2. Load the last SSTable into Memtable when reading from file system
*/

pub struct DBService<K, V> {
    memtable: RwLock<MemTable<K, V>>,
    sstables: RwLock<Vec<SSTable<K, V>>>,
    next_sstable_id: AtomicU16,
    path_to_db: PathBuf,
}

impl<K: Ord, V> DBService<K, V> {
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
        K: Clone + bincode::Encode + bincode::Decode<()>,
        V: bincode::Encode + bincode::Decode<()>,
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

        Ok(Self {
            memtable: RwLock::new(MemTable::new(MEMTABLE_CAPACITY)),
            sstables: RwLock::new(sstables),
            next_sstable_id: AtomicU16::new(max_id + 1),
            path_to_db: path_to_db.as_ref().to_path_buf(),
        })
    }

    pub async fn shutdown(&self) -> Result<(), DBServiceError>
    where
        K: Clone + bincode::Encode,
        V: bincode::Encode,
    {
        let memtable = {
            let mut memtable_guard = self.memtable.write().await;
            std::mem::replace(&mut *memtable_guard, MemTable::new(0))
        };

        if !memtable.is_empty() {
            self.write_memtable_to_sstable(memtable).await?;
        }

        Ok(())
    }

    pub async fn put(&self, key: K, value: V) -> Result<(), DBServiceError>
    where
        K: Clone + bincode::Encode,
        V: bincode::Encode,
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
        K: Clone + bincode::Encode,
        V: bincode::Encode,
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
        K: Clone + bincode::Encode,
        V: bincode::Encode,
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

    pub async fn get<Q: Ord>(&self, key: &Q) -> Result<Option<V>, DBServiceError>
    where
        K: bincode::Decode<()> + Borrow<Q>,
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
        K: Clone + bincode::Encode,
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

#[derive(Debug, thiserror::Error)]
pub enum DBServiceError {
    #[error("memtable error: {0}")]
    MemTable(#[from] MemTableError),
    #[error("sstable error: {0}")]
    SSTable(#[from] SSTableError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_basic_put_get() {
        let db = DBService::new(PathBuf::from("/tmp/test_db_basic"));

        db.put(1, "value1".to_string())
            .await
            .expect("Failed to put");
        db.put(2, "value2".to_string())
            .await
            .expect("Failed to put");

        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, Some("value1".to_string()));

        let result = db.get(&2).await.expect("Failed to get");
        assert_eq!(result, Some("value2".to_string()));

        let result = db.get(&3).await.expect("Failed to get");
        assert_eq!(result, None);

        // Cleanup
        fs::remove_dir_all("/tmp/test_db_basic").ok();
    }

    #[tokio::test]
    async fn test_delete() {
        let db = DBService::new(PathBuf::from("/tmp/test_db_delete"));

        db.put(1, "value1".to_string())
            .await
            .expect("Failed to put");
        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, Some("value1".to_string()));

        db.delete(1).await.expect("Failed to delete");
        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, None);

        // Cleanup
        fs::remove_dir_all("/tmp/test_db_delete").ok();
    }

    #[tokio::test]
    async fn test_memtable_overflow() {
        let db = DBService::new(PathBuf::from("/tmp/test_db_overflow"));

        // Fill memtable beyond capacity to trigger flush
        for i in 0..MEMTABLE_CAPACITY + 10 {
            db.put(i, format!("value{}", i))
                .await
                .expect("Failed to put");
        }

        // Verify data is still accessible
        for i in 0..10 {
            let result = db.get(&i).await.expect("Failed to get");
            assert_eq!(result, Some(format!("value{}", i)));
        }

        // Check that SSTable was created
        let sstables = db.sstables.read().await;
        assert!(!sstables.is_empty(), "SSTable should have been created");

        // Cleanup
        fs::remove_dir_all("/tmp/test_db_overflow").ok();
    }

    #[tokio::test]
    async fn test_read_from_fs() {
        let db_path = PathBuf::from("/tmp/test_db_fs");

        // Create initial database and add small amount of data to trigger one SSTable
        {
            let db = DBService::new(db_path.clone());

            // Add exactly enough data to trigger one SSTable flush
            for i in 0..MEMTABLE_CAPACITY + 1 {
                db.put(i, format!("value{}", i))
                    .await
                    .expect("Failed to put");
            }

            // Verify we can read the data before shutdown
            let result = db.get(&100).await.expect("Failed to get");
            assert_eq!(result, Some("value100".to_string()));

            // Verify SSTable was created during the overflow
            let sstables = db.sstables.read().await;
            assert!(
                !sstables.is_empty(),
                "SSTable should have been created during overflow"
            );

            let result = sstables
                .last()
                .unwrap()
                .get(&100)
                .await
                .expect("failed to get");
            assert_eq!(result, Some(MemTableEntry::Value("value100".to_string())));

            db.shutdown()
                .await
                .expect("failed writing memtable to disk");
        }

        // Read database from filesystem
        let db = DBService::<usize, String>::read_from_fs(&db_path)
            .await
            .expect("Failed to read from fs");

        // First, verify the SSTable was loaded
        let sstables = db.sstables.read().await;
        assert!(!sstables.is_empty(), "SSTables should have been loaded");

        // Test keys that should be in the SSTable (0 to MEMTABLE_CAPACITY-1)
        // After restart, only data in SSTables persists, memtable data is lost
        let result = db.get(&100).await.expect("Failed to get");
        assert_eq!(result, Some("value100".to_string()));

        let result = db.get(&1000).await.expect("Failed to get");
        assert_eq!(result, Some("value1000".to_string()));

        // Cleanup
        fs::remove_dir_all(&db_path).ok();
    }

    #[tokio::test]
    async fn test_read_from_nonexistent_path() {
        let db_path = PathBuf::from("/tmp/test_db_nonexistent");

        // Ensure path doesn't exist
        fs::remove_dir_all(&db_path).ok();

        let db = DBService::<i32, String>::read_from_fs(&db_path)
            .await
            .expect("Failed to read from fs");

        // Should create empty database
        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, None);

        // Should be able to add data
        db.put(1, "test".to_string()).await.expect("Failed to put");
        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, Some("test".to_string()));

        // Cleanup
        fs::remove_dir_all(&db_path).ok();
    }

    #[tokio::test]
    async fn test_delete_with_overflow() {
        let db = DBService::new(PathBuf::from("/tmp/test_db_delete_overflow"));

        // Fill memtable to exactly capacity
        for i in 0..MEMTABLE_CAPACITY {
            db.put(i, format!("value{}", i))
                .await
                .expect("Failed to put");
        }

        // Delete should trigger flush when memtable is full
        db.delete(999).await.expect("Failed to delete");

        // Verify SSTable was created
        let sstables = db.sstables.read().await;
        assert!(
            !sstables.is_empty(),
            "SSTable should have been created by delete"
        );

        // Cleanup
        fs::remove_dir_all("/tmp/test_db_delete_overflow").ok();
    }

    #[tokio::test]
    async fn test_sstable_search_order() {
        let db = DBService::new(PathBuf::from("/tmp/test_db_search_order"));

        // Add initial value
        db.put(1, "old_value".to_string())
            .await
            .expect("Failed to put");

        // Fill memtable to force flush
        for i in 2..MEMTABLE_CAPACITY + 2 {
            db.put(i, format!("value{}", i))
                .await
                .expect("Failed to put");
        }

        // Update the same key in new memtable
        db.put(1, "new_value".to_string())
            .await
            .expect("Failed to put");

        // Should get newer value from memtable, not SSTable
        let result = db.get(&1).await.expect("Failed to get");
        assert_eq!(result, Some("new_value".to_string()));

        // Cleanup
        fs::remove_dir_all("/tmp/test_db_search_order").ok();
    }

    #[tokio::test]
    async fn test_comprehensive_db_operations_with_disk_persistence() {
        let db_path = PathBuf::from("/tmp/test_comprehensive_db");

        // Cleanup any existing test data
        fs::remove_dir_all(&db_path).ok();

        // Phase 1: Test basic operations in memory
        {
            let db = DBService::<i32, String>::new(db_path.clone());

            // Test put operations
            db.put(1, "value1".to_string())
                .await
                .expect("Failed to put key 1");
            db.put(10, "value10".to_string())
                .await
                .expect("Failed to put key 10");
            db.put(5, "value5".to_string())
                .await
                .expect("Failed to put key 5");
            db.put(100, "value100".to_string())
                .await
                .expect("Failed to put key 100");

            // Test get operations
            assert_eq!(
                db.get(&1).await.expect("Failed to get key 1"),
                Some("value1".to_string())
            );
            assert_eq!(
                db.get(&10).await.expect("Failed to get key 10"),
                Some("value10".to_string())
            );
            assert_eq!(
                db.get(&5).await.expect("Failed to get key 5"),
                Some("value5".to_string())
            );
            assert_eq!(
                db.get(&100).await.expect("Failed to get key 100"),
                Some("value100".to_string())
            );
            assert_eq!(
                db.get(&999).await.expect("Failed to get non-existent key"),
                None
            );

            // Test update operations
            db.put(1, "updated_value1".to_string())
                .await
                .expect("Failed to update key 1");
            assert_eq!(
                db.get(&1).await.expect("Failed to get updated key 1"),
                Some("updated_value1".to_string())
            );

            // Test delete operations
            db.delete(5).await.expect("Failed to delete key 5");
            assert_eq!(db.get(&5).await.expect("Failed to get deleted key 5"), None);

            // Test that other keys are still accessible after delete
            assert_eq!(
                db.get(&1).await.expect("Failed to get key 1 after delete"),
                Some("updated_value1".to_string())
            );
            assert_eq!(
                db.get(&10)
                    .await
                    .expect("Failed to get key 10 after delete"),
                Some("value10".to_string())
            );

            // Add many entries to trigger memtable flush to SSTable
            for i in 200..200 + MEMTABLE_CAPACITY + 50 {
                db.put(i as i32, format!("bulk_value{}", i))
                    .await
                    .unwrap_or_else(|_| panic!("Failed to put bulk key {}", i));
            }

            // Verify SSTable was created
            let sstables = db.sstables.read().await;
            assert!(
                !sstables.is_empty(),
                "SSTable should have been created from memtable overflow"
            );

            // Test that we can still read values from both memtable and SSTable
            assert_eq!(
                db.get(&1).await.expect("Failed to get key 1 from SSTable"),
                Some("updated_value1".to_string())
            );
            assert_eq!(
                db.get(&200)
                    .await
                    .expect("Failed to get bulk key from SSTable"),
                Some("bulk_value200".to_string())
            );
            assert_eq!(
                db.get(&210)
                    .await
                    .expect("Failed to get bulk key from memtable"),
                Some("bulk_value210".to_string())
            );

            // Test delete of key in SSTable
            db.delete(1)
                .await
                .expect("Failed to delete key 1 from SSTable");
            assert_eq!(
                db.get(&1).await.expect("Failed to get deleted SSTable key"),
                None
            );

            // Add a few more entries to ensure memtable has data before shutdown
            db.put(2000, "value2000".to_string())
                .await
                .expect("Failed to put key 2000");
            db.put(2001, "value2001".to_string())
                .await
                .expect("Failed to put key 2001");

            // Shutdown to persist memtable to disk
            db.shutdown().await.expect("Failed to shutdown database");
        }

        // Phase 2: Test reading from disk after restart
        {
            let db = DBService::<i32, String>::read_from_fs(&db_path)
                .await
                .expect("Failed to read database from filesystem");

            // Verify SSTable data is still accessible
            assert_eq!(
                db.get(&10).await.expect("Failed to get key 10 from disk"),
                Some("value10".to_string())
            );
            assert_eq!(
                db.get(&100).await.expect("Failed to get key 100 from disk"),
                Some("value100".to_string())
            );
            assert_eq!(
                db.get(&200)
                    .await
                    .expect("Failed to get bulk key from disk"),
                Some("bulk_value200".to_string())
            );

            // Verify deleted keys remain deleted
            assert_eq!(
                db.get(&1)
                    .await
                    .expect("Failed to get deleted key 1 from disk"),
                None
            );
            assert_eq!(
                db.get(&5)
                    .await
                    .expect("Failed to get deleted key 5 from disk"),
                None
            );

            // Verify memtable data that was persisted during shutdown is accessible
            assert_eq!(
                db.get(&2000)
                    .await
                    .expect("Failed to get key 2000 from disk"),
                Some("value2000".to_string())
            );
            assert_eq!(
                db.get(&2001)
                    .await
                    .expect("Failed to get key 2001 from disk"),
                Some("value2001".to_string())
            );

            // Test new operations after restart
            db.put(3000, "value3000_after_restart".to_string())
                .await
                .expect("Failed to put after restart");
            assert_eq!(
                db.get(&3000)
                    .await
                    .expect("Failed to get new key after restart"),
                Some("value3000_after_restart".to_string())
            );

            // Test update of existing persisted data
            db.put(100, "updated_value100_after_restart".to_string())
                .await
                .expect("Failed to update after restart");
            assert_eq!(
                db.get(&100)
                    .await
                    .expect("Failed to get updated key after restart"),
                Some("updated_value100_after_restart".to_string())
            );

            // Test delete after restart
            db.delete(10).await.expect("Failed to delete after restart");
            assert_eq!(
                db.get(&10)
                    .await
                    .expect("Failed to get deleted key after restart"),
                None
            );

            // Verify SSTable search order: newer entries should override older ones
            let original_key = 200;
            db.put(original_key, "newer_value".to_string())
                .await
                .expect("Failed to update existing SSTable key");
            assert_eq!(
                db.get(&original_key)
                    .await
                    .expect("Failed to get updated SSTable key"),
                Some("newer_value".to_string())
            );

            db.shutdown()
                .await
                .expect("failed writting memtable to disk");
        }

        // Phase 3: Test multiple restart cycles
        {
            // Shutdown again and restart to ensure persistence works multiple times
            let db = DBService::<i32, String>::read_from_fs(&db_path)
                .await
                .expect("Failed to read database from filesystem second time");

            db.put(4000, "value4000".to_string())
                .await
                .expect("Failed to put in second restart");
            db.shutdown()
                .await
                .expect("Failed to shutdown database second time");

            let db = DBService::<i32, String>::read_from_fs(&db_path)
                .await
                .expect("Failed to read database from filesystem third time");

            assert_eq!(
                db.get(&4000)
                    .await
                    .expect("Failed to get key from second restart cycle"),
                Some("value4000".to_string())
            );
            assert_eq!(
                db.get(&3000)
                    .await
                    .expect("Failed to get key from first restart"),
                Some("value3000_after_restart".to_string())
            );
            assert_eq!(
                db.get(&100)
                    .await
                    .expect("Failed to get updated key from first restart"),
                Some("updated_value100_after_restart".to_string())
            );
        }

        // Cleanup
        fs::remove_dir_all(&db_path).ok();
    }

    #[ignore]
    #[test]
    fn get_tests_coverage() {
        let target_dir = std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.parent() // remove executable name
                    .and_then(|p| p.parent()) // remove 'deps'
                    .and_then(|p| p.parent()) // remove 'debug' or 'release'
                    .map(|p| p.to_path_buf())
            })
            .unwrap();

        let output = std::process::Command::new("cargo")
            .arg("llvm-cov")
            .arg("--lcov")
            .arg("--output-path")
            .arg(format!("{}/lcov.info", target_dir.display()))
            .output()
            .expect("failed to execute process");

        println!("STDOUT:\n{}", String::from_utf8(output.stdout).unwrap());
        println!("STDERR:\n{}", String::from_utf8(output.stderr).unwrap());
    }
}
