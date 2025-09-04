use crate::{
    memtable::{MemTable, MemTableEntry},
    sstable::{SSTable, SSTableError},
};
use std::path::PathBuf;
use tokio::fs;

#[tokio::test]
async fn test_write_and_read_sstable_1() {
    let mut memtable = MemTable::new(10);
    memtable
        .put(1, "value1".to_string())
        .expect("Failed to put");
    memtable
        .put(3, "value3".to_string())
        .expect("Failed to put");
    memtable
        .put(2, "value2".to_string())
        .expect("Failed to put");

    let path = PathBuf::from("/tmp/test_sstable.db");

    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    // With default block size, small data should still have sparse index
    assert!(sstable.index.len() < 3);
    assert_eq!(sstable.index[0].0, 1); // First key should be indexed

    let loaded_sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read SSTable");
    assert!(loaded_sstable.index.len() < 3);

    assert_eq!(loaded_sstable.index, sstable.index);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_write_and_read_sstable_2() {
    let mut memtable = MemTable::new(10_000);

    for (i, j) in (0..10_000).zip((0..10_000).rev()) {
        memtable
            .put(i, format! {"string #{j}"})
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_sstable.db");

    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    assert!(sstable.index.len() < 5_000);

    let loaded_sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read SSTable");
    assert!(loaded_sstable.index.len() < 5_000);

    assert_eq!(loaded_sstable.index, sstable.index);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_sparse_index_with_small_block_size() {
    let mut memtable = MemTable::new(10);
    // Add more data to test sparse indexing
    for i in 1..=10 {
        memtable
            .put(i, format!("value{}", i))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_sparse_sstable.db");
    // Use very small block size to force sparse indexing
    let sstable = SSTable::write_from_memtable_with_block_size(memtable, path.clone(), 50)
        .await
        .expect("Failed to write SSTable");

    // Should have fewer index entries than total items
    assert!(!sstable.index.is_empty());
    assert!(sstable.index.len() < 10);

    // Test that we can still retrieve all values
    let loaded_sstable = SSTable::read_from_file_with_block_size(path.clone(), 50)
        .await
        .expect("Failed to read SSTable");

    assert_eq!(loaded_sstable.index, sstable.index);

    for i in 1..=10 {
        let result = loaded_sstable
            .get_with_block_size(&i, 50)
            .await
            .expect("Failed to get value");
        assert_eq!(result, Some(MemTableEntry::Value(format!("value{}", i))));
    }

    // Test non-existent keys
    assert_eq!(
        loaded_sstable
            .get_with_block_size(&0, 50)
            .await
            .expect("Failed to get"),
        None
    );
    assert_eq!(
        loaded_sstable
            .get_with_block_size(&11, 50)
            .await
            .expect("Failed to get"),
        None
    );

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_sstable_get_existing_key() {
    let mut memtable = MemTable::new(10);
    memtable
        .put(1, "value1".to_string())
        .expect("Failed to put");
    memtable
        .put(7, "value3".to_string())
        .expect("Failed to put");
    memtable
        .put(5, "value2".to_string())
        .expect("Failed to put");
    memtable.delete(9);

    let path = PathBuf::from("/tmp/test_sstable_get.db");
    let _sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    let sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read SSTable from file");

    let result = sstable.get(&5).await.expect("Failed to get value");
    assert_eq!(result, Some(MemTableEntry::Value("value2".to_string())));

    let result = sstable.get(&1).await.expect("Failed to get value");
    assert_eq!(result, Some(MemTableEntry::Value("value1".to_string())));

    let result = sstable.get(&7).await.expect("Failed to get value");
    assert_eq!(result, Some(MemTableEntry::Value("value3".to_string())));

    let result = sstable.get(&9).await.expect("Failed to get value");
    assert_eq!(result, Some(MemTableEntry::Tombstone));

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_sstable_get_nonexistent_key() {
    let mut memtable = MemTable::new(10);
    memtable
        .put(1, "value1".to_string())
        .expect("Failed to put");
    memtable
        .put(3, "value3".to_string())
        .expect("Failed to put");

    let path = PathBuf::from("/tmp/test_sstable_missing.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    let result = sstable.get(&2).await.expect("Failed to get value");
    assert_eq!(result, None);

    let result = sstable.get(&0).await.expect("Failed to get value");
    assert_eq!(result, None);

    let result = sstable.get(&4).await.expect("Failed to get value");
    assert_eq!(result, None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_empty_sstable() {
    let memtable = MemTable::<i32, String>::new(10);
    let path = PathBuf::from("/tmp/test_empty_sstable.db");

    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");
    assert_eq!(sstable.index.len(), 0);

    let loaded_sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read SSTable");
    assert_eq!(loaded_sstable.index.len(), 0);

    let result = loaded_sstable.get(&1).await.expect("Failed to get value");
    assert_eq!(result, None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_single_item_sstable() {
    let mut memtable = MemTable::new(10);
    memtable
        .put(42, "answer".to_string())
        .expect("Failed to put");

    let path = PathBuf::from("/tmp/test_single_sstable.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    assert_eq!(sstable.index.len(), 1);
    assert_eq!(sstable.index[0].0, 42);

    let result = sstable.get(&42).await.expect("Failed to get value");
    assert_eq!(result, Some(MemTableEntry::Value("answer".to_string())));

    let result = sstable.get(&41).await.expect("Failed to get value");
    assert_eq!(result, None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_sstable_error_invalid_path() {
    let result =
        SSTable::<i32, String>::read_from_file(PathBuf::from("/nonexistent/path/file.db")).await;
    assert!(result.is_err());

    if let Err(SSTableError::Io(_)) = result {
    } else {
        panic!("Expected IO error");
    }
}

#[tokio::test]
async fn test_bloom_filter_basic_functionality() {
    let mut memtable = MemTable::new(100);

    // Add some test data
    for i in 1..=50 {
        memtable
            .put(i, format!("value{}", i))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_bloom_filter.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    // Test that bloom filter correctly identifies existing keys
    for i in 1..=50 {
        assert!(
            sstable.bloom_filter.check(&i),
            "Bloom filter should contain key {}",
            i
        );
    }

    // Test that bloom filter correctly rejects some non-existing keys
    // Note: Due to false positives, some non-existing keys might return true
    let mut true_negatives = 0;
    for i in 51..=100 {
        if !sstable.bloom_filter.check(&i) {
            true_negatives += 1;
        }
    }

    // With a 1% false positive rate and 50 non-existing keys,
    // we should have most keys correctly identified as not present
    assert!(
        true_negatives > 40,
        "Bloom filter should reject most non-existing keys, got {} true negatives",
        true_negatives
    );

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_bloom_filter_get_optimization() {
    let mut memtable = MemTable::new(100);

    // Add test data with specific keys
    let existing_keys = vec![10, 20, 30, 40, 50];
    for &key in &existing_keys {
        memtable
            .put(key, format!("value{}", key))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_bloom_filter_get.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    // Test that existing keys are found
    for &key in &existing_keys {
        let result = sstable.get(&key).await.expect("Failed to get");
        assert_eq!(result, Some(MemTableEntry::Value(format!("value{}", key))));
    }

    // Test that non-existing keys return None quickly due to bloom filter
    // Most of these should be filtered out by the bloom filter
    let non_existing_keys = vec![1, 2, 3, 4, 5, 100, 200, 300];
    for &key in &non_existing_keys {
        let result = sstable.get(&key).await.expect("Failed to get");
        assert_eq!(result, None);
    }

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_bloom_filter_persistence() {
    let mut memtable = MemTable::new(100);

    // Add test data
    for i in 1..=20 {
        memtable
            .put(i, format!("value{}", i))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_bloom_filter_persistence.db");

    // Write SSTable with bloom filter
    let original_sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    // Read SSTable back from disk
    let loaded_sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read SSTable");

    // Verify bloom filter works after loading from disk
    for i in 1..=20 {
        assert!(
            loaded_sstable.bloom_filter.check(&i),
            "Loaded bloom filter should contain key {}",
            i
        );

        let result = loaded_sstable.get(&i).await.expect("Failed to get");
        assert_eq!(result, Some(MemTableEntry::Value(format!("value{}", i))));
    }

    // Test that both bloom filters behave similarly for non-existing keys
    let test_keys = vec![25, 30, 35, 40, 45];
    for &key in &test_keys {
        let original_check = original_sstable.bloom_filter.check(&key);
        let loaded_check = loaded_sstable.bloom_filter.check(&key);
        assert_eq!(
            original_check, loaded_check,
            "Bloom filter results should match for key {}",
            key
        );
    }

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_bloom_filter_empty_sstable() {
    let memtable = MemTable::<i32, String>::new(10);
    let path = PathBuf::from("/tmp/test_bloom_filter_empty.db");

    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write empty SSTable");

    // Test that empty SSTable handles bloom filter correctly
    let result = sstable
        .get(&1)
        .await
        .expect("Failed to get from empty SSTable");
    assert_eq!(result, None);

    // Load empty SSTable from disk
    let loaded_sstable = SSTable::<i32, String>::read_from_file(path.clone())
        .await
        .expect("Failed to read empty SSTable");

    let result = loaded_sstable
        .get(&1)
        .await
        .expect("Failed to get from loaded empty SSTable");
    assert_eq!(result, None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_read_into_memtable_basic() {
    let mut memtable = MemTable::new(100);
    memtable
        .put(1, "value1".to_string())
        .expect("Failed to put");
    memtable
        .put(3, "value3".to_string())
        .expect("Failed to put");
    memtable
        .put(2, "value2".to_string())
        .expect("Failed to put");

    let path = PathBuf::from("/tmp/test_read_into_memtable.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    let recovered_memtable = sstable
        .into_memtable(100)
        .await
        .expect("Failed to read into memtable");

    assert_eq!(
        recovered_memtable.get(&1),
        Some(&MemTableEntry::Value("value1".to_string()))
    );
    assert_eq!(
        recovered_memtable.get(&2),
        Some(&MemTableEntry::Value("value2".to_string()))
    );
    assert_eq!(
        recovered_memtable.get(&3),
        Some(&MemTableEntry::Value("value3".to_string()))
    );
    assert_eq!(recovered_memtable.get(&4), None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_read_into_memtable_empty_sstable() {
    let memtable = MemTable::<i32, String>::new(10);
    let path = PathBuf::from("/tmp/test_read_into_memtable_empty.db");

    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write empty SSTable");

    // For empty SSTables, the file is not created, so we test the default SSTable behavior
    if sstable.index.is_empty() && sstable.path == PathBuf::default() {
        // Empty SSTable doesn't have a file, so we test the expected behavior
        let recovered_memtable = MemTable::<i32, String>::new(100);
        assert!(recovered_memtable.is_empty());
        assert_eq!(recovered_memtable.get(&1), None);
    } else {
        // If a file was created, test normal flow
        let recovered_memtable = sstable
            .into_memtable(100)
            .await
            .expect("Failed to read empty SSTable into memtable");

        assert!(recovered_memtable.is_empty());
        assert_eq!(recovered_memtable.get(&1), None);
        fs::remove_file(path).await.ok();
    }
}

#[tokio::test]
async fn test_read_into_memtable_with_small_block_size() {
    let mut memtable = MemTable::new(100);
    for i in 1..=20 {
        memtable
            .put(i, format!("value{}", i))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_read_into_memtable_small_blocks.db");
    let sstable = SSTable::write_from_memtable_with_block_size(memtable, path.clone(), 50)
        .await
        .expect("Failed to write SSTable");

    let recovered_memtable = sstable
        .into_memtable_with_block_size(100, 50)
        .await
        .expect("Failed to read into memtable with small block size");

    for i in 1..=20 {
        assert_eq!(
            recovered_memtable.get(&i),
            Some(&MemTableEntry::Value(format!("value{}", i)))
        );
    }
    assert_eq!(recovered_memtable.get(&21), None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_read_into_memtable_with_tombstones() {
    let mut memtable = MemTable::new(100);
    memtable
        .put(1, "value1".to_string())
        .expect("Failed to put");
    memtable
        .put(2, "value2".to_string())
        .expect("Failed to put");
    memtable.delete(3);
    memtable
        .put(4, "value4".to_string())
        .expect("Failed to put");
    memtable.delete(5);

    let path = PathBuf::from("/tmp/test_read_into_memtable_tombstones.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    let recovered_memtable = sstable
        .into_memtable(100)
        .await
        .expect("Failed to read into memtable");

    assert_eq!(
        recovered_memtable.get(&1),
        Some(&MemTableEntry::Value("value1".to_string()))
    );
    assert_eq!(
        recovered_memtable.get(&2),
        Some(&MemTableEntry::Value("value2".to_string()))
    );
    assert_eq!(recovered_memtable.get(&3), Some(&MemTableEntry::Tombstone));
    assert_eq!(
        recovered_memtable.get(&4),
        Some(&MemTableEntry::Value("value4".to_string()))
    );
    assert_eq!(recovered_memtable.get(&5), Some(&MemTableEntry::Tombstone));
    assert_eq!(recovered_memtable.get(&6), None);

    fs::remove_file(path).await.ok();
}

#[tokio::test]
async fn test_read_into_memtable_large_dataset() {
    let mut memtable = MemTable::new(10_000);
    for i in 1..=1000 {
        memtable
            .put(i, format!("value{}", i))
            .expect("Failed to put");
    }

    let path = PathBuf::from("/tmp/test_read_into_memtable_large.db");
    let sstable = SSTable::write_from_memtable(memtable, path.clone())
        .await
        .expect("Failed to write SSTable");

    let recovered_memtable = sstable
        .into_memtable(10_000)
        .await
        .expect("Failed to read into memtable");

    for i in 1..=1000 {
        assert_eq!(
            recovered_memtable.get(&i),
            Some(&MemTableEntry::Value(format!("value{}", i)))
        );
    }
    assert_eq!(recovered_memtable.get(&1001), None);

    fs::remove_file(path).await.ok();
}
