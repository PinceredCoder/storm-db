use crate::{DBService, memtable::MemTableEntry, stormdb::MEMTABLE_CAPACITY};
use std::path::PathBuf;
use tokio::fs;

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
    fs::remove_dir_all("/tmp/test_db_basic").await.ok();
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
    fs::remove_dir_all("/tmp/test_db_delete").await.ok();
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
    fs::remove_dir_all("/tmp/test_db_overflow").await.ok();
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
    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_nonexistent_path() {
    let db_path = PathBuf::from("/tmp/test_db_nonexistent");

    // Ensure path doesn't exist
    fs::remove_dir_all(&db_path).await.ok();

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
    fs::remove_dir_all(&db_path).await.ok();
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
    fs::remove_dir_all("/tmp/test_db_delete_overflow")
        .await
        .ok();
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
    fs::remove_dir_all("/tmp/test_db_search_order").await.ok();
}

#[tokio::test]
async fn test_comprehensive_db_operations_with_disk_persistence() {
    let db_path = PathBuf::from("/tmp/test_comprehensive_db");

    // Cleanup any existing test data
    fs::remove_dir_all(&db_path).await.ok();

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
    }

    // Phase 3: Test multiple restart cycles
    {
        {
            // Shutdown again and restart to ensure persistence works multiple times
            let db = DBService::<i32, String>::read_from_fs(&db_path)
                .await
                .expect("Failed to read database from filesystem second time");

            db.put(4000, "value4000".to_string())
                .await
                .expect("Failed to put in second restart");
        }

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
    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_fs_loads_small_sstable_into_memtable() {
    let db_path = PathBuf::from("/tmp/test_db_small_sstable_to_memtable");
    fs::remove_dir_all(&db_path).await.ok();

    // Create database with small amount of data (less than MEMTABLE_CAPACITY)
    {
        let db = DBService::new(db_path.clone());

        // Add small amount of data
        for i in 0..100 {
            db.put(i, format!("value{}", i))
                .await
                .expect("Failed to put");
        }
    }

    // Read from filesystem - should load SSTable into memtable
    let db = DBService::<i32, String>::read_from_fs(&db_path)
        .await
        .expect("Failed to read from fs");

    // Verify no SSTables remain (loaded into memtable)
    let sstables = db.sstables.read().await;
    assert!(
        sstables.is_empty(),
        "Small SSTable should have been loaded into memtable"
    );

    // Verify data is accessible from memtable
    let memtable = db.memtable.read().await;
    assert!(!memtable.is_empty(), "Memtable should contain loaded data");

    for i in 0..100 {
        let result = db.get(&i).await.expect("Failed to get");
        assert_eq!(result, Some(format!("value{}", i)));
    }

    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_fs_keeps_large_sstable_as_sstable() {
    let db_path = PathBuf::from("/tmp/test_db_large_sstable_remains");
    fs::remove_dir_all(&db_path).await.ok();

    // Create database with data equal to MEMTABLE_CAPACITY
    {
        let db = DBService::new(db_path.clone());

        // Add exactly MEMTABLE_CAPACITY items
        for i in 0..MEMTABLE_CAPACITY {
            db.put(i as i32, format!("value{}", i))
                .await
                .expect("Failed to put");
        }
    }

    // Read from filesystem - should keep SSTable as SSTable
    let db = DBService::<i32, String>::read_from_fs(&db_path)
        .await
        .expect("Failed to read from fs");

    // Verify SSTable remains as SSTable
    let sstables = db.sstables.read().await;
    assert_eq!(sstables.len(), 1, "Large SSTable should remain as SSTable");
    assert_eq!(sstables[0].total_items() as usize, MEMTABLE_CAPACITY);

    // Verify memtable is empty
    let memtable = db.memtable.read().await;
    assert!(
        memtable.is_empty(),
        "Memtable should be empty when SSTable is large"
    );

    // Verify data is still accessible
    for i in 0..100 {
        let result = db.get(&i).await.expect("Failed to get");
        assert_eq!(result, Some(format!("value{}", i)));
    }

    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_fs_sstable_id_management() {
    let db_path = PathBuf::from("/tmp/test_db_sstable_id_management");
    fs::remove_dir_all(&db_path).await.ok();

    // Create database and add small SSTable
    {
        let db = DBService::new(db_path.clone());

        for i in 0..50 {
            db.put(i, format!("value{}", i))
                .await
                .expect("Failed to put");
        }
    }

    {
        // Read from filesystem (loads SSTable into memtable)
        let db = DBService::<i32, String>::read_from_fs(&db_path)
            .await
            .expect("Failed to read from fs");

        // Add more data to trigger new SSTable creation
        for i in 100..100 + MEMTABLE_CAPACITY {
            db.put(i as i32, format!("new_value{}", i))
                .await
                .expect("Failed to put");
        }
    }

    // Read again and verify SSTable ID is reused correctly
    let db = DBService::<i32, String>::read_from_fs(&db_path)
        .await
        .expect("Failed to read from fs second time");

    // Should have one SSTable with ID 0 (reused from loaded memtable)
    let sstables = db.sstables.read().await;
    assert_eq!(sstables.len(), 1, "Should have one SSTable after restart");

    // Verify all data is accessible
    for i in 0..50 {
        let result = db.get(&i).await.expect("Failed to get original data");
        assert_eq!(result, Some(format!("value{}", i)));
    }
    for i in 100..100 + MEMTABLE_CAPACITY {
        let result = db.get(&(i as i32)).await.expect("Failed to get new data");
        assert_eq!(result, Some(format!("new_value{}", i)));
    }

    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_fs_multiple_sstables_only_last_loaded() {
    let db_path = PathBuf::from("/tmp/test_db_multiple_sstables");
    fs::remove_dir_all(&db_path).await.ok();

    // Create database with multiple SSTables
    {
        let db = DBService::new(db_path.clone());

        // Create first large SSTable
        for i in 0..MEMTABLE_CAPACITY + 10 {
            db.put(i as i32, format!("first_value{}", i))
                .await
                .expect("Failed to put");
        }

        // Create second large SSTable
        for i in MEMTABLE_CAPACITY + 100..(2 * MEMTABLE_CAPACITY) + 100 {
            db.put(i as i32, format!("second_value{}", i))
                .await
                .expect("Failed to put");
        }

        // Add small amount to create small final SSTable
        for i in 5000..5050 {
            db.put(i, format!("small_value{}", i))
                .await
                .expect("Failed to put");
        }
    }

    // Read from filesystem
    let db = DBService::<i32, String>::read_from_fs(&db_path)
        .await
        .expect("Failed to read from fs");

    // Should have 2 SSTables remaining (first two large ones)
    let sstables = db.sstables.read().await;
    assert_eq!(sstables.len(), 2, "Should have 2 large SSTables remaining");

    // Verify memtable contains the small SSTable data
    let memtable = db.memtable.read().await;
    assert!(
        !memtable.is_empty(),
        "Memtable should contain small SSTable data"
    );

    // Verify all data is accessible
    for i in 0..10 {
        let result = db.get(&i).await.expect("Failed to get first SSTable data");
        assert_eq!(result, Some(format!("first_value{}", i)));
    }

    for i in MEMTABLE_CAPACITY + 100..MEMTABLE_CAPACITY + 110 {
        let result = db
            .get(&(i as i32))
            .await
            .expect("Failed to get second SSTable data");
        assert_eq!(result, Some(format!("second_value{}", i)));
    }

    for i in 5000..5050 {
        let result = db.get(&i).await.expect("Failed to get memtable data");
        assert_eq!(result, Some(format!("small_value{}", i)));
    }

    fs::remove_dir_all(&db_path).await.ok();
}

#[tokio::test]
async fn test_read_from_fs_empty_directory() {
    let db_path = PathBuf::from("/tmp/test_db_empty_directory");
    fs::remove_dir_all(&db_path).await.ok();

    // Read from non-existent directory
    let db = DBService::<i32, String>::read_from_fs(&db_path)
        .await
        .expect("Failed to read from empty directory");

    {
        // Should have empty state
        let sstables = db.sstables.read().await;
        assert!(sstables.is_empty(), "Should have no SSTables");

        let memtable = db.memtable.read().await;
        assert!(memtable.is_empty(), "Should have empty memtable");
    }

    // Should be able to add data
    db.put(1, "test".to_string()).await.expect("Failed to put");
    let result = db.get(&1).await.expect("Failed to get");
    assert_eq!(result, Some("test".to_string()));

    fs::remove_dir_all(&db_path).await.ok();
}
