use crate::memtable::{MemTable, MemTableEntry};

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
