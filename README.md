# Storm DB

A high-performance, persistent key-value storage engine written in Rust, inspired by LSM-tree (Log-Structured Merge-tree) architecture.

## Features

- **In-Memory MemTable**: Fast writes with configurable capacity (default: 1MB)
- **Persistent SSTables**: Compressed, block-based storage for durability
- **Tombstone Deletion**: Efficient deletion with tombstone markers
- **Automatic Compaction**: MemTable automatically flushes to SSTable when full
- **Sparse Indexing**: Memory-efficient block indexing for fast lookups
- **Block Compression**: Uses deflate compression to reduce storage footprint
- **Bloom Filters**: Optimizes negative lookups by avoiding unnecessary disk reads
- **Async/Await Support**: Built with Tokio for high-performance async I/O
- **Generic Key-Value Types**: Supports any key-value types that implement required traits

## Architecture

Storm DB uses an LSM-tree inspired architecture with two main components:

1. **MemTable**: An in-memory sorted structure (BTreeMap) for recent writes
2. **SSTables**: Immutable, compressed files on disk for persistent storage

### Write Path

1. New writes go to the MemTable
2. When MemTable reaches capacity, it's flushed to a new SSTable
3. The MemTable is cleared and ready for new writes

### Read Path

1. First check the MemTable for the key
2. If not found, search SSTables in reverse chronological order (newest first)
3. For each SSTable, check the bloom filter first to avoid unnecessary disk reads
4. Return the first match found, or None if key doesn't exist

### Delete Operations

Deletes are handled using tombstone markers rather than immediate removal, ensuring consistency across the LSM-tree structure.

## Usage

```rust
use storm_db::DBService;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new database
    let db = DBService::new(PathBuf::from("./my_database"));
    
    // Insert key-value pairs
    db.put(1, "Hello".to_string()).await?;
    db.put(2, "World".to_string()).await?;
    
    // Read values
    let value = db.get(&1).await?;
    assert_eq!(value, Some("Hello".to_string()));
    
    // Update values
    db.put(1, "Hi".to_string()).await?;
    let updated = db.get(&1).await?;
    assert_eq!(updated, Some("Hi".to_string()));
    
    // Delete values
    db.delete(2).await?;
    let deleted = db.get(&2).await?;
    assert_eq!(deleted, None);
    
    // Persist any remaining MemTable data
    db.shutdown().await?;
    
    // Later, read from disk
    let db = DBService::<i32, String>::read_from_fs("./my_database").await?;
    let persisted = db.get(&1).await?;
    assert_eq!(persisted, Some("Hi".to_string()));
    
    Ok(())
}
```

## Storage Format

### MemTable

- In-memory BTreeMap with configurable capacity
- Stores both values and tombstone markers
- Automatically flushes to SSTable when full

### SSTable Format

- **Block-based**: Data is organized into compressed blocks (default: 4KB)
- **Sparse Index**: Memory-efficient indexing of block start keys
- **Bloom Filters**: In-memory probabilistic data structure for fast negative lookups (1% false positive rate)
- **Compression**: Deflate compression for reduced storage size
- **Binary Encoding**: Uses bincode for efficient serialization

File structure:

```
[Block 1: compressed_size][Block 1: compressed_data]
[Block 2: compressed_size][Block 2: compressed_data]
...
```

Each block contains multiple key-value pairs in sorted order.

## Type Requirements

Keys and values must implement:

- `Ord` for sorting
- `Hash` for bloom filter operations
- `Clone` for internal operations  
- `bincode::Encode` and `bincode::Decode` for serialization

## Performance Characteristics

- **Writes**: O(log n) to MemTable, amortized O(1) with batch flushes
- **Reads**: O(log n) for MemTable + O(1) bloom filter check + O(log b) for SSTable block lookup where b is blocks per SSTable
- **Negative Lookups**: O(1) for most non-existent keys thanks to bloom filters
- **Space**: Compressed storage with sparse indexing and bloom filters minimizes memory usage
- **Durability**: Configurable flush-to-disk on shutdown or MemTable overflow

## Testing

Run the test suite:

```bash
cargo test
```

The comprehensive test suite covers:

- Basic put/get/delete operations
- MemTable overflow and SSTable creation
- Disk persistence and recovery
- Delete operations across MemTable/SSTable boundaries
- Bloom filter functionality and optimization
- Multiple restart cycles

## Roadmap

- [ ] Write-Ahead Log (WAL) for crash recovery
- [ ] SSTable merging and compaction
- [x] Bloom filters for faster negative lookups
- [ ] Range queries and iterators
- [ ] Configurable compression algorithms
- [ ] Metrics and monitoring

## Dependencies

- `tokio`: Async runtime and I/O
- `bincode`: Fast binary serialization
- `bloomfilter`: Probabilistic data structure for fast negative lookups
- `flate2`: Compression support
- `thiserror`: Error handling
