use std::{
    borrow::Borrow,
    fs,
    io::{self, Seek},
    marker::PhantomData,
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use crate::memtable::MemTable;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

// TODO: add compression for blocks
const DEFAULT_BLOCK_SIZE: u64 = 4 * 1024; // 4KB blocks

struct SSTable<K, V> {
    path: PathBuf,
    index: Vec<(K, u64)>, // Sparse index: only every Nth key
    _phantom: PhantomData<V>,
}

impl<K: Ord + Clone + bincode::Encode, V: bincode::Encode> SSTable<K, V> {
    pub fn write_from_memtable(
        memtable: MemTable<K, V>,
        path: PathBuf,
    ) -> Result<Self, SSTableError> {
        Self::write_from_memtable_with_block_size(memtable, path, DEFAULT_BLOCK_SIZE)
    }

    pub fn write_from_memtable_with_block_size(
        memtable: MemTable<K, V>,
        path: PathBuf,
        block_size: u64,
    ) -> Result<Self, SSTableError> {
        let mut file = fs::File::create(&path)?;
        let mut index = Vec::new();

        let sorted_items = memtable.into_sorted_vec();
        let mut offset = 0;
        let mut bytes_since_last_index = 0;
        let mut first_item = true;

        for (k, v) in sorted_items.into_iter() {
            if first_item || bytes_since_last_index >= block_size {
                index.push((k.clone(), offset));
                bytes_since_last_index = 0;
            }

            let bytes_written =
                bincode::encode_into_std_write((k, v), &mut file, BINCODE_CONFIG)? as u64;
            offset += bytes_written;
            bytes_since_last_index += bytes_written;
            first_item = false;
        }

        Ok(Self {
            path,
            index,
            _phantom: PhantomData,
        })
    }
}

impl<K: bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub fn read_from_file(path: PathBuf) -> Result<Self, SSTableError> {
        Self::read_from_file_with_block_size(path, DEFAULT_BLOCK_SIZE)
    }

    pub fn read_from_file_with_block_size(
        path: PathBuf,
        block_size: u64,
    ) -> Result<Self, SSTableError> {
        let mut file = fs::File::open(&path)?;

        let file_size = file.metadata()?.size();

        let mut index = Vec::with_capacity((file_size / block_size) as usize);
        let mut offset = 0;
        let mut bytes_since_last_index = 0;
        let mut first_item = true;

        loop {
            match bincode::decode_from_std_read::<(K, V), _, _>(&mut file, BINCODE_CONFIG) {
                Ok((key, _value)) => {
                    if first_item || bytes_since_last_index >= block_size {
                        index.push((key, offset));
                        bytes_since_last_index = 0;
                    }

                    let new_offset = file.stream_position()?;
                    let bytes_read = new_offset - offset;
                    bytes_since_last_index += bytes_read;
                    offset = new_offset;
                    first_item = false;
                }
                Err(e) => match e {
                    bincode::error::DecodeError::UnexpectedEnd { .. } => break,
                    bincode::error::DecodeError::Io { inner, .. }
                        if inner.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break;
                    }
                    e => return Err(e.into()),
                },
            }
        }

        Ok(Self {
            path,
            index,
            _phantom: PhantomData,
        })
    }
}

impl<K: Ord + bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub fn get<Q: Borrow<K>>(&self, key: &Q) -> Result<Option<V>, SSTableError> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let search_result = self.index.binary_search_by(|(k, _)| k.cmp(key.borrow()));

        let (start_offset, end_offset_opt) = match search_result {
            Ok(exact_idx) => {
                let start_offset = self.index[exact_idx].1;
                let end_offset_opt = self.index.get(exact_idx + 1).map(|(_, offset)| *offset);
                (start_offset, end_offset_opt)
            }
            Err(insert_idx) => {
                if insert_idx == 0 {
                    return Ok(None);
                }

                let start_idx = insert_idx - 1;
                let start_offset = self.index[start_idx].1;
                let end_offset_opt = self.index.get(insert_idx).map(|(_, offset)| *offset);
                (start_offset, end_offset_opt)
            }
        };

        let mut file = fs::File::open(&self.path)?;
        file.seek(io::SeekFrom::Start(start_offset))?;

        loop {
            let current_offset = file.stream_position()?;

            if end_offset_opt.is_some_and(|end_offset| current_offset >= end_offset) {
                break Ok(None);
            }

            match bincode::decode_from_std_read::<(K, V), _, _>(&mut file, BINCODE_CONFIG) {
                Ok((decoded_key, value)) => match decoded_key.cmp(key.borrow()) {
                    std::cmp::Ordering::Equal => return Ok(Some(value)),
                    std::cmp::Ordering::Greater => return Ok(None),
                    std::cmp::Ordering::Less => continue,
                },
                Err(e) => match e {
                    bincode::error::DecodeError::UnexpectedEnd { .. } => break Ok(None),
                    bincode::error::DecodeError::Io { inner, .. }
                        if inner.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break Ok(None);
                    }
                    e => break Err(e.into()),
                },
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SSTableError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encoding error: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("decoding error: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_write_and_read_sstable() {
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

        let sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");

        // With default block size, small data should still have sparse index
        assert!(sstable.index.len() <= 3);
        assert_eq!(sstable.index[0].0, 1); // First key should be indexed

        let loaded_sstable =
            SSTable::<i32, String>::read_from_file(path.clone()).expect("Failed to read SSTable");
        assert!(loaded_sstable.index.len() <= 3);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_sparse_index_with_small_block_size() {
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
            .expect("Failed to write SSTable");

        // Should have fewer index entries than total items
        assert!(sstable.index.len() < 10);
        assert!(sstable.index.len() >= 1);

        // Test that we can still retrieve all values
        let loaded_sstable = SSTable::read_from_file_with_block_size(path.clone(), 50)
            .expect("Failed to read SSTable");

        for i in 1..=10 {
            let result = loaded_sstable.get(&i).expect("Failed to get value");
            assert_eq!(result, Some(format!("value{}", i)));
        }

        // Test non-existent keys
        assert_eq!(loaded_sstable.get(&0).expect("Failed to get"), None);
        assert_eq!(loaded_sstable.get(&11).expect("Failed to get"), None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_sstable_get_existing_key() {
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

        let path = PathBuf::from("/tmp/test_sstable_get.db");
        let _sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");

        let sstable =
            SSTable::read_from_file(path.clone()).expect("Failed to read SSTable from file");

        let result = sstable.get(&5).expect("Failed to get value");
        assert_eq!(result, Some("value2".to_string()));

        let result = sstable.get(&1).expect("Failed to get value");
        assert_eq!(result, Some("value1".to_string()));

        let result = sstable.get(&7).expect("Failed to get value");
        assert_eq!(result, Some("value3".to_string()));

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_sstable_get_nonexistent_key() {
        let mut memtable = MemTable::new(10);
        memtable
            .put(1, "value1".to_string())
            .expect("Failed to put");
        memtable
            .put(3, "value3".to_string())
            .expect("Failed to put");

        let path = PathBuf::from("/tmp/test_sstable_missing.db");
        let sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");

        let result = sstable.get(&2).expect("Failed to get value");
        assert_eq!(result, None);

        let result = sstable.get(&0).expect("Failed to get value");
        assert_eq!(result, None);

        let result = sstable.get(&4).expect("Failed to get value");
        assert_eq!(result, None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_empty_sstable() {
        let memtable = MemTable::<i32, String>::new(10);
        let path = PathBuf::from("/tmp/test_empty_sstable.db");

        let sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");
        assert_eq!(sstable.index.len(), 0);

        let loaded_sstable =
            SSTable::<i32, String>::read_from_file(path.clone()).expect("Failed to read SSTable");
        assert_eq!(loaded_sstable.index.len(), 0);

        let result = loaded_sstable.get(&1).expect("Failed to get value");
        assert_eq!(result, None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_single_item_sstable() {
        let mut memtable = MemTable::new(10);
        memtable
            .put(42, "answer".to_string())
            .expect("Failed to put");

        let path = PathBuf::from("/tmp/test_single_sstable.db");
        let sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");

        assert_eq!(sstable.index.len(), 1);
        assert_eq!(sstable.index[0].0, 42);

        let result = sstable.get(&42).expect("Failed to get value");
        assert_eq!(result, Some("answer".to_string()));

        let result = sstable.get(&41).expect("Failed to get value");
        assert_eq!(result, None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_sstable_error_invalid_path() {
        let result =
            SSTable::<i32, String>::read_from_file(PathBuf::from("/nonexistent/path/file.db"));
        assert!(result.is_err());

        if let Err(SSTableError::Io(_)) = result {
        } else {
            panic!("Expected IO error");
        }
    }
}
