use std::{
    borrow::Borrow,
    fs,
    io::{self, Read, Seek, Write},
    marker::PhantomData,
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use crate::memtable::MemTable;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024; // 4KB blocks

/*
    TODO:
    1. Implement merging of SSTables
    2. Do not compress too small tables
    3. Do not reallocate buffers*
    4. Better estimates for buffers' sizes
*/

macro_rules! compress {
    ($in_buf:expr, $out_buf:expr) => {{
        let mut compressor =
            flate2::write::DeflateEncoder::new($out_buf, flate2::Compression::fast());
        compressor.write_all($in_buf)?;
        compressor.finish()?
    }};
}

macro_rules! decompress {
    ($in_buf:expr, $out_buf:expr) => {{
        let mut decompressor = flate2::read::DeflateDecoder::new($in_buf);
        decompressor.read_to_end($out_buf)?;
        $out_buf
    }};
}

struct SSTable<K, V> {
    path: PathBuf,
    index: Vec<(K, u32)>, // Sparse index: only every Nth key
    _phantom: PhantomData<V>,
}

impl<K: Ord + Clone + bincode::Encode, V: bincode::Encode> SSTable<K, V> {
    pub fn write_from_memtable(
        memtable: MemTable<K, V>,
        path: PathBuf,
    ) -> Result<Self, SSTableError> {
        Self::write_from_memtable_with_block_size(memtable, path, DEFAULT_BLOCK_SIZE)
    }

    fn write_from_memtable_with_block_size(
        memtable: MemTable<K, V>,
        path: PathBuf,
        block_size: u32,
    ) -> Result<Self, SSTableError> {
        let mut file = fs::File::create(&path)?;
        let mut index = Vec::new();

        let sorted_items = memtable.into_sorted_vec();

        let mut offset = 0;
        let mut bytes_since_last_index = 0;

        let mut encoded = vec![0u8; 2 * block_size as usize];
        let mut compressed = Vec::with_capacity(2 * block_size as usize);

        let Some(mut current_key) = sorted_items.first().map(|(k, _v)| k.clone()) else {
            return Ok(Self {
                path,
                index,
                _phantom: PhantomData,
            });
        };

        for (k, v) in sorted_items.into_iter() {
            if bytes_since_last_index >= block_size as usize {
                index.push((std::mem::replace(&mut current_key, k.clone()), offset));

                let compressed = compress!(&encoded[..bytes_since_last_index], &mut compressed);
                let compressed_size = compressed.len() as u32;

                file.write_all(&compressed_size.to_le_bytes())?;
                file.write_all(compressed)?;

                offset += size_of_val(&compressed_size) as u32 + compressed.len() as u32;

                bytes_since_last_index = 0;
                compressed.clear();
            }

            bytes_since_last_index += bincode::encode_into_slice(
                (k, v),
                &mut encoded[bytes_since_last_index..],
                BINCODE_CONFIG,
            )?;
        }

        if bytes_since_last_index > 0 {
            index.push((current_key, offset));

            let compressed = compress!(&encoded[..bytes_since_last_index], &mut compressed);
            let compressed_size = compressed.len() as u32;

            file.write_all(&compressed_size.to_le_bytes())?;
            file.write_all(compressed)?;
        }

        file.flush()?;

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

    fn read_from_file_with_block_size(
        path: PathBuf,
        block_size: u32,
    ) -> Result<Self, SSTableError> {
        let mut file = fs::File::open(&path)?;
        let file_size = file.metadata()?.size();
        let mut index = Vec::with_capacity((file_size / block_size as u64) as usize);

        let mut offset = 0;

        let mut len_buf = [0u8; size_of::<u32>()];
        let mut compressed_block_buf = vec![0u8; 2 * block_size as usize];
        let mut decompressed_block_buf = Vec::with_capacity(2 * block_size as usize);

        loop {
            if let Err(e) = file.read_exact(&mut len_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e.into());
                }
            };

            let compressed_block_len = u32::from_le_bytes(len_buf) as usize;

            file.read_exact(&mut compressed_block_buf[..compressed_block_len])?;

            let decompressed_block_buf = decompress!(
                &compressed_block_buf[..compressed_block_len],
                &mut decompressed_block_buf
            );

            let ((key, _value), _) = bincode::decode_from_slice::<(K, Option<V>), _>(
                decompressed_block_buf,
                BINCODE_CONFIG,
            )?;

            index.push((key, offset as u32));
            offset += len_buf.len() + compressed_block_len;

            decompressed_block_buf.clear()
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
        self.get_with_block_size(key, DEFAULT_BLOCK_SIZE)
    }

    fn get_with_block_size<Q: Borrow<K>>(
        &self,
        key: &Q,
        block_size: u32,
    ) -> Result<Option<V>, SSTableError> {
        if self.index.is_empty() {
            return Ok(None);
        }

        let search_result = self.index.binary_search_by(|(k, _)| k.cmp(key.borrow()));

        let block_offset = match search_result {
            Ok(exact_idx) => self.index[exact_idx].1,
            Err(insert_idx) => {
                if insert_idx == 0 {
                    return Ok(None);
                }

                self.index[insert_idx - 1].1
            }
        };

        let mut file = fs::File::open(&self.path)?;
        file.seek(io::SeekFrom::Start(block_offset as u64))?;

        let mut len_buf = [0u8; size_of::<u32>()];
        let mut compressed_block_buf = vec![0u8; 2 * block_size as usize];
        let mut decompressed_block_buf = Vec::with_capacity(2 * block_size as usize);

        file.read_exact(&mut len_buf)?;

        let compressed_block_len = u32::from_le_bytes(len_buf) as usize;

        file.read_exact(&mut compressed_block_buf[..compressed_block_len])?;

        let decompressed_block_buf = decompress!(
            &compressed_block_buf[..compressed_block_len],
            &mut decompressed_block_buf
        );

        let mut total_bytes_decoded = 0;

        loop {
            match bincode::decode_from_slice::<(K, Option<V>), _>(
                &decompressed_block_buf[total_bytes_decoded..],
                BINCODE_CONFIG,
            ) {
                Ok(((decoded_key, value), bytes_decoded)) => {
                    total_bytes_decoded += bytes_decoded;

                    match decoded_key.cmp(key.borrow()) {
                        std::cmp::Ordering::Equal => return Ok(value),
                        std::cmp::Ordering::Greater => return Ok(None),
                        std::cmp::Ordering::Less => continue,
                    }
                }
                Err(e) => {
                    if matches!(e, bincode::error::DecodeError::UnexpectedEnd { .. }) {
                        return Ok(None);
                    } else {
                        return Err(e.into());
                    }
                }
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
    fn test_write_and_read_sstable_1() {
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
        assert!(sstable.index.len() < 3);
        assert_eq!(sstable.index[0].0, 1); // First key should be indexed

        let loaded_sstable =
            SSTable::<i32, String>::read_from_file(path.clone()).expect("Failed to read SSTable");
        assert!(loaded_sstable.index.len() < 3);

        assert_eq!(loaded_sstable.index, sstable.index);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_write_and_read_sstable_2() {
        let mut memtable = MemTable::new(10_000);

        for (i, j) in (0..10_000).zip((0..10_000).rev()) {
            memtable
                .put(i, format! {"string #{j}"})
                .expect("Failed to put");
        }

        let path = PathBuf::from("/tmp/test_sstable.db");

        let sstable =
            SSTable::write_from_memtable(memtable, path.clone()).expect("Failed to write SSTable");

        assert!(sstable.index.len() < 5_000);

        let loaded_sstable =
            SSTable::<i32, String>::read_from_file(path.clone()).expect("Failed to read SSTable");
        assert!(loaded_sstable.index.len() < 5_000);

        assert_eq!(loaded_sstable.index, sstable.index);

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

        assert_eq!(loaded_sstable.index, sstable.index);

        for i in 1..=10 {
            let result = loaded_sstable
                .get_with_block_size(&i, 50)
                .expect("Failed to get value");
            assert_eq!(result, Some(format!("value{}", i)));
        }

        // Test non-existent keys
        assert_eq!(
            loaded_sstable
                .get_with_block_size(&0, 50)
                .expect("Failed to get"),
            None
        );
        assert_eq!(
            loaded_sstable
                .get_with_block_size(&11, 50)
                .expect("Failed to get"),
            None
        );

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

    #[test]
    fn get_tests_coverage() {
        let target_dir = std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.parent() // remove executable name
                    .and_then(|p| p.parent()) // remove 'debug' or 'release'
                    .map(|p| p.to_path_buf())
            })
            .unwrap();

        std::process::Command::new("cargo")
            .arg("llvm-cov")
            .arg("--lcov")
            .arg("--output-path")
            .arg(format!("{}/lcov.info", target_dir.display()))
            .output()
            .expect("failed to execute process");
    }
}
