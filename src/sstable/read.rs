use crate::{
    decompress,
    memtable::{MemTable, MemTableEntry},
    sstable::{
        BINCODE_CONFIG, BLOOM_FILTER_FALSE_POSITIVE_RATE, DEFAULT_BLOCK_SIZE, DEFAULT_CACHE_SIZE,
        SSTable, SSTableError,
    },
};
use bloomfilter::Bloom;
use moka::future::Cache;
use std::{hash::Hash, os::unix::fs::MetadataExt};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt},
};

impl<K: Hash + bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub async fn read_from_file(path: PathBuf) -> Result<Self, SSTableError> {
        Self::read_from_file_with_block_size(path, DEFAULT_BLOCK_SIZE).await
    }

    pub(in crate::sstable) async fn read_from_file_with_block_size<P: AsRef<Path>>(
        path: P,
        block_size: u32,
    ) -> Result<Self, SSTableError> {
        let mut file = fs::File::open(&path).await?;
        let file_size = file.metadata().await?.size();

        if file_size == 0 {
            return Ok(Self::default());
        }

        let mut total_items_cnt_buf = [0u8; size_of::<u32>()];
        file.read_exact(&mut total_items_cnt_buf).await?;
        let total_items_cnt = u32::from_le_bytes(total_items_cnt_buf);

        let mut index = Vec::with_capacity((file_size / block_size as u64) as usize);
        let mut offset = total_items_cnt_buf.len();

        let mut bloom_filter =
            Bloom::new_for_fp_rate(total_items_cnt as usize, BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .map_err(SSTableError::BloomFilter)?;

        let mut len_buf = [0u8; size_of::<u32>()];
        let mut compressed_block_buf = vec![0u8; 2 * block_size as usize];
        let mut decompressed_block_buf = Vec::with_capacity(2 * block_size as usize);

        let block_cache = Cache::new(DEFAULT_CACHE_SIZE);

        loop {
            if let Err(e) = file.read_exact(&mut len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e.into());
                }
            };

            let compressed_block_len = u32::from_le_bytes(len_buf) as usize;

            file.read_exact(&mut compressed_block_buf[..compressed_block_len])
                .await?;

            let decompressed_block_buf = decompress!(
                &compressed_block_buf[..compressed_block_len],
                &mut decompressed_block_buf
            );

            block_cache
                .insert(offset as u32, decompressed_block_buf.clone())
                .await;

            let mut total_bytes_decoded = 0;
            let mut first_item = true;

            loop {
                match bincode::decode_from_slice::<(K, MemTableEntry<V>), _>(
                    &decompressed_block_buf[total_bytes_decoded..],
                    BINCODE_CONFIG,
                ) {
                    Ok(((key, _value), bytes_decoded)) => {
                        bloom_filter.set(&key);

                        if first_item {
                            index.push((key, offset as u32));
                            first_item = false;
                        }

                        total_bytes_decoded += bytes_decoded;
                    }
                    Err(e) => {
                        if matches!(e, bincode::error::DecodeError::UnexpectedEnd { .. }) {
                            break;
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }

            offset += len_buf.len() + compressed_block_len;
            decompressed_block_buf.clear()
        }

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            index,
            bloom_filter,
            total_items_cnt,
            block_cache,
            _phantom: PhantomData,
        })
    }
}

impl<K: Ord + bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub async fn read_into_memtable(
        self,
        max_memtable_size: usize,
    ) -> Result<MemTable<K, V>, SSTableError> {
        self.read_into_memtable_with_block_size(max_memtable_size, DEFAULT_BLOCK_SIZE)
            .await
    }

    pub(in crate::sstable) async fn read_into_memtable_with_block_size(
        self,
        max_memtable_size: usize,
        block_size: u32,
    ) -> Result<MemTable<K, V>, SSTableError> {
        let mut file = fs::File::open(&self.path).await?;
        let file_size = file.metadata().await?.size();

        if file_size == 0 {
            return Ok(MemTable::new(max_memtable_size));
        }

        // Skip total items
        file.seek(io::SeekFrom::Start(size_of::<u32>() as u64))
            .await?;

        let mut len_buf = [0u8; size_of::<u32>()];
        let mut compressed_block_buf = vec![0u8; 2 * block_size as usize];
        let mut decompressed_block_buf = Vec::with_capacity(2 * block_size as usize);

        let mut entries = Vec::with_capacity(self.total_items_cnt as usize);

        loop {
            if let Err(e) = file.read_exact(&mut len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                } else {
                    return Err(e.into());
                }
            };

            let compressed_block_len = u32::from_le_bytes(len_buf) as usize;

            file.read_exact(&mut compressed_block_buf[..compressed_block_len])
                .await?;

            let decompressed_block_buf = decompress!(
                &compressed_block_buf[..compressed_block_len],
                &mut decompressed_block_buf
            );

            let mut total_bytes_decoded = 0;

            loop {
                match bincode::decode_from_slice::<(K, MemTableEntry<V>), _>(
                    &decompressed_block_buf[total_bytes_decoded..],
                    BINCODE_CONFIG,
                ) {
                    Ok(((key, value), bytes_decoded)) => {
                        entries.push((key, value));
                        total_bytes_decoded += bytes_decoded;
                    }
                    Err(e) => {
                        if matches!(e, bincode::error::DecodeError::UnexpectedEnd { .. }) {
                            break;
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }

            decompressed_block_buf.clear()
        }

        Ok(MemTable::from_iter(entries.into_iter(), max_memtable_size))
    }
}
