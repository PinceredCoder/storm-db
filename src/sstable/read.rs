use crate::{
    decompress,
    memtable::MemTableEntry,
    sstable::{
        BINCODE_CONFIG, BLOOM_FILTER_FALSE_POSITIVE_RATE, Buffers, DEFAULT_BLOCK_SIZE,
        DEFAULT_CACHE_SIZE, SSTable, SSTableError,
    },
};
use bloomfilter::Bloom;
use moka::future::Cache;
use std::{hash::Hash, os::unix::fs::MetadataExt};
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};
use tokio::{fs, io::AsyncReadExt};

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

        let index = Vec::with_capacity((file_size / block_size as u64) as usize);
        let bloom_filter =
            Bloom::new_for_fp_rate(total_items_cnt as usize, BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .map_err(SSTableError::BloomFilter)?;
        let block_cache = Cache::new(DEFAULT_CACHE_SIZE);

        let mut this = Self {
            path: path.as_ref().to_path_buf(),
            index,
            bloom_filter,
            total_items_cnt,
            block_cache,
            _phantom: PhantomData,
        };

        let mut offset = total_items_cnt_buf.len();
        let mut buffers = Buffers::from_block_size(block_size as usize);

        while let Some(offset_change) = this.parse_block(offset, &mut file, &mut buffers).await? {
            offset += offset_change;
        }

        Ok(this)
    }

    async fn parse_block(
        &mut self,
        offset: usize,
        file: &mut fs::File,
        buffers: &mut Buffers,
    ) -> Result<Option<usize>, SSTableError> {
        if let Err(e) = file.read_exact(&mut buffers.len_buf).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            } else {
                return Err(e.into());
            }
        };

        let compressed_block_len = u32::from_le_bytes(buffers.len_buf) as usize;

        file.read_exact(&mut buffers.compressed_block_buf[..compressed_block_len])
            .await?;

        let decompressed_block_buf = decompress!(
            &buffers.compressed_block_buf[..compressed_block_len],
            &mut buffers.decompressed_block_buf
        );

        self.block_cache
            .insert(offset as u32, decompressed_block_buf.clone())
            .await;

        let mut total_bytes_decoded = 0;
        let mut first_item = true;

        // loop through each key to set the bloom filter
        loop {
            match bincode::decode_from_slice::<(K, MemTableEntry<V>), _>(
                &decompressed_block_buf[total_bytes_decoded..],
                BINCODE_CONFIG,
            ) {
                Ok(((key, _value), bytes_decoded)) => {
                    self.bloom_filter.set(&key);

                    if first_item {
                        self.index.push((key, offset as u32));
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

        buffers.decompressed_block_buf.clear();

        Ok(Some(buffers.len_buf.len() + compressed_block_len))
    }
}
