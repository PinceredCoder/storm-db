use crate::{
    compress,
    memtable::MemTable,
    sstable::{
        BINCODE_CONFIG, BLOOM_FILTER_FALSE_POSITIVE_RATE, DEFAULT_BLOCK_SIZE, DEFAULT_CACHE_SIZE,
        SSTable, SSTableError,
    },
};
use bloomfilter::Bloom;
use moka::future::Cache;
use std::path::Path;
use std::{hash::Hash, marker::PhantomData};
use tokio::{fs, io::AsyncWriteExt};

impl<K: Ord + Clone + Hash + bincode::Encode, V: bincode::Encode> SSTable<K, V> {
    pub async fn write_from_memtable<P: AsRef<Path>>(
        memtable: MemTable<K, V>,
        path: P,
    ) -> Result<Self, SSTableError> {
        Self::write_from_memtable_with_block_size(memtable, path, DEFAULT_BLOCK_SIZE).await
    }

    pub(in crate::sstable) async fn write_from_memtable_with_block_size<P: AsRef<Path>>(
        memtable: MemTable<K, V>,
        path: P,
        block_size: u32,
    ) -> Result<Self, SSTableError> {
        let mut file: fs::File = fs::File::create(&path).await?;

        if memtable.is_empty() {
            return Ok(Self::default());
        }

        let sorted_items = memtable.into_sorted_vec();

        let total_items_cnt = sorted_items.len() as u32;
        file.write_all(&total_items_cnt.to_le_bytes()).await?;

        let bloom_filter =
            Bloom::new_for_fp_rate(sorted_items.len(), BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .map_err(SSTableError::BloomFilter)?;
        let block_cache = Cache::new(DEFAULT_CACHE_SIZE);

        let mut this = Self {
            path: path.as_ref().to_path_buf(),
            index: Vec::with_capacity((total_items_cnt / block_size) as usize),
            bloom_filter,
            total_items_cnt,
            block_cache,
            _phantom: PhantomData,
        };

        let mut offset = size_of::<u32>();
        let mut bytes_since_last_index = 0;

        let mut encoded_buf = vec![0u8; 2 * block_size as usize];
        let mut compressed_buf = Vec::with_capacity(2 * block_size as usize);

        let mut current_key = sorted_items.first().map(|(k, _v)| k.clone()).unwrap();

        for (k, v) in sorted_items.into_iter() {
            if bytes_since_last_index >= block_size as usize {
                offset = this
                    .write_index(
                        std::mem::replace(&mut current_key, k.clone()),
                        offset as u32,
                        &mut file,
                        &mut compressed_buf,
                        encoded_buf[..bytes_since_last_index].to_vec(),
                    )
                    .await?;

                bytes_since_last_index = 0;
                compressed_buf.clear();
            }

            this.bloom_filter.set(&k);

            bytes_since_last_index += bincode::encode_into_slice(
                (k, v),
                &mut encoded_buf[bytes_since_last_index..],
                BINCODE_CONFIG,
            )?;
        }

        if bytes_since_last_index > 0 {
            this.write_index(
                current_key,
                offset as u32,
                &mut file,
                &mut compressed_buf,
                encoded_buf[..bytes_since_last_index].to_vec(),
            )
            .await?;
        }

        file.flush().await?;

        Ok(this)
    }

    async fn write_index(
        &mut self,
        key: K,
        offset: u32,
        file: &mut fs::File,
        compressed_buf: &mut Vec<u8>,
        encoded_data: Vec<u8>,
    ) -> Result<usize, SSTableError> {
        self.index.push((key, offset));

        let compressed_buf = compress!(&encoded_data, compressed_buf);
        let compressed_size = compressed_buf.len() as u32;

        self.block_cache.insert(offset, encoded_data).await;

        file.write_all(&compressed_size.to_le_bytes()).await?;
        file.write_all(compressed_buf).await?;

        Ok(offset as usize + size_of_val(&compressed_size) + compressed_buf.len())
    }
}
