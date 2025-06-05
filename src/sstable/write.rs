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
        let mut file = fs::File::create(&path).await?;
        let mut index = Vec::new();

        if memtable.is_empty() {
            return Ok(Self::default());
        }

        let sorted_items = memtable.into_sorted_vec();

        let total_items_cnt = sorted_items.len() as u32;
        file.write_all(&total_items_cnt.to_le_bytes()).await?;

        let mut offset = size_of::<u32>();
        let mut bytes_since_last_index = 0;

        let mut encoded = vec![0u8; 2 * block_size as usize];
        let mut compressed = Vec::with_capacity(2 * block_size as usize);

        let mut current_key = sorted_items.first().map(|(k, _v)| k.clone()).unwrap();

        let mut bloom_filter =
            Bloom::new_for_fp_rate(sorted_items.len(), BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .map_err(SSTableError::BloomFilter)?;

        let block_cache = Cache::new(DEFAULT_CACHE_SIZE);

        for (k, v) in sorted_items.into_iter() {
            if bytes_since_last_index >= block_size as usize {
                index.push((
                    std::mem::replace(&mut current_key, k.clone()),
                    offset as u32,
                ));

                block_cache
                    .insert(offset as u32, encoded[..bytes_since_last_index].to_vec())
                    .await;

                let compressed = compress!(&encoded[..bytes_since_last_index], &mut compressed);
                let compressed_size = compressed.len() as u32;

                file.write_all(&compressed_size.to_le_bytes()).await?;
                file.write_all(compressed).await?;

                offset += size_of_val(&compressed_size) + compressed.len();

                bytes_since_last_index = 0;
                compressed.clear();
            }

            bloom_filter.set(&k);

            bytes_since_last_index += bincode::encode_into_slice(
                (k, v),
                &mut encoded[bytes_since_last_index..],
                BINCODE_CONFIG,
            )?;
        }

        if bytes_since_last_index > 0 {
            index.push((current_key, offset as u32));

            block_cache
                .insert(offset as u32, encoded[..bytes_since_last_index].to_vec())
                .await;

            let compressed = compress!(&encoded[..bytes_since_last_index], &mut compressed);
            let compressed_size = compressed.len() as u32;

            file.write_all(&compressed_size.to_le_bytes()).await?;
            file.write_all(compressed).await?;
        }

        file.flush().await?;

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
