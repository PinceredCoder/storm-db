use crate::{
    decompress,
    memtable::MemTableEntry,
    sstable::{BINCODE_CONFIG, Buffers, DEFAULT_BLOCK_SIZE, SSTable, SSTableError},
};
use std::hash::Hash;
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt},
};

impl<K: Ord + Hash + bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub async fn get(&self, key: &K) -> Result<Option<MemTableEntry<V>>, SSTableError> {
        self.get_with_block_size(key, DEFAULT_BLOCK_SIZE).await
    }

    pub(in crate::sstable) async fn get_with_block_size(
        &self,
        key: &K,
        block_size: u32,
    ) -> Result<Option<MemTableEntry<V>>, SSTableError> {
        if self.index.is_empty() {
            return Ok(None);
        }

        if !self.bloom_filter.check(key) {
            return Ok(None);
        }

        let search_result = self.index.binary_search_by(|(k, _)| k.cmp(key));

        let block_offset = match search_result {
            Ok(exact_idx) => self.index[exact_idx].1,
            Err(insert_idx) => {
                if insert_idx == 0 {
                    return Ok(None);
                }

                self.index[insert_idx - 1].1
            }
        };

        let decompressed_block_buf = if let Some(buf) = self.block_cache.get(&block_offset).await {
            buf
        } else {
            self.decompress_block(block_offset, block_size as usize)
                .await?
        };

        let mut total_bytes_decoded = 0;

        loop {
            match bincode::decode_from_slice::<(K, MemTableEntry<V>), _>(
                &decompressed_block_buf[total_bytes_decoded..],
                BINCODE_CONFIG,
            ) {
                Ok(((decoded_key, value), bytes_decoded)) => {
                    total_bytes_decoded += bytes_decoded;

                    match decoded_key.cmp(key) {
                        std::cmp::Ordering::Equal => return Ok(Some(value)),
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

    async fn decompress_block(
        &self,
        offset: u32,
        block_size: usize,
    ) -> Result<Vec<u8>, SSTableError> {
        let mut file = fs::File::open(&self.path).await?;
        file.seek(io::SeekFrom::Start(offset as u64)).await?;

        let mut buffers = Buffers::from_block_size(block_size);

        file.read_exact(&mut buffers.len_buf).await?;

        let compressed_block_len = u32::from_le_bytes(buffers.len_buf) as usize;

        file.read_exact(&mut buffers.compressed_block_buf[..compressed_block_len])
            .await?;

        _ = decompress!(
            &buffers.compressed_block_buf[..compressed_block_len],
            &mut buffers.decompressed_block_buf
        );

        self.block_cache
            .insert(offset, buffers.decompressed_block_buf.clone())
            .await;

        Ok(buffers.decompressed_block_buf)
    }
}
