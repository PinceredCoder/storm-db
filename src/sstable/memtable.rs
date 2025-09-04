use std::os::unix::fs::MetadataExt;

use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt},
};

use crate::{
    decompress,
    memtable::{MemTable, MemTableEntry},
    sstable::{BINCODE_CONFIG, Buffers, DEFAULT_BLOCK_SIZE, SSTable, SSTableError},
};

impl<K: Ord + bincode::Decode<()>, V: bincode::Decode<()>> SSTable<K, V> {
    pub async fn into_memtable(
        self,
        max_memtable_size: usize,
    ) -> Result<MemTable<K, V>, SSTableError> {
        self.into_memtable_with_block_size(max_memtable_size, DEFAULT_BLOCK_SIZE)
            .await
    }

    pub(in crate::sstable) async fn into_memtable_with_block_size(
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

        let entries = self
            .parse_into_memtable_entries(&mut file, block_size)
            .await?;

        Ok(MemTable::from_iter(entries.into_iter(), max_memtable_size))
    }

    async fn parse_into_memtable_entries(
        &self,
        file: &mut fs::File,
        block_size: u32,
    ) -> Result<Vec<(K, MemTableEntry<V>)>, SSTableError> {
        let mut buffers = Buffers::from_block_size(block_size as usize);
        let mut entries = Vec::with_capacity(self.total_items_cnt as usize);

        loop {
            if let Err(e) = file.read_exact(&mut buffers.len_buf).await {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(entries);
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

            decompressed_block_buf.clear();
        }
    }
}
