use crate::{
    compress,
    memtable::MemTable,
    sstable::{BINCODE_CONFIG, DEFAULT_BLOCK_SIZE, SSTable, SSTableError},
};
use std::{fs, io::Write, path::Path};

impl<K: Ord + bincode::Encode, V: bincode::Encode> SSTable<K, V> {
    pub fn flush_memtable_sync<P: AsRef<Path>>(
        memtable: MemTable<K, V>,
        path: P,
    ) -> Result<(), SSTableError> {
        Self::flush_memtable_sync_with_block_size(memtable, path, DEFAULT_BLOCK_SIZE)
    }

    fn flush_memtable_sync_with_block_size<P: AsRef<Path>>(
        memtable: MemTable<K, V>,
        path: P,
        block_size: u32,
    ) -> Result<(), SSTableError> {
        if memtable.is_empty() {
            return Ok(());
        }

        let mut file = std::fs::File::create(&path)?;

        let sorted_items = memtable.into_sorted_vec();

        let total_items_cnt = sorted_items.len() as u32;
        file.write_all(&total_items_cnt.to_le_bytes())?;

        let mut bytes_since_last_index = 0;

        let mut encoded_buf = vec![0u8; 2 * block_size as usize];
        let mut compressed_buf = Vec::with_capacity(2 * block_size as usize);

        for (k, v) in sorted_items.into_iter() {
            if bytes_since_last_index >= block_size as usize {
                Self::write_block_sync(
                    &mut file,
                    &mut compressed_buf,
                    &encoded_buf[..bytes_since_last_index],
                )?;

                bytes_since_last_index = 0;
                compressed_buf.clear();
            }

            bytes_since_last_index += bincode::encode_into_slice(
                (k, v),
                &mut encoded_buf[bytes_since_last_index..],
                BINCODE_CONFIG,
            )?;
        }

        if bytes_since_last_index > 0 {
            Self::write_block_sync(
                &mut file,
                &mut compressed_buf,
                &encoded_buf[..bytes_since_last_index],
            )?;
        }

        file.flush()?;

        Ok(())
    }

    fn write_block_sync(
        file: &mut fs::File,
        compressed_buf: &mut Vec<u8>,
        encoded_data: &[u8],
    ) -> Result<(), SSTableError> {
        let compressed_buf = compress!(&encoded_data, compressed_buf);
        let compressed_size = compressed_buf.len() as u32;

        file.write_all(&compressed_size.to_le_bytes())?;
        file.write_all(compressed_buf)?;

        Ok(())
    }
}
