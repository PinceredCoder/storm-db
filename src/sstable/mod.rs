mod error;
mod flush_sync;
mod get;
mod read;
mod write;

#[cfg(test)]
mod tests;

pub use error::SSTableError;

use bloomfilter::Bloom;
use moka::future::Cache;
use std::{marker::PhantomData, path::PathBuf};

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024; // 4KB blocks
const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;
const DEFAULT_CACHE_SIZE: u64 = 10; // Number of blocks to cache

/*
    TODO:
    1. Implement merging of SSTables
    2. Do not compress too small tables*
    3. Do not reallocate buffers*
    4. Better estimates for buffers' sizes*
    5. Add check-sums for files
*/

#[macro_export]
macro_rules! compress {
    ($in_buf:expr, $out_buf:expr) => {{
        use std::io::Write;
        let mut compressor =
            flate2::write::DeflateEncoder::new($out_buf, flate2::Compression::fast());
        compressor.write_all($in_buf)?;
        compressor.finish()?
    }};
}

#[macro_export]
macro_rules! decompress {
    ($in_buf:expr, $out_buf:expr) => {{
        use std::io::Read;
        let mut decompressor = flate2::read::DeflateDecoder::new($in_buf);
        decompressor.read_to_end($out_buf)?;
        $out_buf
    }};
}

pub(crate) struct SSTable<K, V> {
    path: PathBuf,
    index: Vec<(K, u32)>, // Sparse index: only every Nth key
    bloom_filter: Bloom<K>,
    total_items_cnt: u32,
    block_cache: Cache<u32, Vec<u8>>, // Cache blocks by offset
    _phantom: PhantomData<V>,
}

impl<K, V> Default for SSTable<K, V> {
    fn default() -> Self {
        Self {
            path: Default::default(),
            index: Default::default(),
            bloom_filter: Bloom::new_for_fp_rate_with_seed(
                1,
                BLOOM_FILTER_FALSE_POSITIVE_RATE,
                &[0u8; 32],
            )
            .map_err(SSTableError::BloomFilter)
            .unwrap(),
            total_items_cnt: 0,
            block_cache: Cache::new(DEFAULT_CACHE_SIZE),
            _phantom: Default::default(),
        }
    }
}

impl<K, V> SSTable<K, V> {
    pub fn total_items(&self) -> u32 {
        self.total_items_cnt
    }
}
