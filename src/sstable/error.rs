use crate::memtable;

#[derive(Debug, thiserror::Error)]
pub enum SSTableError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encoding error: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("decoding error: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("bloom filter error: {0}")]
    BloomFilter(&'static str),
    #[error("memtable error: {0}")]
    Memtable(#[from] memtable::MemTableError),
}
