use crate::{memtable::MemTableError, sstable::SSTableError};

#[derive(Debug, thiserror::Error)]
pub enum DBServiceError {
    #[error("memtable error: {0}")]
    MemTable(#[from] MemTableError),
    #[error("sstable error: {0}")]
    SSTable(#[from] SSTableError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("runtime error: {0}")]
    Runtime(#[from] tokio::sync::TryLockError),
}
