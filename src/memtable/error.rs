#[derive(Debug, thiserror::Error)]
pub enum MemTableError {
    #[error("memtable capacity exceeded")]
    CapacityExceeded,
}
