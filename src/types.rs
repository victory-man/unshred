use std::sync::Arc;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct ShredBytesMeta {
    pub shred_bytes: Bytes,
    pub received_at_micros: Option<u64>,
}

pub type ProcessedFecSets = moka::sync::Cache<(u64, u32), (), ahash::RandomState>;
