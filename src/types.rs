use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashSet;

#[derive(Debug, Clone)]
pub struct ShredBytesMeta {
    pub shred_bytes: Bytes,
    pub received_at_micros: Option<u64>,
}

pub type ProcessedFecSets = DashSet<(u64, u32)>;
