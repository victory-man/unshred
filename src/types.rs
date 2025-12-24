use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct ShredBytesMeta {
    pub shred_bytes: Bytes,
    pub received_at_micros: Option<u64>,
}
