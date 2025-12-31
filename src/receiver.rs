#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
use crate::types::{ProcessedFecSets, ShredBytesMeta};

use crate::socket_reader::SocketReader;
#[cfg(target_os = "linux")]
use crate::xdp_reader::XdpReader;
use anyhow::Result;
use std::{io, mem::MaybeUninit, sync::Arc};
use tokio::sync::mpsc::Sender;
use tracing::error;

pub const SHRED_SIZE: usize = 1228;
const RECV_BUFFER_SIZE: usize = 64 * 1024 * 1024; // 64MB
const OFFSET_SHRED_SLOT: usize = 65;
const OFFSET_FEC_SET_INDEX: usize = 79;

pub enum UdpReader {
    Socket(SocketReader),

    #[cfg(target_os = "linux")]
    Xdp(XdpReader),
}

pub struct ShredReceiver {
    reader: UdpReader,
}

impl ShredReceiver {
    pub fn new(reader: UdpReader) -> Result<Self> {
        Ok(Self { reader })
    }

    pub async fn run(
        self,
        senders: Vec<Sender<ShredBytesMeta>>,
        processed_fec_sets: Arc<ProcessedFecSets>,
    ) -> Result<()> {
        let handles = match self.reader {
            UdpReader::Socket(socket) => socket.run(senders, processed_fec_sets)?,
            #[cfg(target_os = "linux")]
            UdpReader::Xdp(xdp) => xdp.run(senders, processed_fec_sets)?,
        };
        for handle in handles {
            handle.await??;
        }
        Ok(())
    }

    /// Creates ShredBytesMeta and sends through `senders`
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn process_shred(
        buffer: &[u8],
        senders: &[Sender<ShredBytesMeta>],
        processed_fec_sets: &ProcessedFecSets,
    ) -> Result<()> {
        if buffer.len() < 88 {
            // Minimum shred header size
            return Err(anyhow::anyhow!("Invalid shred size"));
        }

        // Parse shred header
        let slot = u64::from_le_bytes(buffer[OFFSET_SHRED_SLOT..OFFSET_SHRED_SLOT + 8].try_into()?);
        let fec_set_index =
            u32::from_le_bytes(buffer[OFFSET_FEC_SET_INDEX..OFFSET_FEC_SET_INDEX + 4].try_into()?);

        let fec_key = (slot, fec_set_index);
        if processed_fec_sets.contains(&fec_key) {
            return Ok(()); // Exit early
        }

        // Send ShredBytesMeta to processor
        let worker_id = (fec_set_index as usize) % senders.len();
        let sender = &senders[worker_id];
        let shred_bytes_meta = ShredBytesMeta {
            shred_bytes: bytes::Bytes::copy_from_slice(buffer),
            // received_at_micros: Some(*received_at_micros),
        };
        match sender.try_send(shred_bytes_meta) {
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                return Err(anyhow::anyhow!("Channel full, backpressure detected"));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(anyhow::anyhow!("Channel disconnected"));
            }
        };

        Ok(())
    }
}
