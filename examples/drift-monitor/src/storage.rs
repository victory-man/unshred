use crate::types::DriftEvent;

use anyhow::Result;
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tracing::{error, warn};

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub clickhouse_url: String,
    pub clickhouse_db: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
}

pub struct Storage {
    pub clickhouse: Arc<ClickHouseStorage>,
}

impl Storage {
    pub fn new(config: &StorageConfig) -> Result<Self> {
        let clickhouse = Arc::new(ClickHouseStorage::new(
            &config.clickhouse_url,
            &config.clickhouse_db,
            &config.clickhouse_user,
            &config.clickhouse_password,
        ));

        Ok(Self { clickhouse })
    }
}

#[derive(Row, Serialize, Deserialize)]
struct ClickHouseEvent {
    slot: u64,
    signature: String,
    instruction_data: String,
    accounts: Vec<String>,
    liquidation_type: Option<String>,
    received_at_micros: Option<u64>,
    processed_at_micros: u64,
}

impl From<DriftEvent> for ClickHouseEvent {
    fn from(event: DriftEvent) -> Self {
        ClickHouseEvent {
            slot: event.slot,
            signature: event.signature,
            instruction_data: event.instruction_data,
            accounts: event.accounts,
            liquidation_type: event.liquidation_type,
            received_at_micros: event.received_at_micros,
            processed_at_micros: event.processed_at_micros,
        }
    }
}

const BATCH_SIZE: usize = 1000;
const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

pub struct ClickHouseStorage {
    client: Client,
}

impl ClickHouseStorage {
    pub fn new(url: &str, db: &str, user: &str, password: &str) -> Self {
        let client = Client::default()
            .with_url(url)
            .with_database(db)
            .with_user(user)
            .with_password(password);

        Self { client }
    }

    pub async fn run_event_writer(
        &self,
        mut event_rx: tokio::sync::mpsc::Receiver<DriftEvent>,
    ) -> Result<()> {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut interval = tokio::time::interval(FLUSH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let remaining_capacity = BATCH_SIZE.saturating_sub(batch.len());

            tokio::select! {
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        if let Err(e) = self.flush_batch(&mut batch).await {
                            error!("CRITICAL: ClickHouse batch flush failed: {}", e);
                            return Err(e);
                        }
                    }
                }
                n = event_rx.recv_many(&mut batch, remaining_capacity) => {
                    if batch.len() >= BATCH_SIZE {
                        warn!("Dropping events due to batch size limit!");
                        batch.truncate(BATCH_SIZE);
                    }
                    if n == 0 {
                        break; // Channel closed and empty
                    }

                    if batch.len() >= BATCH_SIZE {
                        if let Err(e) = self.flush_batch(&mut batch).await {
                            error!("CRITICAL: ClickHouse batch flush failed: {}", e);
                            return Err(e);
                        }
                        interval.reset();
                    }
                }
            }
        }

        if !batch.is_empty() {
            self.flush_batch(&mut batch).await?;
        }

        Ok(())
    }

    async fn flush_batch(&self, batch: &mut Vec<DriftEvent>) -> Result<()> {
        let mut insert = self.client.insert("drift_events")?;
        for event in batch.drain(..) {
            insert.write(&ClickHouseEvent::from(event)).await?;
        }
        if let Err(e) = insert.end().await {
            error!("Could not write events! {:?}", e);
            return Err(anyhow::anyhow!(
                "Failed to flush batch into ClickHouse: {}",
                e
            ));
        };

        Ok(())
    }

    pub async fn delete_events_with_cutoff(&self, cutoff_slot: u64) -> Result<()> {
        let query = format!(
            "ALTER TABLE drift_events DELETE WHERE slot <= {}",
            cutoff_slot
        );
        self.client.query(&query).execute().await?;
        Ok(())
    }

    async fn get_table_size_gb(&self) -> Result<f64> {
        let query = "SELECT formatReadableSize(sum(bytes_on_disk)) as size_readable FROM system.parts WHERE database = 'drift' AND table = 'drift_events'";

        let size_str: String = self.client.query(query).fetch_one::<String>().await?;

        Self::parse_readable_size_to_gb(&size_str)
    }

    fn parse_readable_size_to_gb(size_str: &str) -> Result<f64> {
        let parts: Vec<&str> = size_str.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid size format: {}", size_str));
        }

        let value: f64 = parts[0].parse()?;
        let unit = parts[1];

        let gb = match unit {
            "B" => value / (1024.0 * 1024.0 * 1024.0),
            "KiB" => value / (1024.0 * 1024.0),
            "MiB" => value / 1024.0,
            "GiB" => value,
            "TiB" => value * 1024.0,
            _ => return Err(anyhow::anyhow!("Unknown unit: {}", unit)),
        };

        Ok(gb)
    }

    async fn get_oldest_and_newest_slots(&self) -> Result<(u64, u64)> {
        let query = "SELECT min(slot) as min_slot, max(slot) as max_slot FROM drift.drift_events";

        let result = self.client.query(query).fetch_one::<(u64, u64)>().await?;

        Ok(result)
    }

    pub async fn cleanup_slots_by_size(&self, max_size_gb: f64) -> Result<()> {
        let current_size = self.get_table_size_gb().await?;

        if current_size <= max_size_gb {
            return Ok(());
        }

        let (oldest_slot, newest_slot) = self.get_oldest_and_newest_slots().await?;
        let total_slots = newest_slot.saturating_sub(oldest_slot);

        if total_slots == 0 {
            return Ok(());
        }

        // Estimate: remove enough slots to get to 80% of max size
        let target_size = max_size_gb * 0.8;
        let size_reduction_needed = current_size - target_size;
        let size_per_slot = current_size / total_slots as f64;
        let slots_to_remove = (size_reduction_needed / size_per_slot).ceil() as u64;

        let cutoff_slot = oldest_slot + slots_to_remove;

        self.delete_events_with_cutoff(cutoff_slot).await?;

        Ok(())
    }
}
