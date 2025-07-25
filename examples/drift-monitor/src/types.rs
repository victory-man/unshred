use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftEvent {
    pub slot: u64,
    pub signature: String,
    pub confirmed: bool,
    pub instruction_data: String,
    pub accounts: Vec<String>,
    pub liquidation_type: Option<String>,
    pub received_at_micros: Option<u64>,
    pub processed_at_micros: u64,
}
