use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnshredConfig {
    pub bind_address: String,
}

impl Default for UnshredConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8001".to_string(),
        }
    }
}
