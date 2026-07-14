use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct UserInteractionRequest {
    pub tool: String,
    pub input: Value,
}

pub type UserInteractionHandler =
    Arc<dyn Fn(&UserInteractionRequest) -> Result<Value> + Send + Sync>;
