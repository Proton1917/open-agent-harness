use serde::{Deserialize, Serialize};
use serde_json::Value;

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Option::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: Role,
    pub content: Value,
}

impl Message {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: Value::String(text.into()),
        }
    }

    pub fn assistant(content: Vec<Value>) -> Self {
        Self {
            role: Role::Assistant,
            content: Value::Array(content),
        }
    }

    pub fn tool_results(results: Vec<Value>) -> Self {
        Self {
            role: Role::User,
            content: Value::Array(results),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub input_tokens: u64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub output_tokens: u64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub cache_creation_input_tokens: u64,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub cache_read_input_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelResponse {
    pub id: String,
    #[serde(default)]
    pub content: Vec<Value>,
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
}

impl SessionUsage {
    pub fn add(&mut self, usage: &Usage) {
        self.input_tokens = self.input_tokens.saturating_add(usage.input_tokens);
        self.output_tokens = self.output_tokens.saturating_add(usage.output_tokens);
        self.cache_creation_input_tokens = self
            .cache_creation_input_tokens
            .saturating_add(usage.cache_creation_input_tokens);
        self.cache_read_input_tokens = self
            .cache_read_input_tokens
            .saturating_add(usage.cache_read_input_tokens);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_treats_missing_and_null_counters_as_zero() {
        let usage: Usage = serde_json::from_value(serde_json::json!({
            "input_tokens": null,
            "output_tokens": 7,
            "cache_creation_input_tokens": null
        }))
        .unwrap();

        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.cache_creation_input_tokens, 0);
        assert_eq!(usage.cache_read_input_tokens, 0);
    }

    #[test]
    fn session_usage_saturates_untrusted_counters() {
        let mut total = SessionUsage {
            input_tokens: u64::MAX,
            ..SessionUsage::default()
        };
        total.add(&Usage {
            input_tokens: 1,
            output_tokens: u64::MAX,
            cache_creation_input_tokens: u64::MAX,
            cache_read_input_tokens: u64::MAX,
        });
        total.add(&Usage {
            input_tokens: 1,
            output_tokens: 1,
            cache_creation_input_tokens: 1,
            cache_read_input_tokens: 1,
        });
        assert_eq!(total.input_tokens, u64::MAX);
        assert_eq!(total.output_tokens, u64::MAX);
        assert_eq!(total.cache_creation_input_tokens, u64::MAX);
        assert_eq!(total.cache_read_input_tokens, u64::MAX);
    }
}
