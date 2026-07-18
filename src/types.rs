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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn saturating_sub(&self, earlier: &Self) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_sub(earlier.input_tokens),
            output_tokens: self.output_tokens.saturating_sub(earlier.output_tokens),
            cache_creation_input_tokens: self
                .cache_creation_input_tokens
                .saturating_sub(earlier.cache_creation_input_tokens),
            cache_read_input_tokens: self
                .cache_read_input_tokens
                .saturating_sub(earlier.cache_read_input_tokens),
        }
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

    #[test]
    fn session_usage_delta_is_per_turn_and_saturating() {
        let before = SessionUsage {
            input_tokens: 100,
            output_tokens: 40,
            cache_creation_input_tokens: 20,
            cache_read_input_tokens: 10,
        };
        let after = SessionUsage {
            input_tokens: 125,
            output_tokens: 47,
            cache_creation_input_tokens: 15,
            cache_read_input_tokens: 13,
        };

        assert_eq!(
            after.saturating_sub(&before),
            SessionUsage {
                input_tokens: 25,
                output_tokens: 7,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 3,
            }
        );
    }
}
