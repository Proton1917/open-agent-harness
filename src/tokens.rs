use serde_json::Value;

use crate::types::Message;

pub const MEDIA_TOKEN_ESTIMATE: usize = 2_000;

pub fn rough_token_count(content: &str, bytes_per_token: usize) -> usize {
    content.len().div_ceil(bytes_per_token.max(1))
}

pub fn bytes_per_token_for_extension(extension: &str) -> usize {
    match extension
        .trim_start_matches('.')
        .to_ascii_lowercase()
        .as_str()
    {
        "json" | "jsonl" | "jsonc" => 2,
        _ => 4,
    }
}

pub fn estimate_messages(messages: &[Message]) -> usize {
    messages.iter().map(estimate_message).sum()
}

pub fn estimate_message(message: &Message) -> usize {
    estimate_content(&message.content)
}

pub fn estimate_content(content: &Value) -> usize {
    match content {
        Value::Null => 0,
        Value::String(text) => rough_token_count(text, 4),
        Value::Array(blocks) => blocks.iter().map(estimate_block).sum(),
        other => rough_token_count(&other.to_string(), 4),
    }
}

pub(crate) fn estimate_block(block: &Value) -> usize {
    let Some(object) = block.as_object() else {
        return rough_token_count(&block.to_string(), 4);
    };
    match object.get("type").and_then(Value::as_str).unwrap_or("") {
        "text" => text_field(object, "text"),
        "image" | "document" => MEDIA_TOKEN_ESTIMATE,
        "tool_result" => object.get("content").map(estimate_content).unwrap_or(0),
        "tool_use" | "server_tool_use" | "mcp_tool_use" => {
            let name = object.get("name").and_then(Value::as_str).unwrap_or("");
            rough_token_count(name, 4)
                + object
                    .get("input")
                    .map(|value| rough_token_count(&value.to_string(), 4))
                    .unwrap_or(0)
        }
        "thinking" => text_field(object, "thinking"),
        "redacted_thinking" => text_field(object, "data"),
        _ => rough_token_count(&block.to_string(), 4),
    }
}

fn text_field(object: &serde_json::Map<String, Value>, field: &str) -> usize {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(|text| rough_token_count(text, 4))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_json_uses_tighter_ratio() {
        assert_eq!(bytes_per_token_for_extension("json"), 2);
        assert_eq!(bytes_per_token_for_extension("rs"), 4);
    }

    #[test]
    fn media_does_not_count_encoded_payload_bytes() {
        let content = serde_json::json!([{
            "type":"image",
            "source":{"data":"x".repeat(100_000)}
        }]);
        assert_eq!(estimate_content(&content), MEDIA_TOKEN_ESTIMATE);
    }
}
