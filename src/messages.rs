use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use crate::types::{Message, Role};

pub fn normalize_for_api(messages: &[Message]) -> Vec<Message> {
    let normalized = messages
        .iter()
        .filter_map(normalize_message)
        .collect::<Vec<_>>();
    let merged = merge_adjacent_roles(normalized);
    let repaired = repair_tool_pairing(merged);
    let mut normalized = merge_adjacent_roles(repaired);
    if normalized
        .first()
        .is_some_and(|message| message.role == Role::Assistant)
    {
        normalized.insert(0, Message::user_text("Conversation resumed."));
    }
    normalized
}

fn normalize_message(message: &Message) -> Option<Message> {
    let blocks = content_blocks(&message.content)
        .into_iter()
        .filter(|block| !is_empty_block(block))
        .collect::<Vec<_>>();
    (!blocks.is_empty()).then_some(Message {
        role: message.role,
        content: Value::Array(blocks),
    })
}

fn content_blocks(content: &Value) -> Vec<Value> {
    match content {
        Value::String(text) => vec![json!({"type": "text", "text": text})],
        Value::Array(blocks) => blocks.clone(),
        Value::Null => Vec::new(),
        value => vec![json!({"type": "text", "text": value.to_string()})],
    }
}

fn is_empty_block(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("text")
        && block
            .get("text")
            .and_then(Value::as_str)
            .is_none_or(|text| text.trim().is_empty())
}

fn repair_tool_pairing(messages: Vec<Message>) -> Vec<Message> {
    let mut repaired = Vec::with_capacity(messages.len());
    let mut messages = messages.into_iter().peekable();

    while let Some(mut message) = messages.next() {
        match message.role {
            Role::Assistant => {
                let tool_ids = sanitize_tool_uses(&mut message);
                if !content_blocks(&message.content).is_empty() {
                    repaired.push(message);
                }
                if tool_ids.is_empty() {
                    continue;
                }

                let user = messages
                    .next_if(|next| next.role == Role::User)
                    .unwrap_or_else(|| Message::tool_results(Vec::new()));
                repaired.push(repair_result_turn(user, &tool_ids));
            }
            Role::User => {
                strip_tool_results(&mut message);
                if !content_blocks(&message.content).is_empty() {
                    repaired.push(message);
                }
            }
        }
    }

    repaired
}

fn sanitize_tool_uses(message: &mut Message) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut ids = Vec::new();
    let mut blocks = Vec::new();

    for block in content_blocks(&message.content) {
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            blocks.push(block);
            continue;
        }
        let Some(id) = block
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if seen.insert(id.to_owned()) {
            ids.push(id.to_owned());
            blocks.push(block);
        }
    }

    message.content = Value::Array(blocks);
    ids
}

fn repair_result_turn(user: Message, tool_ids: &[String]) -> Message {
    let expected = tool_ids.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut results = HashMap::with_capacity(tool_ids.len());
    let mut remaining = Vec::new();

    for block in content_blocks(&user.content) {
        if block.get("type").and_then(Value::as_str) != Some("tool_result") {
            remaining.push(block);
            continue;
        }
        let Some(id) = block.get("tool_use_id").and_then(Value::as_str) else {
            continue;
        };
        if expected.contains(id) {
            results.entry(id.to_owned()).or_insert(block);
        }
    }

    let mut blocks = Vec::with_capacity(tool_ids.len().saturating_add(remaining.len()));
    for id in tool_ids {
        blocks.push(results.remove(id).unwrap_or_else(|| {
            json!({
                "type": "tool_result",
                "tool_use_id": id,
                "content": "Tool execution was interrupted before a result was recorded.",
                "is_error": true
            })
        }));
    }
    blocks.extend(remaining);
    Message::tool_results(blocks)
}

fn strip_tool_results(message: &mut Message) {
    let blocks = content_blocks(&message.content)
        .into_iter()
        .filter(|block| block.get("type").and_then(Value::as_str) != Some("tool_result"))
        .collect();
    message.content = Value::Array(blocks);
}

fn merge_adjacent_roles(messages: Vec<Message>) -> Vec<Message> {
    let mut merged: Vec<Message> = Vec::new();
    for message in messages {
        match merged.last_mut() {
            Some(previous) if previous.role == message.role => {
                let mut blocks = content_blocks(&previous.content);
                blocks.extend(content_blocks(&message.content));
                previous.content = Value::Array(blocks);
            }
            _ => merged.push(message),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocks(message: &Message) -> &[Value] {
        message.content.as_array().unwrap()
    }

    #[test]
    fn merges_adjacent_roles_before_repairing_results() {
        let messages = vec![
            Message::user_text("question"),
            Message::assistant(vec![json!({
                "type":"tool_use","id":"t1","name":"Read","input":{}
            })]),
            Message::user_text("keep this text"),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"t1","content":"actual result"
            })]),
        ];

        let normalized = normalize_for_api(&messages);

        assert_eq!(normalized.len(), 3);
        let result_turn = blocks(&normalized[2]);
        assert_eq!(
            result_turn
                .iter()
                .filter(|block| block["type"] == "tool_result")
                .count(),
            1
        );
        assert_eq!(result_turn[0]["tool_use_id"], "t1");
        assert_eq!(result_turn[0]["content"], "actual result");
        assert_eq!(result_turn[1]["text"], "keep this text");
    }

    #[test]
    fn keeps_one_local_result_per_call_and_synthesizes_missing_results() {
        let messages = vec![
            Message::user_text("question"),
            Message::assistant(vec![
                json!({"type":"tool_use","id":"a","name":"Read","input":{}}),
                json!({"type":"tool_use","id":"b","name":"Read","input":{}}),
            ]),
            Message::tool_results(vec![
                json!({"type":"tool_result","tool_use_id":"wrong","content":"orphan"}),
                json!({"type":"tool_result","tool_use_id":"b","content":"first"}),
                json!({"type":"tool_result","tool_use_id":"b","content":"duplicate"}),
                json!({"type":"text","text":"keep"}),
            ]),
        ];

        let normalized = normalize_for_api(&messages);

        let result_turn = blocks(&normalized[2]);
        assert_eq!(result_turn.len(), 3);
        assert_eq!(result_turn[0]["tool_use_id"], "a");
        assert_eq!(result_turn[0]["is_error"], true);
        assert_eq!(result_turn[1]["tool_use_id"], "b");
        assert_eq!(result_turn[1]["content"], "first");
        assert_eq!(result_turn[2]["text"], "keep");
    }

    #[test]
    fn removes_misplaced_and_orphaned_results() {
        let messages = vec![
            Message::user_text("question"),
            Message::assistant(vec![json!({
                "type":"tool_use","id":"t1","name":"Read","input":{}
            })]),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"t1","content":"local"
            })]),
            Message::assistant(vec![json!({"type":"text","text":"done"})]),
            Message::tool_results(vec![
                json!({"type":"tool_result","tool_use_id":"t1","content":"misplaced"}),
                json!({"type":"tool_result","tool_use_id":"never-seen","content":"orphan"}),
            ]),
        ];

        let normalized = normalize_for_api(&messages);

        let serialized = serde_json::to_string(&normalized).unwrap();
        assert_eq!(serialized.matches("tool_result").count(), 1);
        assert!(serialized.contains("local"));
        assert!(!serialized.contains("misplaced"));
        assert!(!serialized.contains("orphan"));
    }

    #[test]
    fn removes_duplicate_and_empty_historical_tool_use_ids() {
        let messages = vec![
            Message::user_text("question"),
            Message::assistant(vec![
                json!({"type":"tool_use","id":"same","name":"Read","input":{}}),
                json!({"type":"tool_use","id":"same","name":"Read","input":{}}),
                json!({"type":"tool_use","id":"","name":"Read","input":{}}),
            ]),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"same","content":"only"
            })]),
        ];

        let normalized = normalize_for_api(&messages);

        assert_eq!(
            blocks(&normalized[1])
                .iter()
                .filter(|block| block["type"] == "tool_use")
                .count(),
            1
        );
        assert_eq!(
            blocks(&normalized[2])
                .iter()
                .filter(|block| block["type"] == "tool_result")
                .count(),
            1
        );
    }

    #[test]
    fn inserts_a_result_turn_when_execution_was_interrupted() {
        let normalized = normalize_for_api(&[Message::assistant(vec![json!({
            "type":"tool_use","id":"t1","name":"Read","input":{}
        })])]);

        assert_eq!(normalized.len(), 3);
        assert_eq!(normalized[0].role, Role::User);
        assert_eq!(blocks(&normalized[2])[0]["tool_use_id"], "t1");
        assert_eq!(blocks(&normalized[2])[0]["is_error"], true);
    }

    #[test]
    fn removes_orphaned_tool_results() {
        let normalized = normalize_for_api(&[Message::tool_results(vec![json!({
            "type":"tool_result","tool_use_id":"missing","content":"x"
        })])]);
        assert!(normalized.is_empty());
    }
}
