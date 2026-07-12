use std::collections::HashSet;

use serde_json::{Value, json};

use crate::types::{Message, Role};

pub fn normalize_for_api(messages: &[Message]) -> Vec<Message> {
    let mut normalized = messages
        .iter()
        .filter_map(normalize_message)
        .collect::<Vec<_>>();
    repair_tool_pairing(&mut normalized);
    let mut merged = merge_adjacent_roles(normalized);
    if merged
        .first()
        .is_some_and(|message| message.role == Role::Assistant)
    {
        merged.insert(0, Message::user_text("Conversation resumed."));
    }
    merged
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

fn repair_tool_pairing(messages: &mut Vec<Message>) {
    let mut index = 0;
    while index < messages.len() {
        if messages[index].role != Role::Assistant {
            index += 1;
            continue;
        }
        let tool_ids = tool_use_ids(&messages[index].content);
        if tool_ids.is_empty() {
            index += 1;
            continue;
        }
        if messages
            .get(index + 1)
            .is_none_or(|message| message.role != Role::User)
        {
            messages.insert(index + 1, Message::tool_results(Vec::new()));
        }

        let next = &mut messages[index + 1];
        let mut tool_results = Vec::new();
        let mut remaining = Vec::new();
        for block in content_blocks(&next.content) {
            if block.get("type").and_then(Value::as_str) == Some("tool_result")
                && block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| tool_ids.iter().any(|expected| expected == id))
            {
                tool_results.push(block);
            } else {
                remaining.push(block);
            }
        }
        let result_ids = tool_results
            .iter()
            .filter_map(|block| {
                block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect::<HashSet<_>>();
        for id in tool_ids.iter().filter(|id| !result_ids.contains(*id)) {
            tool_results.push(json!({
                "type": "tool_result",
                "tool_use_id": id,
                "content": "Tool execution was interrupted before a result was recorded.",
                "is_error": true
            }));
        }
        tool_results.extend(remaining);
        next.content = Value::Array(tool_results);
        index += 2;
    }

    let all_tool_ids = messages
        .iter()
        .flat_map(|message| tool_use_ids(&message.content))
        .collect::<HashSet<_>>();
    for message in messages
        .iter_mut()
        .filter(|message| message.role == Role::User)
    {
        let mut blocks = content_blocks(&message.content);
        blocks.retain(|block| {
            block.get("type").and_then(Value::as_str) != Some("tool_result")
                || block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| all_tool_ids.contains(id))
        });
        message.content = Value::Array(blocks);
    }
    messages.retain(|message| !content_blocks(&message.content).is_empty());
}

fn tool_use_ids(content: &Value) -> Vec<String> {
    content_blocks(content)
        .into_iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .filter_map(|block| {
            block
                .get("id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
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

    #[test]
    fn merges_users_and_repairs_missing_tool_result() {
        let messages = vec![
            Message::assistant(vec![json!({
                "type":"tool_use","id":"t1","name":"Read","input":{}
            })]),
            Message::user_text("next"),
            Message::user_text("again"),
        ];
        let normalized = normalize_for_api(&messages);
        assert_eq!(normalized.len(), 3);
        let user = normalized[2].content.as_array().unwrap();
        assert!(user.iter().any(|block| block["tool_use_id"] == "t1"));
        assert_eq!(
            user.iter().filter(|block| block["type"] == "text").count(),
            2
        );
    }

    #[test]
    fn removes_orphaned_tool_results() {
        let normalized = normalize_for_api(&[Message::tool_results(vec![json!({
            "type":"tool_result","tool_use_id":"missing","content":"x"
        })])]);
        assert!(normalized.is_empty());
    }
}
