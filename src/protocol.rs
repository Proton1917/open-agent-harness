use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde_json::{Map, Value, json};

use crate::types::{Message, ModelResponse, Role, Usage};

const MAX_STREAM_EVENTS: usize = 100_000;
const MAX_CONTENT_BLOCKS: usize = 4_096;
const MAX_TOOL_ARGUMENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ApiFormat {
    /// Infer the wire protocol from the configured API path.
    Auto,
    /// Provider-neutral content-block Messages protocol.
    Messages,
    /// OpenAI-compatible Chat Completions protocol.
    #[value(name = "chat-completions", alias = "chat", alias = "openai-chat")]
    ChatCompletions,
    /// OpenAI-compatible Responses protocol.
    #[value(name = "responses", alias = "openai-responses")]
    Responses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ChatTokensField {
    /// Current Chat Completions field used by modern reasoning models.
    #[value(name = "max-completion-tokens")]
    MaxCompletionTokens,
    /// Legacy field used by older OpenAI-compatible servers.
    #[value(name = "max-tokens")]
    MaxTokens,
}

impl ApiFormat {
    pub fn infer(self, api_path: &str) -> Self {
        if self != Self::Auto {
            return self;
        }
        let path = api_path
            .split_once('?')
            .map_or(api_path, |(path, _)| path)
            .trim_end_matches('/')
            .to_ascii_lowercase();
        if path.ends_with("/chat/completions") {
            Self::ChatCompletions
        } else if path.ends_with("/responses") {
            Self::Responses
        } else {
            Self::Messages
        }
    }
}

pub struct RequestParts<'a> {
    pub model: &'a str,
    pub max_tokens: u32,
    pub system: &'a str,
    pub messages: &'a [Message],
    pub tools: &'a [Value],
    pub stream: bool,
    pub chat_tokens_field: ChatTokensField,
    pub include_stream_usage: bool,
}

pub fn encode_request(format: ApiFormat, request: RequestParts<'_>) -> Result<Value> {
    match format {
        ApiFormat::Messages => Ok(json!({
            "model": request.model,
            "max_tokens": request.max_tokens,
            "system": request.system,
            "messages": messages_without_provider_state(request.messages),
            "tools": request.tools,
            "stream": request.stream,
        })),
        ApiFormat::ChatCompletions => encode_chat_request(request),
        ApiFormat::Responses => encode_responses_request(request),
        ApiFormat::Auto => bail!("API format 必须在编码请求前完成解析"),
    }
}

pub fn parse_response(format: ApiFormat, value: Value) -> Result<ModelResponse> {
    common_response_error(&value)?;
    match format {
        ApiFormat::Messages => parse_messages_response(value),
        ApiFormat::ChatCompletions => parse_chat_response(&value),
        ApiFormat::Responses => parse_responses_response(&value),
        ApiFormat::Auto => bail!("API format 必须在解析响应前完成解析"),
    }
}

fn parse_messages_response(value: Value) -> Result<ModelResponse> {
    validate_messages_response_envelope(&value)?;
    let response: ModelResponse = serde_json::from_value(value).context("消息响应结构无效")?;
    if response.content.len() > MAX_CONTENT_BLOCKS {
        bail!("消息响应 content block 超过 {MAX_CONTENT_BLOCKS} 个限制")
    }
    Ok(response)
}

fn validate_messages_response_envelope(value: &Value) -> Result<()> {
    if value.get("type").and_then(Value::as_str) != Some("message") {
        bail!("Messages 响应 type 必须是 message")
    }
    if value.get("role").and_then(Value::as_str) != Some("assistant") {
        bail!("Messages 响应 role 必须是 assistant")
    }
    require_nonempty_string(value, "id", "Messages 响应")?;
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .context("Messages 响应 content 必须是 array")?;
    if content.len() > MAX_CONTENT_BLOCKS {
        bail!("消息响应 content block 超过 {MAX_CONTENT_BLOCKS} 个限制")
    }
    for block in content {
        let block_type = require_nonempty_string(block, "type", "Messages content block")?;
        if block_type == "tool_use" {
            validate_messages_tool_use(block)?;
        }
    }
    Ok(())
}

fn validate_messages_tool_use(block: &Value) -> Result<()> {
    require_nonempty_string(block, "id", "Messages tool_use")?;
    require_nonempty_string(block, "name", "Messages tool_use")?;
    if !block.get("input").is_some_and(Value::is_object) {
        bail!("Messages tool_use input 必须存在且是 object")
    }
    Ok(())
}

pub(crate) enum StreamDecoder {
    Messages(MessagesStream),
    ChatCompletions(ChatStream),
    Responses(ResponsesStream),
}

impl StreamDecoder {
    pub(crate) fn new(format: ApiFormat) -> Result<Self> {
        match format {
            ApiFormat::Messages => Ok(Self::Messages(MessagesStream::default())),
            ApiFormat::ChatCompletions => Ok(Self::ChatCompletions(ChatStream::default())),
            ApiFormat::Responses => Ok(Self::Responses(ResponsesStream::default())),
            ApiFormat::Auto => bail!("API format 必须在解析 stream 前完成解析"),
        }
    }

    pub(crate) fn apply(
        &mut self,
        event: Value,
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    ) -> Result<bool> {
        match self {
            Self::Messages(stream) => stream.apply(event, on_text_delta),
            Self::ChatCompletions(stream) => stream.apply(event, on_text_delta),
            Self::Responses(stream) => stream.apply(event, on_text_delta),
        }
    }

    pub(crate) fn mark_done(&mut self) -> Result<()> {
        match self {
            Self::Messages(_) => Ok(()),
            Self::ChatCompletions(stream) => stream.mark_done(),
            Self::Responses(stream) => stream.mark_done(),
        }
    }

    pub(crate) fn finish(self) -> Result<ModelResponse> {
        match self {
            Self::Messages(stream) => stream.finish(),
            Self::ChatCompletions(stream) => stream.finish(),
            Self::Responses(stream) => stream.finish(),
        }
    }
}

fn encode_chat_request(request: RequestParts<'_>) -> Result<Value> {
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.to_owned()));
    body.insert(
        match request.chat_tokens_field {
            ChatTokensField::MaxCompletionTokens => "max_completion_tokens",
            ChatTokensField::MaxTokens => "max_tokens",
        }
        .into(),
        Value::from(request.max_tokens),
    );
    body.insert(
        "messages".into(),
        Value::Array(chat_messages(request.system, request.messages)?),
    );
    let tools = function_tools(request.tools)?;
    if !tools.is_empty() {
        body.insert("tools".into(), Value::Array(tools));
    }
    body.insert("stream".into(), Value::Bool(request.stream));
    if request.stream && request.include_stream_usage {
        body.insert("stream_options".into(), json!({"include_usage": true}));
    }
    Ok(Value::Object(body))
}

fn encode_responses_request(request: RequestParts<'_>) -> Result<Value> {
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.to_owned()));
    body.insert("max_output_tokens".into(), Value::from(request.max_tokens));
    body.insert(
        "instructions".into(),
        Value::String(request.system.to_owned()),
    );
    body.insert(
        "input".into(),
        Value::Array(responses_input(request.messages)?),
    );
    let tools = responses_tools(request.tools)?;
    if !tools.is_empty() {
        body.insert("tools".into(), Value::Array(tools));
    }
    body.insert("stream".into(), Value::Bool(request.stream));
    body.insert("store".into(), Value::Bool(false));
    body.insert("include".into(), json!(["reasoning.encrypted_content"]));
    Ok(Value::Object(body))
}

fn messages_without_provider_state(messages: &[Message]) -> Vec<Message> {
    messages
        .iter()
        .filter_map(|message| {
            let content = match &message.content {
                Value::Array(blocks) => Value::Array(
                    blocks
                        .iter()
                        .filter(|block| {
                            block.get("type").and_then(Value::as_str) != Some("provider_state")
                        })
                        .cloned()
                        .collect(),
                ),
                other => other.clone(),
            };
            (!matches!(&content, Value::Array(blocks) if blocks.is_empty())).then_some(Message {
                role: message.role,
                content,
            })
        })
        .collect()
}

fn function_tools(tools: &[Value]) -> Result<Vec<Value>> {
    tools
        .iter()
        .map(|tool| {
            let object = tool.as_object().context("工具定义必须是 object")?;
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .context("工具定义缺少 name")?;
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let parameters = object
                .get("input_schema")
                .cloned()
                .unwrap_or_else(empty_object_schema);
            Ok(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                }
            }))
        })
        .collect()
}

fn responses_tools(tools: &[Value]) -> Result<Vec<Value>> {
    tools
        .iter()
        .map(|tool| {
            let object = tool.as_object().context("工具定义必须是 object")?;
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .context("工具定义缺少 name")?;
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let parameters = object
                .get("input_schema")
                .cloned()
                .unwrap_or_else(empty_object_schema);
            Ok(json!({
                "type": "function",
                "name": name,
                "description": description,
                "parameters": parameters,
            }))
        })
        .collect()
}

fn empty_object_schema() -> Value {
    json!({"type":"object","properties":{}})
}

fn chat_messages(system: &str, messages: &[Message]) -> Result<Vec<Value>> {
    let mut output = vec![json!({"role":"system","content":system})];
    for message in messages {
        match message.role {
            Role::User => append_chat_user_message(&mut output, &message.content)?,
            Role::Assistant => output.push(chat_assistant_message(&message.content)?),
        }
    }
    Ok(output)
}

fn append_chat_user_message(output: &mut Vec<Value>, content: &Value) -> Result<()> {
    if let Some(text) = content.as_str() {
        output.push(json!({"role":"user","content":text}));
        return Ok(());
    }
    let blocks = content
        .as_array()
        .context("user message content 必须是 string 或 array")?;
    let mut text = String::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => text.push_str(block.get("text").and_then(Value::as_str).unwrap_or("")),
            Some("tool_result") => {
                let call_id = block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .context("tool_result 缺少 tool_use_id")?;
                let result = tool_result_text(block);
                output.push(json!({
                    "role":"tool",
                    "tool_call_id":call_id,
                    "content":result,
                }));
            }
            _ => {}
        }
    }
    if !text.is_empty() {
        output.push(json!({"role":"user","content":text}));
    }
    Ok(())
}

fn chat_assistant_message(content: &Value) -> Result<Value> {
    if let Some(text) = content.as_str() {
        return Ok(json!({"role":"assistant","content":text}));
    }
    let blocks = content
        .as_array()
        .context("assistant message content 必须是 string 或 array")?;
    let mut text = String::new();
    let mut calls = Vec::new();
    let mut reasoning_details = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => text.push_str(block.get("text").and_then(Value::as_str).unwrap_or("")),
            Some("provider_state")
                if block.get("format").and_then(Value::as_str) == Some("chat-completions") =>
            {
                append_reasoning_details(&mut reasoning_details, block.get("reasoning_details"))?;
            }
            Some("tool_use") => {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 id")?;
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 name")?;
                let arguments =
                    serialize_arguments(block.get("input").unwrap_or(&Value::Object(Map::new())))?;
                calls.push(json!({
                    "id":id,
                    "type":"function",
                    "function":{"name":name,"arguments":arguments},
                }));
            }
            _ => {}
        }
    }
    let mut message = Map::new();
    message.insert("role".into(), Value::String("assistant".into()));
    message.insert(
        "content".into(),
        if text.is_empty() && !calls.is_empty() {
            Value::Null
        } else {
            Value::String(text)
        },
    );
    if !calls.is_empty() {
        message.insert("tool_calls".into(), Value::Array(calls));
    }
    if !reasoning_details.is_empty() {
        message.insert("reasoning_details".into(), Value::Array(reasoning_details));
    }
    Ok(Value::Object(message))
}

fn append_reasoning_details(output: &mut Vec<Value>, value: Option<&Value>) -> Result<()> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Array(details)) => {
            if output.len().saturating_add(details.len()) > MAX_CONTENT_BLOCKS {
                bail!("reasoning_details 超过 {MAX_CONTENT_BLOCKS} 个限制")
            }
            output.extend(details.iter().cloned());
            Ok(())
        }
        Some(_) => bail!("reasoning_details 必须是 array 或 null"),
    }
}

fn chat_provider_state(reasoning_details: Vec<Value>) -> Value {
    json!({
        "type":"provider_state",
        "format":"chat-completions",
        "reasoning_details":reasoning_details,
    })
}

fn responses_input(messages: &[Message]) -> Result<Vec<Value>> {
    let mut output = Vec::new();
    let mut fallback_message_index = 0usize;
    for message in messages {
        if let Some(text) = message.content.as_str() {
            output.push(response_message_item(
                message.role,
                text,
                &mut fallback_message_index,
            ));
            continue;
        }
        let blocks = message
            .content
            .as_array()
            .context("message content 必须是 string 或 array")?;
        let mut text = String::new();
        let mut replayed_calls = HashSet::new();
        let mut replayed_text = VecDeque::new();
        for block in blocks {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let block_text = block.get("text").and_then(Value::as_str).unwrap_or("");
                    if replayed_text
                        .front()
                        .is_some_and(|expected| expected == block_text)
                    {
                        replayed_text.pop_front();
                    } else {
                        text.push_str(block_text);
                    }
                }
                Some("tool_use") if message.role == Role::Assistant => {
                    flush_response_text(
                        &mut output,
                        &mut text,
                        message.role,
                        &mut fallback_message_index,
                    );
                    let call_id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .context("tool_use 缺少 id")?;
                    if replayed_calls.remove(call_id) {
                        continue;
                    }
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .context("tool_use 缺少 name")?;
                    let arguments = serialize_arguments(
                        block.get("input").unwrap_or(&Value::Object(Map::new())),
                    )?;
                    output.push(json!({
                        "type":"function_call",
                        "id":call_id,
                        "call_id":call_id,
                        "name":name,
                        "arguments":arguments,
                    }));
                }
                Some("tool_result") if message.role == Role::User => {
                    flush_response_text(
                        &mut output,
                        &mut text,
                        message.role,
                        &mut fallback_message_index,
                    );
                    let call_id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .context("tool_result 缺少 tool_use_id")?;
                    output.push(json!({
                        "type":"function_call_output",
                        "call_id":call_id,
                        "output":tool_result_text(block),
                    }));
                }
                Some("provider_state") if message.role == Role::Assistant => {
                    flush_response_text(
                        &mut output,
                        &mut text,
                        message.role,
                        &mut fallback_message_index,
                    );
                    let Some(item) = replayable_response_item(block) else {
                        continue;
                    };
                    match item.get("type").and_then(Value::as_str) {
                        Some("function_call") => {
                            if let Some(call_id) = item.get("call_id").and_then(Value::as_str) {
                                replayed_calls.insert(call_id.to_owned());
                            }
                        }
                        Some("message") => {
                            let covered = response_message_text(&Value::Object(item.clone()));
                            if !covered.is_empty() {
                                replayed_text.push_back(covered);
                            }
                        }
                        _ => {}
                    }
                    output.push(Value::Object(item.clone()));
                }
                _ => {}
            }
        }
        flush_response_text(
            &mut output,
            &mut text,
            message.role,
            &mut fallback_message_index,
        );
    }
    Ok(output)
}

fn replayable_response_item(block: &Value) -> Option<&Map<String, Value>> {
    if block.get("format").and_then(Value::as_str) != Some("responses") {
        return None;
    }
    let item = block.get("item")?.as_object()?;
    match item.get("type").and_then(Value::as_str) {
        Some("reasoning")
            if item
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_some_and(|content| !content.is_empty()) =>
        {
            Some(item)
        }
        Some("function_call" | "message") => Some(item),
        _ => None,
    }
}

fn flush_response_text(
    output: &mut Vec<Value>,
    text: &mut String,
    role: Role,
    fallback_message_index: &mut usize,
) {
    if !text.is_empty() {
        output.push(response_message_item(role, text, fallback_message_index));
        text.clear();
    }
}

fn response_message_item(role: Role, text: &str, fallback_message_index: &mut usize) -> Value {
    match role {
        Role::User => json!({
            "type":"message",
            "role":"user",
            "content":[{"type":"input_text","text":text}],
        }),
        Role::Assistant => {
            let id = format!("msg_local_{}", *fallback_message_index);
            *fallback_message_index = fallback_message_index.saturating_add(1);
            json!({
                "type":"message",
                "id":id,
                "status":"completed",
                "role":"assistant",
                "content":[{"type":"output_text","text":text}],
            })
        }
    }
}

fn tool_result_text(block: &Value) -> String {
    let content = block.get("content").unwrap_or(&Value::Null);
    let text = value_as_text(content);
    if block
        .get("is_error")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        format!("Error: {text}")
    } else {
        text
    }
}

fn value_as_text(value: &Value) -> String {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()))
}

fn parse_chat_response(value: &Value) -> Result<ModelResponse> {
    let choices = value
        .get("choices")
        .and_then(Value::as_array)
        .context("Chat Completions 响应缺少 choices array")?;
    if choices.len() != 1 {
        bail!("Chat Completions 响应必须只包含一个 choice")
    }
    let choice = &choices[0];
    if choice.get("index").and_then(Value::as_u64) != Some(0) {
        bail!("Chat Completions choice index 必须是 0")
    }
    if let Some(error) = choice.get("error").filter(|error| !error.is_null()) {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Chat Completions choice 返回未知错误");
        bail!("Model response error: {message}")
    }
    let message = choice
        .get("message")
        .and_then(Value::as_object)
        .context("Chat Completions 响应缺少 message")?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        bail!("Chat Completions message role 必须是 assistant")
    }
    let has_modern_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty());
    let has_legacy_call = message
        .get("function_call")
        .is_some_and(|call| !call.is_null());
    if has_modern_calls && has_legacy_call {
        bail!("Chat Completions 禁止混用 tool_calls 与 legacy function_call")
    }
    let mut content = Vec::new();
    let mut reasoning_details = Vec::new();
    append_reasoning_details(&mut reasoning_details, message.get("reasoning_details"))?;
    if !reasoning_details.is_empty() {
        content.push(chat_provider_state(reasoning_details));
    }
    append_chat_content(&mut content, message.get("content"))?;
    append_chat_tool_calls(&mut content, message.get("tool_calls"))?;
    if content.iter().all(|block| block["type"] != "tool_use") {
        append_legacy_function_call(&mut content, message.get("function_call"))?;
    }
    let finish_reason = choice
        .get("finish_reason")
        .and_then(Value::as_str)
        .context("Chat Completions 响应缺少 finish_reason")?;
    if finish_reason == "error" {
        bail!("Chat Completions response 以 error 结束")
    }
    let has_tools = content.iter().any(|block| block["type"] == "tool_use");
    if has_tools && !matches!(finish_reason, "tool_calls" | "function_call") {
        bail!("Chat Completions 返回工具调用，但 finish_reason 不是 tool_calls")
    }
    if !has_tools && matches!(finish_reason, "tool_calls" | "function_call") {
        bail!("Chat Completions 以 tool_calls 结束，但没有完整工具调用")
    }
    let stop_reason = Some(canonical_stop_reason(finish_reason));
    Ok(ModelResponse {
        id: response_id(value),
        content,
        stop_reason,
        usage: chat_usage(value.get("usage")),
    })
}

fn append_chat_content(output: &mut Vec<Value>, content: Option<&Value>) -> Result<()> {
    match content {
        Some(Value::String(text)) if !text.is_empty() => {
            output.push(json!({"type":"text","text":text}));
        }
        Some(Value::Array(parts)) => {
            if parts.len() > MAX_CONTENT_BLOCKS {
                bail!("Chat message content 超过 {MAX_CONTENT_BLOCKS} 个限制")
            }
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        output.push(json!({"type":"text","text":text}));
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn append_chat_tool_calls(output: &mut Vec<Value>, calls: Option<&Value>) -> Result<()> {
    let calls = match calls {
        None | Some(Value::Null) => return Ok(()),
        Some(Value::Array(calls)) => calls,
        Some(_) => bail!("Chat message.tool_calls 必须是 array 或 null"),
    };
    if calls.len() > MAX_CONTENT_BLOCKS {
        bail!("Chat tool_call 超过 {MAX_CONTENT_BLOCKS} 个限制")
    }
    for call in calls {
        if call.get("type").and_then(Value::as_str) != Some("function") {
            bail!("tool_call type 必须存在且是 function")
        }
        let function = call
            .get("function")
            .and_then(Value::as_object)
            .context("tool_call 缺少 function")?;
        let name = function
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .context("tool_call 缺少 function.name")?;
        let input = parse_arguments(function.get("arguments"))?;
        let id = call
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .context("tool_call 缺少非空 id")?;
        output.push(json!({"type":"tool_use","id":id,"name":name,"input":input}));
    }
    Ok(())
}

fn append_legacy_function_call(output: &mut Vec<Value>, call: Option<&Value>) -> Result<()> {
    let Some(call) = call.and_then(Value::as_object) else {
        return Ok(());
    };
    let name = call
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .context("function_call 缺少 name")?;
    let input = parse_arguments(call.get("arguments"))?;
    output.push(json!({"type":"tool_use","id":"call_0","name":name,"input":input}));
    Ok(())
}

fn parse_responses_response(value: &Value) -> Result<ModelResponse> {
    common_response_error(value)?;
    match value.get("status").and_then(Value::as_str) {
        Some("completed") => {}
        Some(status) => {
            let detail = value
                .pointer("/incomplete_details/reason")
                .or_else(|| value.pointer("/error/message"))
                .and_then(Value::as_str)
                .unwrap_or("no completion detail");
            bail!("Responses response ended with status {status}: {detail}")
        }
        None => bail!("Responses 响应缺少 completed status"),
    }
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .context("Responses 响应缺少 output")?;
    if output.len() > MAX_CONTENT_BLOCKS {
        bail!("Responses output item 超过 {MAX_CONTENT_BLOCKS} 个限制")
    }
    let mut content = Vec::new();
    let mut item_ids = HashSet::with_capacity(output.len());
    for item in output {
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            if id.is_empty() || !item_ids.insert(id) {
                bail!("Responses output item id 为空或重复")
            }
        }
        append_response_item(&mut content, item)?;
    }
    let has_tools = content.iter().any(|block| block["type"] == "tool_use");
    let stop_reason = Some(if has_tools { "tool_use" } else { "end_turn" }.to_owned());
    Ok(ModelResponse {
        id: response_id(value),
        content,
        stop_reason,
        usage: responses_usage(value.get("usage")),
    })
}

fn append_response_item(content: &mut Vec<Value>, item: &Value) -> Result<()> {
    match item.get("type").and_then(Value::as_str) {
        Some("message") => {
            validate_response_message(item)?;
            content.push(response_provider_state(item));
            let text = response_message_text(item);
            if !text.is_empty() {
                content.push(json!({"type":"text","text":text}));
            }
        }
        Some("function_call") => {
            validate_completed_function_call_status(item)?;
            require_nonempty_string(item, "id", "function_call")?;
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .filter(|call_id| !call_id.is_empty())
                .context("function_call 缺少 call_id")?;
            let name = item
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .context("function_call 缺少 name")?;
            let input = parse_arguments(item.get("arguments"))?;
            content.push(response_provider_state(item));
            content.push(json!({
                "type":"tool_use","id":call_id,"name":name,"input":input,
            }));
        }
        Some("reasoning")
            if item
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_some_and(|encrypted| !encrypted.is_empty()) =>
        {
            require_nonempty_string(item, "id", "Responses reasoning item")?;
            content.push(response_provider_state(item));
        }
        _ => {}
    }
    Ok(())
}

fn response_provider_state(item: &Value) -> Value {
    json!({
        "type":"provider_state",
        "format":"responses",
        "item":item,
    })
}

fn response_message_text(item: &Value) -> String {
    item.get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| match part.get("type").and_then(Value::as_str) {
            Some("output_text" | "text") => part.get("text").and_then(Value::as_str),
            Some("refusal") => part.get("refusal").and_then(Value::as_str),
            _ => None,
        })
        .collect()
}

fn validate_response_message(item: &Value) -> Result<()> {
    require_nonempty_string(item, "id", "Responses message")?;
    if item.get("status").and_then(Value::as_str) != Some("completed") {
        bail!("Responses message 缺少 completed status")
    }
    if item.get("role").and_then(Value::as_str) != Some("assistant") {
        bail!("Responses output message role 必须是 assistant")
    }
    let parts = item
        .get("content")
        .and_then(Value::as_array)
        .context("Responses message 缺少 content array")?;
    if parts.len() > MAX_CONTENT_BLOCKS {
        bail!("Responses message content 超过 {MAX_CONTENT_BLOCKS} 个限制")
    }
    Ok(())
}

fn validate_completed_function_call_status(item: &Value) -> Result<()> {
    match item.get("status") {
        None => Ok(()),
        Some(Value::String(status)) if status == "completed" => Ok(()),
        Some(_) => bail!("Responses completed function_call status 若存在必须是 completed"),
    }
}

fn require_nonempty_string<'a>(value: &'a Value, field: &str, kind: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .with_context(|| format!("{kind} 缺少非空 {field}"))
}

fn response_id(value: &Value) -> String {
    value
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("response")
        .to_owned()
}

fn parse_arguments(value: Option<&Value>) -> Result<Value> {
    let parsed = match value {
        None | Some(Value::Null) => bail!("工具 arguments 缺失"),
        Some(Value::String(arguments)) if arguments.trim().is_empty() => {
            bail!("工具 arguments 不能为空")
        }
        Some(Value::String(arguments)) => {
            check_tool_argument_bytes(arguments.len())?;
            serde_json::from_str(arguments).context("工具 arguments 不是有效 JSON")
        }
        Some(value @ Value::Object(_)) => {
            let encoded = serde_json::to_vec(value).context("无法编码工具 arguments")?;
            check_tool_argument_bytes(encoded.len())?;
            Ok(value.clone())
        }
        Some(_) => bail!("工具 arguments 必须是 JSON string 或 object"),
    }?;
    if !parsed.is_object() {
        bail!("工具 arguments 必须解析为 JSON object")
    }
    Ok(parsed)
}

fn serialize_arguments(value: &Value) -> Result<String> {
    if !value.is_object() {
        bail!("工具 input 必须是 JSON object")
    }
    let encoded = serde_json::to_string(value).context("无法编码工具 input")?;
    check_tool_argument_bytes(encoded.len())?;
    Ok(encoded)
}

fn check_tool_argument_bytes(bytes: usize) -> Result<()> {
    if bytes > MAX_TOOL_ARGUMENT_BYTES {
        bail!("工具 arguments 超过 {MAX_TOOL_ARGUMENT_BYTES} 字节限制")
    }
    Ok(())
}

fn common_response_error(value: &Value) -> Result<()> {
    let Some(error) = value.get("error").filter(|error| !error.is_null()) else {
        return Ok(());
    };
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
        .unwrap_or("model endpoint 返回未知错误");
    bail!("Model stream error: {message}")
}

fn validate_completed_response(value: &Value) -> Result<()> {
    common_response_error(value)?;
    match value.get("status").and_then(Value::as_str) {
        Some("completed") => Ok(()),
        Some(status) => bail!("Responses terminal response status 不是 completed: {status}"),
        None => bail!("Responses terminal response 缺少 completed status"),
    }
}

fn canonical_stop_reason(reason: &str) -> String {
    match reason {
        "tool_calls" | "function_call" => "tool_use".to_owned(),
        other => other.to_owned(),
    }
}

fn chat_usage(value: Option<&Value>) -> Option<Usage> {
    let value = value?.as_object()?;
    Some(Usage {
        input_tokens: nullable_u64(value.get("prompt_tokens")),
        output_tokens: nullable_u64(value.get("completion_tokens")),
        cache_creation_input_tokens: nullable_u64(
            value
                .get("prompt_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("cache_write_tokens")),
        ),
        cache_read_input_tokens: nullable_u64(
            value
                .get("prompt_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("cached_tokens")),
        ),
    })
}

fn responses_usage(value: Option<&Value>) -> Option<Usage> {
    let value = value?.as_object()?;
    Some(Usage {
        input_tokens: nullable_u64(value.get("input_tokens")),
        output_tokens: nullable_u64(value.get("output_tokens")),
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: nullable_u64(
            value
                .get("input_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("cached_tokens")),
        ),
    })
}

fn nullable_u64(value: Option<&Value>) -> u64 {
    value.and_then(Value::as_u64).unwrap_or(0)
}

#[derive(Default)]
pub(crate) struct MessagesStream {
    id: Option<String>,
    blocks: BTreeMap<usize, Value>,
    active_blocks: BTreeSet<usize>,
    partial_json: HashMap<usize, String>,
    stop_reason: Option<String>,
    usage: Option<Usage>,
    started: bool,
    message_delta_seen: bool,
    stopped: bool,
    event_count: usize,
}

impl MessagesStream {
    fn apply(
        &mut self,
        event: Value,
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    ) -> Result<bool> {
        bump_event_count(&mut self.event_count)?;
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        if self.stopped && event_type != "ping" {
            bail!("message_stop 之后收到额外 SSE 事件")
        }
        match event_type {
            "message_start" => {
                if self.started {
                    bail!("SSE stream 包含重复 message_start")
                }
                let message = event
                    .get("message")
                    .and_then(Value::as_object)
                    .context("message_start 缺少 message object")?;
                if message.get("type").and_then(Value::as_str) != Some("message") {
                    bail!("message_start.message type 必须是 message")
                }
                if message.get("role").and_then(Value::as_str) != Some("assistant") {
                    bail!("message_start.message role 必须是 assistant")
                }
                let id = message
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .context("message_start.message 缺少非空 id")?;
                let content = message
                    .get("content")
                    .and_then(Value::as_array)
                    .context("message_start.message content 必须是 array")?;
                if !content.is_empty() {
                    bail!("message_start.message content 必须是空 array")
                }
                self.started = true;
                self.id = Some(id.to_owned());
                self.usage = message
                    .get("usage")
                    .filter(|usage| !usage.is_null())
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?;
            }
            "content_block_start" => {
                self.require_started()?;
                self.require_before_message_delta()?;
                let index = event_index(&event)?;
                if self.blocks.contains_key(&index) || !self.active_blocks.insert(index) {
                    bail!("SSE stream 包含重复 content block index {index}")
                }
                if self.blocks.len() >= MAX_CONTENT_BLOCKS {
                    bail!("SSE content block 超过 {MAX_CONTENT_BLOCKS} 个限制")
                }
                let block = event
                    .get("content_block")
                    .cloned()
                    .context("content_block_start 缺少 content_block")?;
                validate_messages_stream_block_start(&block)?;
                let initial_text = if block.get("type").and_then(Value::as_str) == Some("text") {
                    block
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned()
                } else {
                    String::new()
                };
                self.blocks.insert(index, block);
                if !initial_text.is_empty() {
                    if let Some(callback) = on_text_delta {
                        callback(&initial_text);
                    }
                    return Ok(true);
                }
            }
            "content_block_delta" => {
                self.require_started()?;
                self.require_before_message_delta()?;
                let index = event_index(&event)?;
                if !self.active_blocks.contains(&index) {
                    bail!("SSE delta 对应的 content block 未处于打开状态")
                }
                let delta = event
                    .get("delta")
                    .context("content_block_delta 缺少 delta")?;
                let block_type = self
                    .blocks
                    .get(&index)
                    .and_then(|block| block.get("type"))
                    .and_then(Value::as_str)
                    .context("SSE content block 缺少 type")?;
                let delta_type = require_nonempty_string(delta, "type", "Messages content delta")?;
                match (block_type, delta_type) {
                    ("text", "text_delta") => {
                        let text = delta
                            .get("text")
                            .and_then(Value::as_str)
                            .context("text_delta 缺少 string text")?;
                        append_string(self.blocks.get_mut(&index), "text", text)?;
                        if let Some(callback) = on_text_delta {
                            callback(text);
                        }
                        return Ok(!text.is_empty());
                    }
                    ("tool_use", "input_json_delta") => {
                        let partial = self.partial_json.entry(index).or_default();
                        partial.push_str(
                            delta
                                .get("partial_json")
                                .and_then(Value::as_str)
                                .context("input_json_delta 缺少 string partial_json")?,
                        );
                        if partial.len() > MAX_TOOL_ARGUMENT_BYTES {
                            bail!("stream tool arguments 超过 {MAX_TOOL_ARGUMENT_BYTES} 字节限制")
                        }
                    }
                    ("thinking", "thinking_delta") => append_string(
                        self.blocks.get_mut(&index),
                        "thinking",
                        delta
                            .get("thinking")
                            .and_then(Value::as_str)
                            .context("thinking_delta 缺少 string thinking")?,
                    )?,
                    ("thinking", "signature_delta") => append_string(
                        self.blocks.get_mut(&index),
                        "signature",
                        delta
                            .get("signature")
                            .and_then(Value::as_str)
                            .context("signature_delta 缺少 string signature")?,
                    )?,
                    _ => bail!(
                        "Messages delta type {delta_type} 与 content block type {block_type} 不匹配"
                    ),
                }
            }
            "content_block_stop" => {
                self.require_started()?;
                self.require_before_message_delta()?;
                let index = event_index(&event)?;
                if !self.active_blocks.remove(&index) {
                    bail!("SSE content_block_stop 没有对应的打开 block")
                }
                if let Some(partial) = self.partial_json.remove(&index) {
                    let input: Value =
                        serde_json::from_str(&partial).context("tool input JSON 拼接失败")?;
                    if !input.is_object() {
                        bail!("stream tool_use input 必须是 object")
                    }
                    self.blocks
                        .get_mut(&index)
                        .and_then(Value::as_object_mut)
                        .context("tool_use content block 不是 object")?
                        .insert("input".into(), input);
                }
                if self
                    .blocks
                    .get(&index)
                    .and_then(|block| block.get("type"))
                    .and_then(Value::as_str)
                    == Some("tool_use")
                {
                    validate_messages_tool_use(
                        self.blocks
                            .get(&index)
                            .context("stream tool_use content block 丢失")?,
                    )?;
                }
            }
            "message_delta" => {
                self.require_started()?;
                if self.message_delta_seen {
                    bail!("SSE stream 包含重复 message_delta")
                }
                if !self.active_blocks.is_empty() {
                    bail!("content block 尚未结束时收到 message_delta")
                }
                self.message_delta_seen = true;
                self.stop_reason = event
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if let Some(usage) = event.get("usage") {
                    merge_messages_usage(&mut self.usage, usage)?;
                }
            }
            "error" => {
                common_response_error(&event)?;
                bail!("Messages stream 返回失败事件")
            }
            "message_stop" => {
                self.require_started()?;
                if !self.active_blocks.is_empty() {
                    bail!("SSE 在 content block 结束前收到 message_stop")
                }
                self.stopped = true;
            }
            "ping" => {}
            _ => {}
        }
        Ok(false)
    }

    fn finish(self) -> Result<ModelResponse> {
        if !self.started || !self.stopped {
            bail!("SSE stream 在 message_stop 前中断")
        }
        if !self.active_blocks.is_empty() {
            bail!("SSE stream 存在未结束的 content block")
        }
        if !self.partial_json.is_empty() {
            bail!("SSE 在工具输入 JSON 完成前中断")
        }
        for (expected, index) in self.blocks.keys().copied().enumerate() {
            if index != expected {
                bail!("Messages content block index 必须连续且从 0 开始")
            }
        }
        for block in self.blocks.values() {
            if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                validate_messages_tool_use(block)?;
            }
        }
        Ok(ModelResponse {
            id: self.id.context("SSE 流缺少 message_start.id")?,
            content: self.blocks.into_values().collect(),
            stop_reason: self.stop_reason,
            usage: self.usage,
        })
    }

    fn require_started(&self) -> Result<()> {
        if !self.started {
            bail!("SSE content event 出现在 message_start 之前")
        }
        Ok(())
    }

    fn require_before_message_delta(&self) -> Result<()> {
        if self.message_delta_seen {
            bail!("message_delta 之后收到 content block 事件")
        }
        Ok(())
    }
}

fn validate_messages_stream_block_start(block: &Value) -> Result<()> {
    let block_type = require_nonempty_string(block, "type", "Messages content block")?;
    match block_type {
        "text" => {
            if !block.get("text").is_some_and(Value::is_string) {
                bail!("Messages text block 必须包含 string text")
            }
        }
        "tool_use" => validate_messages_tool_use(block)?,
        "thinking" => {
            if !block.get("thinking").is_some_and(Value::is_string) {
                bail!("Messages thinking block 必须包含 string thinking")
            }
            if block
                .get("signature")
                .is_some_and(|signature| !signature.is_string())
            {
                bail!("Messages thinking block signature 若存在必须是 string")
            }
        }
        "redacted_thinking" if !block.get("data").is_some_and(Value::is_string) => {
            bail!("Messages redacted_thinking block 必须包含 string data")
        }
        "redacted_thinking" => {}
        _ => {}
    }
    Ok(())
}

#[derive(Default)]
struct ChatToolCall {
    id: Option<String>,
    name: String,
    arguments: String,
}

#[derive(Default)]
pub(crate) struct ChatStream {
    id: Option<String>,
    text: String,
    calls: BTreeMap<usize, ChatToolCall>,
    reasoning_details: Vec<Value>,
    stop_reason: Option<String>,
    usage: Option<Usage>,
    saw_modern_calls: bool,
    saw_legacy_call: bool,
    done: bool,
    event_count: usize,
}

impl ChatStream {
    fn apply(
        &mut self,
        event: Value,
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    ) -> Result<bool> {
        bump_event_count(&mut self.event_count)?;
        if self.done {
            bail!("[DONE] 之后收到额外 Chat Completions 事件")
        }
        common_response_error(&event)?;
        if let Some(id) = event.get("id").and_then(Value::as_str) {
            set_consistent_string(&mut self.id, id, "Chat response id")?;
        }
        if let Some(usage) = chat_usage(event.get("usage")) {
            self.usage = Some(usage);
        }
        let Some(choices) = event.get("choices").and_then(Value::as_array) else {
            return Ok(false);
        };
        if choices.is_empty() {
            return Ok(false);
        }
        if choices.len() != 1 {
            bail!("Chat Completions stream 必须只包含一个 choice")
        }
        if self.stop_reason.is_some() {
            bail!("Chat finish_reason 之后收到额外 choice")
        }
        let choice = &choices[0];
        if choice.get("index").and_then(Value::as_u64) != Some(0) {
            bail!("Chat Completions stream choice index 必须是 0")
        }
        if let Some(error) = choice.get("error").filter(|error| !error.is_null()) {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Chat Completions choice 返回未知错误");
            bail!("Model stream error: {message}")
        }
        let choice_reason = choice.get("finish_reason").and_then(Value::as_str);
        let delta = choice.get("delta").and_then(Value::as_object);
        let delta_reason = delta
            .and_then(|delta| delta.get("finish_reason"))
            .and_then(Value::as_str);
        if let (Some(left), Some(right)) = (choice_reason, delta_reason) {
            if left != right {
                bail!("Chat stream 包含冲突的 finish_reason")
            }
        }
        let mut streamed = false;
        if let Some(delta) = delta {
            if let Some(role) = delta.get("role").and_then(Value::as_str) {
                if role != "assistant" {
                    bail!("Chat stream delta role 必须是 assistant")
                }
            }
            if let Some(content) = delta.get("content") {
                if !content.is_null() {
                    let text = content
                        .as_str()
                        .context("Chat delta.content 必须是 string 或 null")?;
                    self.text.push_str(text);
                    if let Some(callback) = on_text_delta {
                        callback(text);
                    }
                    streamed |= !text.is_empty();
                }
            }
            append_reasoning_details(&mut self.reasoning_details, delta.get("reasoning_details"))?;
            if let Some(calls) = delta.get("tool_calls") {
                let calls = calls
                    .as_array()
                    .context("Chat delta.tool_calls 必须是 array")?;
                if calls.len() > MAX_CONTENT_BLOCKS {
                    bail!("Chat tool_call 超过 {MAX_CONTENT_BLOCKS} 个限制")
                }
                if !calls.is_empty() {
                    if self.saw_legacy_call {
                        bail!("Chat stream 禁止混用 tool_calls 与 legacy function_call")
                    }
                    self.saw_modern_calls = true;
                }
                let mut chunk_indexes = HashSet::with_capacity(calls.len());
                for call in calls {
                    if let Some(call_type) = call.get("type") {
                        if call_type.as_str() != Some("function") {
                            bail!("Chat stream tool_call type 若存在必须是 function")
                        }
                    }
                    let index = call
                        .get("index")
                        .and_then(Value::as_u64)
                        .context("Chat tool_call 缺少非负 index")?;
                    let index =
                        usize::try_from(index).context("Chat tool_call index 超出平台范围")?;
                    if index >= MAX_CONTENT_BLOCKS {
                        bail!("Chat tool_call index 超过 {MAX_CONTENT_BLOCKS} 限制")
                    }
                    if !chunk_indexes.insert(index) {
                        bail!("Chat stream 同一 chunk 包含重复 tool_call index {index}")
                    }
                    if !self.calls.contains_key(&index) && self.calls.len() >= MAX_CONTENT_BLOCKS {
                        bail!("stream tool call 超过 {MAX_CONTENT_BLOCKS} 个限制")
                    }
                    let target = self.calls.entry(index).or_default();
                    if let Some(id) = call.get("id").and_then(Value::as_str) {
                        set_consistent_string(&mut target.id, id, "Chat tool_call id")?;
                    }
                    if let Some(function) = call.get("function") {
                        let function = function
                            .as_object()
                            .context("Chat tool_call.function 必须是 object")?;
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            target.name.push_str(name);
                        }
                        if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                            append_tool_argument_delta(&mut target.arguments, arguments)?;
                        }
                    }
                }
            }
            if let Some(function) = delta.get("function_call") {
                if self.saw_modern_calls {
                    bail!("Chat stream 禁止混用 tool_calls 与 legacy function_call")
                }
                self.saw_legacy_call = true;
                let function = function
                    .as_object()
                    .context("Chat delta.function_call 必须是 object")?;
                let target = self.calls.entry(0).or_default();
                target.id.get_or_insert_with(|| "call_0".to_owned());
                if let Some(name) = function.get("name").and_then(Value::as_str) {
                    target.name.push_str(name);
                }
                if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                    append_tool_argument_delta(&mut target.arguments, arguments)?;
                }
            }
        }
        if let Some(reason) = choice_reason.or(delta_reason) {
            if reason == "error" {
                bail!("Chat Completions stream 以 error 结束")
            }
            self.stop_reason = Some(canonical_stop_reason(reason));
        }
        Ok(streamed)
    }

    fn mark_done(&mut self) -> Result<()> {
        if self.done {
            bail!("Chat Completions stream 包含重复 [DONE]")
        }
        self.done = true;
        Ok(())
    }

    fn finish(self) -> Result<ModelResponse> {
        if !self.done {
            bail!("Chat Completions stream 在 [DONE] 前中断")
        }
        if self.stop_reason.is_none() {
            bail!("Chat Completions stream 缺少 finish_reason")
        }
        let mut content = Vec::new();
        if !self.reasoning_details.is_empty() {
            content.push(chat_provider_state(self.reasoning_details));
        }
        if !self.text.is_empty() {
            content.push(json!({"type":"text","text":self.text}));
        }
        let has_calls = !self.calls.is_empty();
        if has_calls && self.stop_reason.as_deref() != Some("tool_use") {
            bail!("Chat stream 返回工具调用，但 finish_reason 不是 tool_calls")
        }
        if !has_calls && self.stop_reason.as_deref() == Some("tool_use") {
            bail!("Chat stream 以 tool_calls 结束，但没有完整工具调用")
        }
        for (expected, index) in self.calls.keys().copied().enumerate() {
            if index != expected {
                bail!("Chat stream tool_call index 必须连续且从 0 开始")
            }
        }
        for (index, call) in self.calls {
            if call.name.is_empty() {
                bail!("stream tool_call 缺少 function.name")
            }
            let id = call
                .id
                .with_context(|| format!("stream tool_call index {index} 缺少 id"))?;
            let input = parse_arguments(Some(&Value::String(call.arguments)))?;
            content.push(json!({
                "type":"tool_use",
                "id":id,
                "name":call.name,
                "input":input,
            }));
        }
        Ok(ModelResponse {
            id: self.id.unwrap_or_else(|| "stream-response".to_owned()),
            content,
            stop_reason: self.stop_reason,
            usage: self.usage,
        })
    }
}

#[derive(Default)]
struct ResponseCall {
    call_id: Option<String>,
    name: String,
    arguments: String,
    arguments_done: bool,
}

#[derive(Default)]
struct ResponseContentPart {
    part_type: String,
    streamed: String,
    output_text_done: Option<String>,
    content_part_done: bool,
    content_part_done_text: Option<String>,
}

#[derive(Default)]
pub(crate) struct ResponsesStream {
    id: Option<String>,
    content_parts: BTreeMap<(usize, usize), ResponseContentPart>,
    calls: BTreeMap<usize, ResponseCall>,
    items: BTreeMap<usize, Value>,
    item_indices: HashMap<String, usize>,
    added_items: BTreeSet<usize>,
    completed_items: BTreeSet<usize>,
    final_response: Option<Value>,
    created: bool,
    terminal: bool,
    saw_done: bool,
    sequence_mode: Option<bool>,
    last_sequence: Option<u64>,
    event_count: usize,
}

impl ResponsesStream {
    fn apply(
        &mut self,
        event: Value,
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    ) -> Result<bool> {
        bump_event_count(&mut self.event_count)?;
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        if self.saw_done {
            bail!("Responses [DONE] 之后收到额外事件")
        }
        if self.terminal {
            bail!("Responses terminal event 之后收到额外事件")
        }
        self.validate_sequence(event_type, &event)?;
        if response_event_requires_created(event_type) && !self.created {
            bail!("Responses event {event_type} 出现在 response.created 之前")
        }
        if let Some(response_id) = event.get("response_id").and_then(Value::as_str) {
            set_consistent_string(&mut self.id, response_id, "Responses response id")?;
        }
        match event_type {
            "error" | "response.error" | "response.failed" => {
                if let Some(response) = event.get("response") {
                    common_response_error(response)?;
                }
                common_response_error(&event)?;
                bail!("Responses stream 返回失败事件")
            }
            "response.created" => {
                if self.created {
                    bail!("Responses stream 包含重复 response.created")
                }
                self.created = true;
                let response = event
                    .get("response")
                    .context("response.created 缺少 response")?;
                self.capture_id(response)?;
            }
            "response.in_progress" => {
                if let Some(response) = event.get("response") {
                    self.capture_id(response)?;
                }
            }
            "response.output_item.added" => {
                let index = output_index(&event)?;
                if !self.added_items.insert(index) {
                    bail!("Responses output item {index} 重复 added")
                }
                let item = event
                    .get("item")
                    .context("response.output_item.added 缺少 item")?;
                self.capture_item(index, item, false)?;
            }
            "response.output_item.done" => {
                let index = output_index(&event)?;
                let item = event
                    .get("item")
                    .context("response.output_item.done 缺少 item")?;
                self.capture_item(index, item, true)?;
            }
            "response.content_part.added" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "message")?;
                self.require_open_item(index)?;
                let content_index = content_index(&event)?;
                let key = (index, content_index);
                if self.content_parts.contains_key(&key) {
                    bail!("Responses content part ({index}, {content_index}) 重复 added")
                }
                let part = event
                    .get("part")
                    .context("response.content_part.added 缺少 part")?;
                let part_type = require_nonempty_string(part, "type", "Responses content part")?;
                let initial = response_part_text(part, part_type)?.unwrap_or_default();
                self.content_parts.insert(
                    key,
                    ResponseContentPart {
                        part_type: part_type.to_owned(),
                        streamed: initial.to_owned(),
                        ..ResponseContentPart::default()
                    },
                );
                if !initial.is_empty() {
                    if let Some(callback) = on_text_delta {
                        callback(initial);
                    }
                    return Ok(true);
                }
            }
            "response.output_text.delta" | "response.content_part.delta" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "message")?;
                self.require_open_item(index)?;
                let content_index = content_index(&event)?;
                let text = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .context("Responses text delta 缺少 string delta")?;
                let part = self
                    .content_parts
                    .get_mut(&(index, content_index))
                    .with_context(|| {
                        format!(
                            "Responses text delta 在 content_part.added 之前引用 ({index}, {content_index})"
                        )
                    })?;
                if event_type == "response.output_text.delta"
                    && !matches!(part.part_type.as_str(), "output_text" | "text")
                {
                    bail!("Responses output_text delta 引用了非 output_text part")
                }
                if response_part_field(&part.part_type).is_none() {
                    bail!("Responses text delta 引用了非文本 content part")
                }
                if part.output_text_done.is_some() || part.content_part_done {
                    bail!("Responses text delta 出现在 content part done 之后")
                }
                part.streamed.push_str(text);
                if let Some(callback) = on_text_delta {
                    callback(text);
                }
                return Ok(!text.is_empty());
            }
            "response.output_text.done" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "message")?;
                self.require_open_item(index)?;
                let content_index = content_index(&event)?;
                let text = event
                    .get("text")
                    .and_then(Value::as_str)
                    .context("response.output_text.done 缺少 text")?;
                let part = self
                    .content_parts
                    .get_mut(&(index, content_index))
                    .with_context(|| {
                        format!(
                            "response.output_text.done 在 content_part.added 之前引用 ({index}, {content_index})"
                        )
                    })?;
                if !matches!(part.part_type.as_str(), "output_text" | "text") {
                    bail!("response.output_text.done 引用了非 output_text part")
                }
                if part.output_text_done.is_some() {
                    bail!("Responses content part 包含重复 output_text.done")
                }
                if part.streamed != text {
                    bail!("response.output_text.done 与 text delta 不一致")
                }
                part.output_text_done = Some(text.to_owned());
            }
            "response.content_part.done" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "message")?;
                self.require_open_item(index)?;
                let content_index = content_index(&event)?;
                let done_part = event
                    .get("part")
                    .context("response.content_part.done 缺少 part")?;
                let done_type =
                    require_nonempty_string(done_part, "type", "Responses content part done")?;
                let part = self
                    .content_parts
                    .get_mut(&(index, content_index))
                    .with_context(|| {
                        format!(
                            "response.content_part.done 在 content_part.added 之前引用 ({index}, {content_index})"
                        )
                    })?;
                if part.content_part_done {
                    bail!("Responses content part 包含重复 content_part.done")
                }
                if part.part_type != done_type {
                    bail!("response.content_part.done 的 part type 与 added 冲突")
                }
                let done_text = response_part_text(done_part, done_type)?;
                if let Some(done_text) = done_text {
                    if part.streamed != done_text {
                        bail!("response.content_part.done 与 text delta 不一致")
                    }
                    if part
                        .output_text_done
                        .as_deref()
                        .is_some_and(|snapshot| snapshot != done_text)
                    {
                        bail!("Responses content part done snapshots 不一致")
                    }
                    part.content_part_done_text = Some(done_text.to_owned());
                }
                part.content_part_done = true;
            }
            "response.function_call_arguments.delta" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "function_call")?;
                self.require_open_item(index)?;
                let delta = event
                    .get("delta")
                    .and_then(Value::as_str)
                    .context("Responses function arguments delta 缺少 string delta")?;
                let target = self.calls.entry(index).or_default();
                if target.arguments_done {
                    bail!("Responses function arguments delta 出现在 arguments.done 之后")
                }
                append_tool_argument_delta(&mut target.arguments, delta)?;
            }
            "response.function_call_arguments.done" => {
                let index = output_index(&event)?;
                self.require_event_item(&event, index, "function_call")?;
                self.require_open_item(index)?;
                let arguments = event
                    .get("arguments")
                    .and_then(Value::as_str)
                    .context("Responses function arguments done 缺少 arguments")?;
                let target = self.calls.entry(index).or_default();
                if target.arguments_done {
                    bail!("Responses function call 包含重复 arguments.done")
                }
                replace_tool_argument_snapshot(
                    &mut target.arguments,
                    arguments,
                    "Responses function arguments done",
                )?;
                target.arguments_done = true;
            }
            "response.completed" | "response.done" => {
                let response = event
                    .get("response")
                    .context("Responses terminal event 缺少 response")?;
                validate_completed_response(response)?;
                self.capture_id(response)?;
                self.final_response = Some(response.clone());
                self.terminal = true;
            }
            "response.incomplete" | "response.cancelled" => {
                let status = event_type.trim_start_matches("response.");
                let detail = event
                    .pointer("/response/incomplete_details/reason")
                    .or_else(|| event.pointer("/response/error/message"))
                    .and_then(Value::as_str)
                    .unwrap_or("no completion detail");
                bail!("Responses stream ended as {status}: {detail}")
            }
            _ => {}
        }
        Ok(false)
    }

    fn mark_done(&mut self) -> Result<()> {
        if self.saw_done {
            bail!("Responses stream 包含重复 [DONE]")
        }
        if !self.terminal {
            bail!("Responses stream 在 terminal event 前收到 [DONE]")
        }
        self.saw_done = true;
        Ok(())
    }

    fn capture_id(&mut self, response: &Value) -> Result<()> {
        if let Some(id) = response.get("id").and_then(Value::as_str) {
            set_consistent_string(&mut self.id, id, "Responses response id")?;
        }
        Ok(())
    }

    fn validate_sequence(&mut self, event_type: &str, event: &Value) -> Result<()> {
        if !response_event_is_structural(event_type) {
            return Ok(());
        }
        let has_sequence = event.get("sequence_number").is_some();
        match self.sequence_mode {
            Some(expected) if expected != has_sequence => {
                bail!("Responses sequence_number 在同一 stream 中不得混用有/无模式")
            }
            Some(_) => {}
            None => self.sequence_mode = Some(has_sequence),
        }
        let Some(sequence) = event.get("sequence_number") else {
            return Ok(());
        };
        let sequence = sequence
            .as_u64()
            .context("Responses sequence_number 必须是非负整数")?;
        if self
            .last_sequence
            .is_some_and(|previous| sequence <= previous)
        {
            bail!("Responses sequence_number 必须严格递增")
        }
        self.last_sequence = Some(sequence);
        Ok(())
    }

    fn require_event_item(&self, event: &Value, index: usize, expected_type: &str) -> Result<()> {
        let item = self.items.get(&index).with_context(|| {
            format!("Responses event 在 output_item.added 前引用 index {index}")
        })?;
        if item.get("type").and_then(Value::as_str) != Some(expected_type) {
            bail!("Responses event 引用了错误类型的 output item")
        }
        if let Some(event_id) = event.get("item_id").and_then(Value::as_str) {
            let item_id = require_nonempty_string(item, "id", "Responses output item")?;
            if event_id != item_id {
                bail!("Responses event item_id 与 output item 冲突")
            }
        }
        Ok(())
    }

    fn require_open_item(&self, index: usize) -> Result<()> {
        if self.completed_items.contains(&index) {
            bail!("Responses content event 出现在 output_item.done 之后")
        }
        Ok(())
    }

    fn capture_item(&mut self, index: usize, item: &Value, completed: bool) -> Result<()> {
        let item_type = require_nonempty_string(item, "type", "Responses output item")?;
        if self.items.len() >= MAX_CONTENT_BLOCKS && !self.items.contains_key(&index) {
            bail!("Responses output item 超过 {MAX_CONTENT_BLOCKS} 个限制")
        }
        if self.completed_items.contains(&index) {
            if completed {
                bail!("Responses output item {index} 重复完成")
            }
            bail!("Responses output item {index} 完成后再次 added")
        }
        if let Some(item_id) = item.get("id").and_then(Value::as_str) {
            if item_id.is_empty() {
                bail!("Responses output item id 不得为空")
            }
            match self.item_indices.get(item_id) {
                Some(existing) if *existing != index => {
                    bail!("Responses output item id 被多个 index 重用")
                }
                Some(_) => {}
                None => {
                    self.item_indices.insert(item_id.to_owned(), index);
                }
            }
        }
        if let Some(existing) = self.items.get(&index) {
            validate_response_item_identity(existing, item)?;
            if !completed {
                if existing != item {
                    bail!("Responses output_item.added 包含冲突的重复 item")
                }
                return Ok(());
            }
        }
        self.items.insert(index, item.clone());
        if completed {
            self.completed_items.insert(index);
        }
        match item_type {
            "function_call" => {
                if completed {
                    validate_completed_function_call_status(item)?;
                    parse_arguments(item.get("arguments"))?;
                }
                require_nonempty_string(item, "id", "function_call")?;
                let call_id = require_nonempty_string(item, "call_id", "function_call")?;
                let target = self.calls.entry(index).or_default();
                set_consistent_string(&mut target.call_id, call_id, "Responses function call_id")?;
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    if !target.name.is_empty() && target.name != name {
                        bail!("Responses function name 在 stream 中发生冲突")
                    }
                    target.name = name.to_owned();
                }
                if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
                    if !arguments.is_empty() {
                        replace_tool_argument_snapshot(
                            &mut target.arguments,
                            arguments,
                            "Responses function arguments item",
                        )?;
                    }
                }
            }
            "message" => {
                require_nonempty_string(item, "id", "Responses message")?;
                if item.get("role").and_then(Value::as_str) != Some("assistant") {
                    bail!("Responses output message role 必须是 assistant")
                }
                if completed {
                    validate_response_message(item)?;
                    self.validate_completed_message_parts(index, item)?;
                }
            }
            "reasoning" => {
                require_nonempty_string(item, "id", "Responses reasoning item")?;
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_completed_message_parts(&self, output_index: usize, item: &Value) -> Result<()> {
        let completed = item
            .get("content")
            .and_then(Value::as_array)
            .context("Responses message 缺少 content array")?;
        let streamed_parts = self
            .content_parts
            .range((output_index, 0)..=(output_index, MAX_CONTENT_BLOCKS - 1))
            .collect::<Vec<_>>();

        let has_streamed_text = streamed_parts
            .iter()
            .any(|(_, part)| response_part_field(&part.part_type).is_some());
        let mut streamed_all = String::new();
        for ((_, content_index), streamed) in streamed_parts {
            let complete_part = completed.get(*content_index).with_context(|| {
                format!("Responses completed message 缺少 streamed content_index {content_index}")
            })?;
            let complete_type =
                require_nonempty_string(complete_part, "type", "Responses completed content part")?;
            if streamed.part_type != complete_type {
                bail!(
                    "Responses content part ({output_index}, {content_index}) type 在 stream 中发生冲突"
                )
            }
            if let Some(complete_text) = response_part_text(complete_part, complete_type)? {
                if streamed.streamed != complete_text {
                    bail!(
                        "Responses content part ({output_index}, {content_index}) 与 text delta 不一致"
                    )
                }
                if streamed
                    .output_text_done
                    .as_deref()
                    .is_some_and(|snapshot| snapshot != complete_text)
                    || streamed
                        .content_part_done_text
                        .as_deref()
                        .is_some_and(|snapshot| snapshot != complete_text)
                {
                    bail!(
                        "Responses content part ({output_index}, {content_index}) done snapshot 不一致"
                    )
                }
                streamed_all.push_str(&streamed.streamed);
            }
        }

        if has_streamed_text {
            let mut completed_all = String::new();
            for (content_index, complete_part) in completed.iter().enumerate() {
                let complete_type = require_nonempty_string(
                    complete_part,
                    "type",
                    "Responses completed content part",
                )?;
                if let Some(complete_text) = response_part_text(complete_part, complete_type)? {
                    if !self
                        .content_parts
                        .contains_key(&(output_index, content_index))
                    {
                        bail!(
                            "Responses completed message 包含未经 content_part.added 的文本 part {content_index}"
                        )
                    }
                    completed_all.push_str(complete_text);
                }
            }
            if streamed_all != completed_all {
                bail!("Responses completed message 与 streamed text 整体不一致")
            }
        }
        Ok(())
    }

    fn validate_stream_indices(&self) -> Result<()> {
        for (expected, actual) in self.items.keys().copied().enumerate() {
            if actual != expected {
                bail!("Responses output_index 必须连续且从 0 开始：期望 {expected}，实际 {actual}")
            }
        }
        if self.added_items.len() != self.items.len()
            || self
                .items
                .keys()
                .any(|index| !self.added_items.contains(index))
        {
            bail!("Responses stream 包含未经 output_item.added 的 completed item")
        }
        if self.completed_items.len() != self.items.len()
            || self
                .items
                .keys()
                .any(|index| !self.completed_items.contains(index))
        {
            bail!("Responses stream 在 output_item.done 前结束")
        }

        let mut current_output = None;
        let mut expected_content = 0;
        for &(output_index, content_index) in self.content_parts.keys() {
            if current_output != Some(output_index) {
                current_output = Some(output_index);
                expected_content = 0;
            }
            if content_index != expected_content {
                bail!(
                    "Responses content_index 必须在 output {output_index} 内连续且从 0 开始：期望 {expected_content}，实际 {content_index}"
                )
            }
            expected_content += 1;
        }
        Ok(())
    }

    fn validate_terminal_output(&self, output: &[Value]) -> Result<()> {
        if output.len() != self.items.len() {
            bail!("Responses terminal output 与 streamed output item 数量不一致")
        }
        for (index, terminal_item) in output.iter().enumerate() {
            let streamed_item = self.items.get(&index).with_context(|| {
                format!("Responses terminal output 缺少 streamed index {index}")
            })?;
            validate_terminal_response_item(index, streamed_item, terminal_item)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<ModelResponse> {
        if !self.terminal {
            bail!("Responses stream 在 terminal event 前中断")
        }
        self.validate_stream_indices()?;
        let terminal_response = self
            .final_response
            .as_ref()
            .context("Responses terminal event 缺少完整 response")?;
        match terminal_response.get("output") {
            Some(Value::Array(output)) => self.validate_terminal_output(output)?,
            Some(_) => bail!("Responses terminal response output 必须是 array"),
            None => {}
        }
        let mut response = self
            .final_response
            .context("Responses terminal event 缺少完整 response")?;
        match response.get("output") {
            Some(Value::Array(_)) => return parse_responses_response(&response),
            Some(_) => bail!("Responses terminal response output 必须是 array"),
            None => {}
        }
        let output = self.items.into_values().collect::<Vec<_>>();
        response
            .as_object_mut()
            .context("Responses terminal response 必须是 object")?
            .insert("output".into(), Value::Array(output));
        parse_responses_response(&response)
    }
}

fn response_event_requires_created(event_type: &str) -> bool {
    matches!(
        event_type,
        "response.in_progress"
            | "response.output_item.added"
            | "response.output_item.done"
            | "response.content_part.added"
            | "response.content_part.delta"
            | "response.content_part.done"
            | "response.output_text.delta"
            | "response.output_text.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done"
            | "response.completed"
            | "response.done"
            | "response.incomplete"
            | "response.cancelled"
            | "response.error"
            | "response.failed"
    )
}

fn response_event_is_structural(event_type: &str) -> bool {
    event_type == "error"
        || event_type == "response.created"
        || response_event_requires_created(event_type)
}

fn response_part_field(part_type: &str) -> Option<&'static str> {
    match part_type {
        "output_text" | "text" => Some("text"),
        "refusal" => Some("refusal"),
        _ => None,
    }
}

fn response_part_text<'a>(part: &'a Value, part_type: &str) -> Result<Option<&'a str>> {
    let Some(field) = response_part_field(part_type) else {
        return Ok(None);
    };
    part.get(field)
        .and_then(Value::as_str)
        .map(Some)
        .with_context(|| format!("Responses {part_type} content part 缺少 string {field}"))
}

fn validate_terminal_response_item(index: usize, streamed: &Value, terminal: &Value) -> Result<()> {
    for field in ["type", "id", "call_id", "name", "role"] {
        if streamed.get(field) != terminal.get(field) {
            bail!("Responses terminal output index {index} 的 {field} 与 streamed item 不一致")
        }
    }
    match streamed.get("type").and_then(Value::as_str) {
        Some("function_call") => {
            validate_completed_function_call_status(streamed)?;
            validate_completed_function_call_status(terminal)?;
            let streamed_arguments = streamed
                .get("arguments")
                .and_then(Value::as_str)
                .context("streamed function_call 缺少 arguments")?;
            let terminal_arguments = terminal
                .get("arguments")
                .and_then(Value::as_str)
                .context("terminal function_call 缺少 arguments")?;
            if streamed_arguments != terminal_arguments {
                bail!(
                    "Responses terminal output index {index} 的 function_call arguments 与 streamed item 不一致"
                )
            }
        }
        Some("message") => {
            validate_response_message(streamed)?;
            validate_response_message(terminal)?;
            let streamed_parts = streamed["content"]
                .as_array()
                .context("streamed message 缺少 content")?;
            let terminal_parts = terminal["content"]
                .as_array()
                .context("terminal message 缺少 content")?;
            if streamed_parts.len() != terminal_parts.len() {
                bail!(
                    "Responses terminal output index {index} 的 message content 数量与 streamed item 不一致"
                )
            }
            for (content_index, (streamed_part, terminal_part)) in
                streamed_parts.iter().zip(terminal_parts.iter()).enumerate()
            {
                let streamed_type = require_nonempty_string(
                    streamed_part,
                    "type",
                    "streamed message content part",
                )?;
                let terminal_type = require_nonempty_string(
                    terminal_part,
                    "type",
                    "terminal message content part",
                )?;
                if streamed_type != terminal_type {
                    bail!(
                        "Responses terminal output ({index}, {content_index}) content type 与 streamed item 不一致"
                    )
                }
                match response_part_field(streamed_type) {
                    Some(_) => {
                        if response_part_text(streamed_part, streamed_type)?
                            != response_part_text(terminal_part, terminal_type)?
                        {
                            bail!(
                                "Responses terminal output ({index}, {content_index}) text 与 streamed item 不一致"
                            )
                        }
                    }
                    None if streamed_part != terminal_part => {
                        bail!(
                            "Responses terminal output ({index}, {content_index}) content 与 streamed item 不一致"
                        )
                    }
                    None => {}
                }
            }
        }
        Some("reasoning") => {
            if streamed != terminal {
                bail!("Responses terminal reasoning item {index} 与 streamed item 不一致")
            }
        }
        Some(_) | None => {
            if streamed != terminal {
                bail!("Responses terminal output index {index} 与 streamed item 不一致")
            }
        }
    }
    Ok(())
}

fn output_index(event: &Value) -> Result<usize> {
    let index = event
        .get("output_index")
        .and_then(Value::as_u64)
        .context("Responses event 缺少 output_index")?;
    let index = usize::try_from(index).context("Responses output_index 超出平台范围")?;
    if index >= MAX_CONTENT_BLOCKS {
        bail!("Responses output_index 超过 {MAX_CONTENT_BLOCKS} 限制")
    }
    Ok(index)
}

fn content_index(event: &Value) -> Result<usize> {
    let raw = event
        .get("content_index")
        .context("Responses content event 缺少 content_index")?;
    let index = raw
        .as_u64()
        .context("Responses content_index 必须是非负整数")?;
    let index = usize::try_from(index).context("Responses content_index 超出平台范围")?;
    if index >= MAX_CONTENT_BLOCKS {
        bail!("Responses content_index 超过 {MAX_CONTENT_BLOCKS} 限制")
    }
    Ok(index)
}

fn validate_response_item_identity(previous: &Value, current: &Value) -> Result<()> {
    for field in ["type", "id", "call_id", "name", "role"] {
        let before = previous.get(field).and_then(Value::as_str);
        let after = current.get(field).and_then(Value::as_str);
        if let (Some(before), Some(after)) = (before, after) {
            if before != after {
                bail!("Responses output item 的 {field} 在 stream 中发生冲突")
            }
        }
    }
    Ok(())
}

fn event_index(event: &Value) -> Result<usize> {
    let index = event
        .get("index")
        .and_then(Value::as_u64)
        .context("SSE content event 缺少 index")?;
    let index = usize::try_from(index).context("SSE content event index 超出平台范围")?;
    if index >= MAX_CONTENT_BLOCKS {
        bail!("SSE content event index 超过 {MAX_CONTENT_BLOCKS} 限制")
    }
    Ok(index)
}

fn bump_event_count(count: &mut usize) -> Result<()> {
    *count = count.saturating_add(1);
    if *count > MAX_STREAM_EVENTS {
        bail!("SSE event 超过 {MAX_STREAM_EVENTS} 个限制")
    }
    Ok(())
}

fn set_consistent_string(target: &mut Option<String>, value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} 不得为空")
    }
    match target {
        Some(existing) if existing != value => bail!("{label} 在 stream 中发生冲突"),
        Some(_) => Ok(()),
        None => {
            *target = Some(value.to_owned());
            Ok(())
        }
    }
}

fn append_tool_argument_delta(target: &mut String, delta: &str) -> Result<()> {
    let size = target
        .len()
        .checked_add(delta.len())
        .context("stream tool arguments 大小溢出")?;
    check_tool_argument_bytes(size)?;
    target.push_str(delta);
    Ok(())
}

fn replace_tool_argument_snapshot(target: &mut String, snapshot: &str, label: &str) -> Result<()> {
    check_tool_argument_bytes(snapshot.len())?;
    if !target.is_empty() && target != snapshot {
        bail!("{label} 与已接收 delta 不一致")
    }
    *target = snapshot.to_owned();
    Ok(())
}

fn merge_messages_usage(target: &mut Option<Usage>, value: &Value) -> Result<()> {
    if value.is_null() {
        return Ok(());
    }
    let object = value
        .as_object()
        .context("SSE usage 必须是 object 或 null")?;
    let usage = target.get_or_insert_with(zero_usage);
    merge_usage_counter(object, "input_tokens", &mut usage.input_tokens)?;
    merge_usage_counter(object, "output_tokens", &mut usage.output_tokens)?;
    merge_usage_counter(
        object,
        "cache_creation_input_tokens",
        &mut usage.cache_creation_input_tokens,
    )?;
    merge_usage_counter(
        object,
        "cache_read_input_tokens",
        &mut usage.cache_read_input_tokens,
    )?;
    Ok(())
}

fn merge_usage_counter(object: &Map<String, Value>, field: &str, target: &mut u64) -> Result<()> {
    let Some(value) = object.get(field) else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    *target = value
        .as_u64()
        .with_context(|| format!("usage.{field} 必须是非负整数或 null"))?;
    Ok(())
}

fn append_string(block: Option<&mut Value>, field: &str, delta: &str) -> Result<()> {
    let object = block
        .and_then(Value::as_object_mut)
        .context("SSE delta 对应的 content block 不存在")?;
    let target = object
        .entry(field)
        .or_insert_with(|| Value::String(String::new()));
    let target = target
        .as_str()
        .context("SSE content block 字段不是 string")?;
    let mut combined = String::with_capacity(target.len() + delta.len());
    combined.push_str(target);
    combined.push_str(delta);
    object.insert(field.to_owned(), Value::String(combined));
    Ok(())
}

fn zero_usage() -> Usage {
    Usage {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tools() -> Vec<Value> {
        vec![json!({
            "name":"Read",
            "description":"Read a file",
            "input_schema":{"type":"object","properties":{"path":{"type":"string"}}}
        })]
    }

    fn conversation() -> Vec<Message> {
        vec![
            Message::user_text("read it"),
            Message::assistant(vec![json!({
                "type":"tool_use","id":"call-1","name":"Read","input":{"path":"a.txt"}
            })]),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"call-1","content":"hello","is_error":false
            })]),
        ]
    }

    #[test]
    fn format_auto_detection_uses_path_without_query() {
        assert_eq!(
            ApiFormat::Auto.infer("/v1/chat/completions?api-version=1"),
            ApiFormat::ChatCompletions
        );
        assert_eq!(
            ApiFormat::Auto.infer("/api/v1/responses"),
            ApiFormat::Responses
        );
        assert_eq!(ApiFormat::Auto.infer("/v1/messages"), ApiFormat::Messages);
    }

    #[test]
    fn chat_request_maps_tools_calls_and_results() {
        let body = encode_request(
            ApiFormat::ChatCompletions,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &conversation(),
                tools: &tools(),
                stream: true,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][2]["tool_calls"][0]["id"], "call-1");
        assert_eq!(body["messages"][3]["role"], "tool");
        assert_eq!(body["tools"][0]["function"]["parameters"]["type"], "object");
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn chat_request_can_target_legacy_compatible_token_field() {
        let body = encode_request(
            ApiFormat::ChatCompletions,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &[],
                tools: &[],
                stream: true,
                chat_tokens_field: ChatTokensField::MaxTokens,
                include_stream_usage: false,
            },
        )
        .unwrap();
        assert_eq!(body["max_tokens"], 10);
        assert!(body.get("max_completion_tokens").is_none());
        assert!(body.get("stream_options").is_none());
    }

    #[test]
    fn responses_request_maps_items_and_disables_storage() {
        let body = encode_request(
            ApiFormat::Responses,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &conversation(),
                tools: &tools(),
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(body["store"], false);
        assert_eq!(body["input"][1]["type"], "function_call");
        assert_eq!(body["input"][1]["id"], "call-1");
        assert_eq!(body["input"][2]["type"], "function_call_output");
        assert_eq!(body["tools"][0]["name"], "Read");
    }

    #[test]
    fn responses_reasoning_state_is_replayed_only_to_responses() {
        let messages = vec![Message::assistant(vec![json!({
            "type":"provider_state",
            "format":"responses",
            "item":{
                "type":"reasoning",
                "id":"rs-test",
                "summary":[],
                "encrypted_content":"opaque-state"
            }
        })])];
        let responses = encode_request(
            ApiFormat::Responses,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        let messages_body = encode_request(
            ApiFormat::Messages,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(responses["input"][0]["encrypted_content"], "opaque-state");
        assert_eq!(messages_body["messages"], json!([]));
    }

    #[test]
    fn chat_response_accepts_null_usage_fields_and_tool_calls() {
        let response = parse_response(
            ApiFormat::ChatCompletions,
            json!({
                "id":"chat-1",
                "choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{
                    "id":"call-1","type":"function","function":{"name":"Read","arguments":"{\"path\":\"a.txt\"}"}
                }]},"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":null,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":null}}
            }),
        )
        .unwrap();
        assert_eq!(response.stop_reason.as_deref(), Some("tool_use"));
        assert_eq!(response.content[0]["input"]["path"], "a.txt");
        assert_eq!(response.usage.unwrap().input_tokens, 0);
    }

    #[test]
    fn incomplete_terminal_tool_calls_are_not_synthesized() {
        let chat_without_id = json!({
            "id":"chat-1","choices":[{"index":0,"message":{
                "role":"assistant","content":null,"tool_calls":[{
                    "type":"function","function":{"name":"Read","arguments":"{}"}
                }]
            },"finish_reason":"tool_calls"}]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, chat_without_id).is_err());

        let chat_without_arguments = json!({
            "id":"chat-1","choices":[{"index":0,"message":{
                "role":"assistant","content":null,"tool_calls":[{
                    "id":"call-1","type":"function","function":{"name":"Read"}
                }]
            },"finish_reason":"tool_calls"}]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, chat_without_arguments).is_err());

        let responses_without_arguments = json!({
            "id":"resp-1","status":"completed","output":[{
                "type":"function_call","id":"fc-1","call_id":"call-1","name":"Read"
            }]
        });
        assert!(parse_response(ApiFormat::Responses, responses_without_arguments).is_err());

        let mut stream = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{"tool_calls":[{
                    "index":0,"function":{"name":"Read","arguments":"{}"}
                }]},"finish_reason":"tool_calls"}]}),
                None,
            )
            .unwrap();
        stream.mark_done().unwrap();
        assert!(stream.finish().is_err());
    }

    #[test]
    fn chat_choices_and_stream_tool_calls_require_explicit_indexes() {
        let missing_choice_index = json!({
            "id":"chat-1","choices":[{
                "message":{"role":"assistant","content":"done"},"finish_reason":"stop"
            }]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, missing_choice_index).is_err());

        let duplicate_choices = json!({
            "id":"chat-1","choices":[
                {"index":0,"message":{"role":"assistant","content":"first"},"finish_reason":"stop"},
                {"index":1,"message":{"role":"assistant","content":"second"},"finish_reason":"stop"}
            ]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, duplicate_choices).is_err());

        let mut missing_stream_choice_index =
            StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        assert!(
            missing_stream_choice_index
                .apply(
                    json!({"id":"chat-1","choices":[{
                        "delta":{"content":"partial"},"finish_reason":null
                    }]}),
                    None,
                )
                .is_err()
        );

        let mut missing_tool_index = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        assert!(
            missing_tool_index
                .apply(
                    json!({"id":"chat-1","choices":[{"index":0,"delta":{"tool_calls":[{
                        "id":"call-1","function":{"name":"Read","arguments":"{}"}
                    }]},"finish_reason":"tool_calls"}]}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn chat_response_rejects_choice_errors_and_invalid_modern_call_types() {
        let choice_error = json!({
            "id":"chat-1","choices":[{"index":0,"error":{"message":"failed"},
                "message":{"role":"assistant","content":null},"finish_reason":"stop"}]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, choice_error).is_err());

        for call_type in [None, Some("custom")] {
            let mut call = json!({
                "id":"call-1","function":{"name":"Read","arguments":"{}"}
            });
            if let Some(call_type) = call_type {
                call["type"] = Value::String(call_type.to_owned());
            }
            let response = json!({
                "id":"chat-1","choices":[{"index":0,"message":{
                    "role":"assistant","content":null,"tool_calls":[call]
                },"finish_reason":"tool_calls"}]
            });
            assert!(parse_response(ApiFormat::ChatCompletions, response).is_err());
        }

        let mixed = json!({
            "id":"chat-1","choices":[{"index":0,"message":{
                "role":"assistant","content":null,
                "tool_calls":[{"id":"call-1","type":"function","function":{"name":"Read","arguments":"{}"}}],
                "function_call":{"name":"Read","arguments":"{}"}
            },"finish_reason":"tool_calls"}]
        });
        assert!(parse_response(ApiFormat::ChatCompletions, mixed).is_err());
    }

    #[test]
    fn chat_stream_rejects_invalid_call_dialects_indexes_and_types() {
        let mut invalid_type = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        assert!(
            invalid_type
                .apply(
                    json!({"id":"chat","choices":[{"index":0,"delta":{"tool_calls":[{
                        "index":0,"id":"call-1","type":"custom",
                        "function":{"name":"Read","arguments":"{}"}
                    }]},"finish_reason":"tool_calls"}]}),
                    None,
                )
                .is_err()
        );

        let mut duplicate_index = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        assert!(
            duplicate_index
                .apply(
                    json!({"id":"chat","choices":[{"index":0,"delta":{"tool_calls":[
                        {"index":0,"id":"call-1","type":"function","function":{"name":"Read","arguments":"{}"}},
                        {"index":0,"id":"call-2","type":"function","function":{"name":"Glob","arguments":"{}"}}
                    ]},"finish_reason":"tool_calls"}]}),
                    None,
                )
                .is_err()
        );

        let mut mixed = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        mixed
            .apply(
                json!({"id":"chat","choices":[{"index":0,"delta":{"tool_calls":[{
                    "index":0,"id":"call-1","type":"function",
                    "function":{"name":"Read","arguments":"{}"}
                }]},"finish_reason":null}]}),
                None,
            )
            .unwrap();
        assert!(
            mixed
                .apply(
                    json!({"id":"chat","choices":[{"index":0,"delta":{"function_call":{
                        "name":"Read","arguments":"{}"
                    }},"finish_reason":"function_call"}]}),
                    None,
                )
                .is_err()
        );

        let mut gapped = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        gapped
            .apply(
                json!({"id":"chat","choices":[{"index":0,"delta":{"tool_calls":[{
                    "index":1,"id":"call-2","type":"function",
                    "function":{"name":"Read","arguments":"{}"}
                }]},"finish_reason":"tool_calls"}]}),
                None,
            )
            .unwrap();
        gapped.mark_done().unwrap();
        assert!(gapped.finish().is_err());

        let mut redacted = StreamDecoder::new(ApiFormat::Messages).unwrap();
        redacted
            .apply(
                json!({"type":"message_start","message":{
                    "type":"message","id":"redacted","role":"assistant","content":[]
                }}),
                None,
            )
            .unwrap();
        redacted
            .apply(
                json!({"type":"content_block_start","index":0,"content_block":{
                    "type":"redacted_thinking","data":"opaque"
                }}),
                None,
            )
            .unwrap();
        redacted
            .apply(json!({"type":"content_block_stop","index":0}), None)
            .unwrap();
        redacted
            .apply(json!({"type":"message_stop"}), None)
            .unwrap();
        assert_eq!(redacted.finish().unwrap().content[0]["data"], "opaque");
    }

    #[test]
    fn messages_complete_response_requires_strict_envelope_and_tool_inputs() {
        let valid = json!({
            "type":"message","id":"msg-1","role":"assistant",
            "content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{}}],
            "stop_reason":"tool_use","usage":null
        });
        assert!(parse_response(ApiFormat::Messages, valid.clone()).is_ok());

        for (pointer, invalid) in [
            ("/type", Value::String("error".to_owned())),
            ("/role", Value::String("user".to_owned())),
            ("/id", Value::String(String::new())),
            ("/content/0/id", Value::String(String::new())),
            ("/content/0/name", Value::String(String::new())),
            ("/content/0/input", Value::Array(vec![])),
        ] {
            let mut response = valid.clone();
            *response.pointer_mut(pointer).unwrap() = invalid;
            assert!(parse_response(ApiFormat::Messages, response).is_err());
        }
    }

    #[test]
    fn messages_stream_requires_strict_start_delta_pairing_and_contiguous_blocks() {
        let mut invalid_start = StreamDecoder::new(ApiFormat::Messages).unwrap();
        assert!(
            invalid_start
                .apply(
                    json!({"type":"message_start","message":{
                        "type":"message","id":"msg","role":"assistant","content":[{}]
                    }}),
                    None,
                )
                .is_err()
        );

        let start = json!({"type":"message_start","message":{
            "type":"message","id":"msg","role":"assistant","content":[],"usage":null
        }});
        let mut mismatched = StreamDecoder::new(ApiFormat::Messages).unwrap();
        mismatched.apply(start.clone(), None).unwrap();
        mismatched
            .apply(
                json!({"type":"content_block_start","index":0,"content_block":{
                    "type":"text","text":""
                }}),
                None,
            )
            .unwrap();
        assert!(
            mismatched
                .apply(
                    json!({"type":"content_block_delta","index":0,"delta":{
                        "type":"input_json_delta","partial_json":"{}"
                    }}),
                    None,
                )
                .is_err()
        );

        let mut unknown_delta = StreamDecoder::new(ApiFormat::Messages).unwrap();
        unknown_delta.apply(start.clone(), None).unwrap();
        unknown_delta
            .apply(
                json!({"type":"content_block_start","index":0,"content_block":{
                    "type":"thinking","thinking":""
                }}),
                None,
            )
            .unwrap();
        assert!(
            unknown_delta
                .apply(
                    json!({"type":"content_block_delta","index":0,"delta":{
                        "type":"unknown_delta"
                    }}),
                    None,
                )
                .is_err()
        );

        let mut non_object_input = StreamDecoder::new(ApiFormat::Messages).unwrap();
        non_object_input.apply(start.clone(), None).unwrap();
        non_object_input
            .apply(
                json!({"type":"content_block_start","index":0,"content_block":{
                    "type":"tool_use","id":"tool-1","name":"Read","input":{}
                }}),
                None,
            )
            .unwrap();
        non_object_input
            .apply(
                json!({"type":"content_block_delta","index":0,"delta":{
                    "type":"input_json_delta","partial_json":"[]"
                }}),
                None,
            )
            .unwrap();
        assert!(
            non_object_input
                .apply(json!({"type":"content_block_stop","index":0}), None)
                .is_err()
        );

        let mut gapped = StreamDecoder::new(ApiFormat::Messages).unwrap();
        gapped.apply(start, None).unwrap();
        gapped
            .apply(
                json!({"type":"content_block_start","index":1,"content_block":{
                    "type":"text","text":""
                }}),
                None,
            )
            .unwrap();
        gapped
            .apply(json!({"type":"content_block_stop","index":1}), None)
            .unwrap();
        gapped.apply(json!({"type":"message_stop"}), None).unwrap();
        assert!(gapped.finish().is_err());
    }

    #[test]
    fn responses_fallback_history_has_stable_required_assistant_fields() {
        let messages = vec![
            Message::user_text("first"),
            Message::assistant(vec![json!({"type":"text","text":"same"})]),
            Message::user_text("second"),
            Message::assistant(vec![json!({"type":"text","text":"same"})]),
        ];
        let first = encode_request(
            ApiFormat::Responses,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        let second = encode_request(
            ApiFormat::Responses,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first["input"][1]["id"], "msg_local_0");
        assert_eq!(first["input"][1]["status"], "completed");
        assert_eq!(first["input"][3]["id"], "msg_local_1");
        assert_eq!(first["input"][3]["status"], "completed");
    }

    #[test]
    fn responses_response_maps_text_calls_and_usage() {
        let response = parse_response(
            ApiFormat::Responses,
            json!({
                "id":"resp-1","status":"completed",
                "output":[
                    {"type":"message","id":"msg-1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"working"}]},
                    {"type":"function_call","id":"fc-1","status":"completed","call_id":"call-1","name":"Read","arguments":"{\"path\":\"a.txt\"}"}
                ],
                "usage":{"input_tokens":5,"output_tokens":null,"input_tokens_details":{"cached_tokens":2}}
            }),
        )
        .unwrap();
        assert!(
            response
                .content
                .iter()
                .any(|block| block["type"] == "text" && block["text"] == "working")
        );
        assert!(
            response
                .content
                .iter()
                .any(|block| block["type"] == "tool_use" && block["id"] == "call-1")
        );
        assert_eq!(response.usage.unwrap().cache_read_input_tokens, 2);
    }

    #[test]
    fn responses_completed_function_call_status_is_optional_but_strict_when_present() {
        for status in [None, Some("completed")] {
            let mut call = json!({
                "type":"function_call","id":"fc-1","call_id":"call-1",
                "name":"Read","arguments":"{}"
            });
            if let Some(status) = status {
                call["status"] = json!(status);
            }
            assert!(
                parse_response(
                    ApiFormat::Responses,
                    json!({"id":"resp-1","status":"completed","output":[call]}),
                )
                .is_ok()
            );
        }
        assert!(
            parse_response(
                ApiFormat::Responses,
                json!({"id":"resp-1","status":"completed","output":[{
                    "type":"function_call","id":"fc-1","call_id":"call-1",
                    "name":"Read","arguments":"{}","status":"in_progress"
                }]}),
            )
            .is_err()
        );

        for status in [None, Some("completed")] {
            let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
            stream
                .apply(
                    json!({"type":"response.created","response":{
                        "id":"resp-1","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.added","output_index":0,"item":{
                        "type":"function_call","id":"fc-1","call_id":"call-1",
                        "name":"Read","arguments":"","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            let mut done = json!({
                "type":"function_call","id":"fc-1","call_id":"call-1",
                "name":"Read","arguments":"{}"
            });
            if let Some(status) = status {
                done["status"] = json!(status);
            }
            stream
                .apply(
                    json!({"type":"response.output_item.done","output_index":0,"item":done}),
                    None,
                )
                .unwrap();
        }

        let mut rejected = StreamDecoder::new(ApiFormat::Responses).unwrap();
        rejected
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        rejected
            .apply(
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"function_call","id":"fc-1","call_id":"call-1",
                    "name":"Read","arguments":"","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        assert!(
            rejected
                .apply(
                    json!({"type":"response.output_item.done","output_index":0,"item":{
                        "type":"function_call","id":"fc-1","call_id":"call-1",
                        "name":"Read","arguments":"{}","status":"in_progress"
                    }}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn responses_replay_preserves_required_item_identity_and_order() {
        let reasoning = json!({
            "type":"reasoning","id":"rs-1","summary":[],"encrypted_content":"opaque"
        });
        let message = json!({
            "type":"message","id":"msg-1","status":"completed","role":"assistant",
            "content":[{"type":"output_text","text":"working"}]
        });
        let function_call = json!({
            "type":"function_call","id":"fc-1","status":"completed",
            "call_id":"call-1","name":"Read","arguments":"{\"path\":\"a.txt\"}"
        });
        let response = parse_response(
            ApiFormat::Responses,
            json!({
                "id":"resp-1","status":"completed",
                "output":[reasoning.clone(),message.clone(),function_call.clone()]
            }),
        )
        .unwrap();
        let messages = vec![
            Message::assistant(response.content),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"call-1","content":"done"
            })]),
        ];
        let body = encode_request(
            ApiFormat::Responses,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        let input = body["input"].as_array().unwrap();
        assert_eq!(input[0], reasoning);
        assert_eq!(input[1], message);
        assert_eq!(input[2], function_call);
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call-1");
    }

    #[test]
    fn chat_reasoning_details_round_trip_without_reordering() {
        let details = json!([
            {"type":"reasoning.encrypted","data":"first"},
            {"type":"reasoning.summary","text":"second"}
        ]);
        let response = parse_response(
            ApiFormat::ChatCompletions,
            json!({
                "id":"chat-1",
                "choices":[{"index":0,"message":{
                    "role":"assistant","content":null,
                    "reasoning_details":details.clone(),
                    "tool_calls":[{"id":"call-1","type":"function","function":{
                        "name":"Read","arguments":"{\"path\":\"a.txt\"}"
                    }}]
                },"finish_reason":"tool_calls"}]
            }),
        )
        .unwrap();
        let messages = vec![Message::assistant(response.content)];
        let body = encode_request(
            ApiFormat::ChatCompletions,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: false,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(body["messages"][1]["reasoning_details"], details);
        assert_eq!(body["messages"][1]["tool_calls"][0]["id"], "call-1");
    }

    #[test]
    fn chat_stream_accumulates_split_parallel_calls_and_final_usage() {
        let mut stream = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{"content":"hi ","tool_calls":[
                    {"index":0,"id":"c1","function":{"name":"Read","arguments":"{\"path\":"}},
                    {"index":1,"id":"c2","function":{"name":"Glob","arguments":"{\"pattern\":"}}
                ]},"finish_reason":null}]}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{"content":"there","tool_calls":[
                    {"index":0,"function":{"arguments":"\"a\"}"}},
                    {"index":1,"function":{"arguments":"\"*.rs\"}"}}
                ]},"finish_reason":"tool_calls"}]}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[],"usage":{"prompt_tokens":null,"completion_tokens":9}}),
                None,
            )
            .unwrap();
        stream.mark_done().unwrap();
        let response = stream.finish().unwrap();
        assert_eq!(response.content[0]["text"], "hi there");
        assert_eq!(response.content[1]["input"]["path"], "a");
        assert_eq!(response.content[2]["input"]["pattern"], "*.rs");
        assert_eq!(response.usage.unwrap().output_tokens, 9);
    }

    #[test]
    fn chat_stream_replays_reasoning_details_exactly() {
        let first = json!({"type":"reasoning.encrypted","data":"first"});
        let second = json!({"type":"reasoning.summary","text":"second"});
        let mut stream = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{
                    "reasoning_details":[first.clone()],
                    "tool_calls":[{"index":0,"id":"call-1","function":{
                        "name":"Read","arguments":"{\"path\":"
                    }}]
                },"finish_reason":null}]}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{
                    "reasoning_details":[second.clone()],
                    "tool_calls":[{"index":0,"function":{"arguments":"\"a.txt\"}"}}]
                },"finish_reason":"tool_calls"}]}),
                None,
            )
            .unwrap();
        stream.mark_done().unwrap();
        let response = stream.finish().unwrap();
        let messages = vec![Message::assistant(response.content)];
        let body = encode_request(
            ApiFormat::ChatCompletions,
            RequestParts {
                model: "model",
                max_tokens: 10,
                system: "system",
                messages: &messages,
                tools: &[],
                stream: true,
                chat_tokens_field: ChatTokensField::MaxCompletionTokens,
                include_stream_usage: true,
            },
        )
        .unwrap();
        assert_eq!(
            body["messages"][1]["reasoning_details"],
            json!([first, second])
        );
    }

    #[test]
    fn chat_and_responses_append_every_argument_delta_exactly() {
        let fragments = ["{\"value\":\"", "{", "\"}"];
        let mut chat = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        for (index, fragment) in fragments.iter().enumerate() {
            chat.apply(
                json!({"id":"chat-1","choices":[{"index":0,"delta":{"tool_calls":[{
                    "index":0,
                    "id":(index == 0).then_some("call-1"),
                    "function":{"name":(index == 0).then_some("Write"),"arguments":fragment}
                }]},"finish_reason":(index == fragments.len() - 1).then_some("tool_calls")}]}),
                None,
            )
            .unwrap();
        }
        chat.mark_done().unwrap();
        let chat_response = chat.finish().unwrap();
        assert_eq!(chat_response.content[0]["input"]["value"], "{");

        let mut responses = StreamDecoder::new(ApiFormat::Responses).unwrap();
        responses
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        responses
            .apply(
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"function_call","id":"fc-1","call_id":"call-1",
                    "name":"Write","arguments":"","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        for fragment in fragments {
            responses
                .apply(
                    json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":fragment}),
                    None,
                )
                .unwrap();
        }
        responses
            .apply(
                json!({"type":"response.output_item.done","output_index":0,"item":{
                    "type":"function_call","id":"fc-1","call_id":"call-1",
                    "name":"Write","arguments":"{\"value\":\"{\"}","status":"completed"
                }}),
                None,
            )
            .unwrap();
        responses
            .apply(
                json!({"type":"response.done","response":{
                    "id":"resp-1","status":"completed"
                }}),
                None,
            )
            .unwrap();
        responses.mark_done().unwrap();
        let responses_response = responses.finish().unwrap();
        assert_eq!(responses_response.content[1]["input"]["value"], "{");
    }

    #[test]
    fn responses_done_marker_and_identity_state_are_strict() {
        let mut early_done = StreamDecoder::new(ApiFormat::Responses).unwrap();
        assert!(early_done.mark_done().is_err());

        for event in [
            json!({"type":"response.in_progress","response":{
                "id":"resp-1","status":"in_progress"
            }}),
            json!({"type":"response.output_item.added","output_index":0,"item":{
                "type":"reasoning","id":"rs-1"
            }}),
        ] {
            let mut before_created = StreamDecoder::new(ApiFormat::Responses).unwrap();
            assert!(before_created.apply(event, None).is_err());
        }

        let mut duplicate_created = StreamDecoder::new(ApiFormat::Responses).unwrap();
        let created = json!({"type":"response.created","response":{
            "id":"resp-1","status":"in_progress"
        }});
        duplicate_created.apply(created.clone(), None).unwrap();
        assert!(duplicate_created.apply(created, None).is_err());

        let mut numbered_then_unnumbered = StreamDecoder::new(ApiFormat::Responses).unwrap();
        numbered_then_unnumbered
            .apply(
                json!({"type":"response.created","sequence_number":0,"response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        assert!(
            numbered_then_unnumbered
                .apply(
                    json!({"type":"response.in_progress","response":{
                        "id":"resp-1","status":"in_progress"
                    }}),
                    None,
                )
                .is_err()
        );

        let mut unnumbered_then_numbered = StreamDecoder::new(ApiFormat::Responses).unwrap();
        unnumbered_then_numbered
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        assert!(
            unnumbered_then_numbered
                .apply(
                    json!({"type":"response.in_progress","sequence_number":1,"response":{
                        "id":"resp-1","status":"in_progress"
                    }}),
                    None,
                )
                .is_err()
        );

        let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
        stream
            .apply(
                json!({"type":"response.created","sequence_number":1,"response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        assert!(
            stream
                .apply(
                    json!({"type":"response.in_progress","sequence_number":1,"response":{
                        "id":"resp-1","status":"in_progress"
                    }}),
                    None,
                )
                .is_err()
        );

        let mut duplicate_item = StreamDecoder::new(ApiFormat::Responses).unwrap();
        duplicate_item
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        let added = json!({"type":"response.output_item.added","output_index":0,"item":{
            "type":"reasoning","id":"rs-1"
        }});
        duplicate_item.apply(added.clone(), None).unwrap();
        assert!(duplicate_item.apply(added, None).is_err());

        let mut complete = StreamDecoder::new(ApiFormat::Responses).unwrap();
        assert!(
            complete
                .apply(
                    json!({"type":"response.done","response":{
                        "id":"resp-1","status":"completed","output":[]
                    }}),
                    None,
                )
                .is_err()
        );
        let mut complete = StreamDecoder::new(ApiFormat::Responses).unwrap();
        complete
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        complete
            .apply(
                json!({"type":"response.done","response":{
                    "id":"resp-1","status":"completed","output":[]
                }}),
                None,
            )
            .unwrap();
        complete.mark_done().unwrap();
        assert!(complete.mark_done().is_err());
        assert!(
            complete
                .apply(json!({"type":"response.in_progress"}), None)
                .is_err()
        );
    }

    #[test]
    fn responses_function_argument_done_lifecycle_is_strict() {
        fn call_stream() -> StreamDecoder {
            let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
            stream
                .apply(
                    json!({"type":"response.created","response":{
                        "id":"resp-1","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.added","output_index":0,"item":{
                        "type":"function_call","id":"fc-1","call_id":"call-1",
                        "name":"Read","arguments":"","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
        }

        let mut delta_after_done = call_stream();
        delta_after_done
            .apply(
                json!({"type":"response.function_call_arguments.done",
                    "output_index":0,"arguments":"{}"}),
                None,
            )
            .unwrap();
        assert!(
            delta_after_done
                .apply(
                    json!({"type":"response.function_call_arguments.delta",
                        "output_index":0,"delta":" "}),
                    None,
                )
                .is_err()
        );

        let mut duplicate_done = call_stream();
        duplicate_done
            .apply(
                json!({"type":"response.function_call_arguments.done",
                    "output_index":0,"arguments":"{}"}),
                None,
            )
            .unwrap();
        assert!(
            duplicate_done
                .apply(
                    json!({"type":"response.function_call_arguments.done",
                        "output_index":0,"arguments":"{}"}),
                    None,
                )
                .is_err()
        );

        let mut after_item_done = call_stream();
        after_item_done
            .apply(
                json!({"type":"response.output_item.done","output_index":0,"item":{
                    "type":"function_call","id":"fc-1","call_id":"call-1",
                    "name":"Read","arguments":"{}","status":"completed"
                }}),
                None,
            )
            .unwrap();
        assert!(
            after_item_done
                .apply(
                    json!({"type":"response.function_call_arguments.done",
                        "output_index":0,"arguments":"{}"}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn responses_stream_tracks_multi_part_text_by_content_index() {
        let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
        stream
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-parts","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"message","id":"msg-parts","status":"in_progress",
                    "role":"assistant","content":[]
                }}),
                None,
            )
            .unwrap();

        stream
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":1,"item_id":"msg-parts","part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.content_part.delta","output_index":0,"content_index":1,"item_id":"msg-parts","delta":" world"}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.content_part.done","output_index":0,"content_index":1,"item_id":"msg-parts","part":{
                    "type":"output_text","text":" world"
                }}),
                None,
            )
            .unwrap();

        stream
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg-parts","part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-parts","delta":"Hello"}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"msg-parts","text":"Hello"}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_item.done","output_index":0,"item":{
                    "type":"message","id":"msg-parts","status":"completed","role":"assistant",
                    "content":[
                        {"type":"output_text","text":"Hello"},
                        {"type":"output_text","text":" world"}
                    ]
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.completed","response":{
                    "id":"resp-parts","status":"completed"
                }}),
                None,
            )
            .unwrap();

        let response = stream.finish().unwrap();
        assert!(
            response
                .content
                .iter()
                .any(|block| block["type"] == "text" && block["text"] == "Hello world")
        );
    }

    #[test]
    fn responses_stream_rejects_invalid_or_unannounced_content_indices() {
        fn message_stream() -> StreamDecoder {
            let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
            stream
                .apply(
                    json!({"type":"response.created","response":{
                        "id":"resp-parts","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.added","output_index":0,"item":{
                        "type":"message","id":"msg-parts","status":"in_progress",
                        "role":"assistant","content":[]
                    }}),
                    None,
                )
                .unwrap();
            stream
        }

        for content_index in [json!(-1), json!(MAX_CONTENT_BLOCKS)] {
            let mut stream = message_stream();
            assert!(
                stream
                    .apply(
                        json!({"type":"response.content_part.added","output_index":0,"content_index":content_index,"part":{
                            "type":"output_text","text":""
                        }}),
                        None,
                    )
                .is_err()
            );
        }

        let mut upper_boundary = message_stream();
        upper_boundary
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":MAX_CONTENT_BLOCKS - 1,"part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();

        let mut missing_added = message_stream();
        assert!(
            missing_added
                .apply(
                    json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"orphan"}),
                    None,
                )
                .is_err()
        );

        let mut duplicate = message_stream();
        let added = json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{
            "type":"output_text","text":""
        }});
        duplicate.apply(added.clone(), None).unwrap();
        assert!(duplicate.apply(added, None).is_err());

        let mut conflict = message_stream();
        conflict
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{
                    "type":"output_text","text":"same"
                }}),
                None,
            )
            .unwrap();
        assert!(
            conflict
                .apply(
                    json!({"type":"response.content_part.done","output_index":0,"content_index":0,"part":{
                        "type":"refusal","refusal":"same"
                    }}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn responses_stream_rejects_partial_or_conflicting_completed_text() {
        fn stream_with_text(text: &str) -> StreamDecoder {
            let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
            stream
                .apply(
                    json!({"type":"response.created","response":{
                        "id":"resp-parts","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.added","output_index":0,"item":{
                        "type":"message","id":"msg-parts","status":"in_progress",
                        "role":"assistant","content":[]
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{
                        "type":"output_text","text":""
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":text}),
                    None,
                )
                .unwrap();
            stream
        }

        let mut conflicting_snapshot = stream_with_text("hello");
        assert!(
            conflicting_snapshot
                .apply(
                    json!({"type":"response.output_text.done","output_index":0,"content_index":0,"text":"different"}),
                    None,
                )
                .is_err()
        );

        let mut shifted_part_boundary = stream_with_text("hello");
        shifted_part_boundary
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":1,"part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();
        shifted_part_boundary
            .apply(
                json!({"type":"response.output_text.delta","output_index":0,"content_index":1,"delta":"world"}),
                None,
            )
            .unwrap();
        assert!(
            shifted_part_boundary
                .apply(
                    json!({"type":"response.output_item.done","output_index":0,"item":{
                        "type":"message","id":"msg-parts","status":"completed","role":"assistant",
                        "content":[
                            {"type":"output_text","text":"hell"},
                            {"type":"output_text","text":"oworld"}
                        ]
                    }}),
                    None,
                )
                .is_err()
        );

        let mut partial = stream_with_text("hello");
        assert!(
            partial
                .apply(
                    json!({"type":"response.output_item.done","output_index":0,"item":{
                        "type":"message","id":"msg-parts","status":"completed","role":"assistant",
                        "content":[
                            {"type":"output_text","text":"hello"},
                            {"type":"output_text","text":" unstreamed"}
                        ]
                    }}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn responses_stream_finish_requires_contiguous_output_and_content_indices() {
        let mut output_gap = StreamDecoder::new(ApiFormat::Responses).unwrap();
        output_gap
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-gap","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        output_gap
            .apply(
                json!({"type":"response.output_item.added","output_index":1,"item":{
                    "type":"reasoning","id":"rs-gap"
                }}),
                None,
            )
            .unwrap();
        output_gap
            .apply(
                json!({"type":"response.output_item.done","output_index":1,"item":{
                    "type":"reasoning","id":"rs-gap"
                }}),
                None,
            )
            .unwrap();
        output_gap
            .apply(
                json!({"type":"response.completed","response":{
                    "id":"resp-gap","status":"completed"
                }}),
                None,
            )
            .unwrap();
        assert!(output_gap.finish().is_err());

        let mut content_gap = StreamDecoder::new(ApiFormat::Responses).unwrap();
        content_gap
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-gap","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        content_gap
            .apply(
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"message","id":"msg-gap","status":"in_progress",
                    "role":"assistant","content":[]
                }}),
                None,
            )
            .unwrap();
        content_gap
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":1,"part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();
        content_gap
            .apply(
                json!({"type":"response.output_text.delta","output_index":0,"content_index":1,"delta":"text"}),
                None,
            )
            .unwrap();
        content_gap
            .apply(
                json!({"type":"response.output_item.done","output_index":0,"item":{
                    "type":"message","id":"msg-gap","status":"completed","role":"assistant",
                    "content":[
                        {"type":"metadata","value":"not text"},
                        {"type":"output_text","text":"text"}
                    ]
                }}),
                None,
            )
            .unwrap();
        content_gap
            .apply(
                json!({"type":"response.completed","response":{
                    "id":"resp-gap","status":"completed"
                }}),
                None,
            )
            .unwrap();
        assert!(content_gap.finish().is_err());
    }

    #[test]
    fn responses_terminal_output_must_match_streamed_items() {
        fn function_stream(terminal_item: Value) -> StreamDecoder {
            let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
            stream
                .apply(
                    json!({"type":"response.created","response":{
                        "id":"resp-match","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.added","output_index":0,"item":{
                        "type":"function_call","id":"fc-1","call_id":"call-1",
                        "name":"Read","arguments":"","status":"in_progress"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.output_item.done","output_index":0,"item":{
                        "type":"function_call","id":"fc-1","call_id":"call-1",
                        "name":"Read","arguments":"{}","status":"completed"
                    }}),
                    None,
                )
                .unwrap();
            stream
                .apply(
                    json!({"type":"response.completed","response":{
                        "id":"resp-match","status":"completed","output":[terminal_item]
                    }}),
                    None,
                )
                .unwrap();
            stream
        }

        for terminal_item in [
            json!({
                "type":"function_call","id":"fc-other","call_id":"call-1",
                "name":"Read","arguments":"{}","status":"completed"
            }),
            json!({
                "type":"function_call","id":"fc-1","call_id":"call-1",
                "name":"Write","arguments":"{}","status":"completed"
            }),
            json!({
                "type":"function_call","id":"fc-1","call_id":"call-1",
                "name":"Read","arguments":"{\"different\":true}","status":"completed"
            }),
        ] {
            assert!(function_stream(terminal_item).finish().is_err());
        }

        let streamed_message = json!({
            "type":"message","id":"msg-1","status":"completed","role":"assistant",
            "content":[{"type":"output_text","text":"stream A"}]
        });
        let terminal_message = json!({
            "type":"message","id":"msg-1","status":"completed","role":"assistant",
            "content":[{"type":"output_text","text":"terminal B"}]
        });
        assert!(validate_terminal_response_item(0, &streamed_message, &terminal_message).is_err());
    }

    #[test]
    fn protocol_bounds_and_messages_terminal_phase_are_enforced() {
        let prefix = "{\"value\":\"";
        let suffix = "\"}";
        let exact = format!(
            "{prefix}{}{suffix}",
            "x".repeat(MAX_TOOL_ARGUMENT_BYTES - prefix.len() - suffix.len())
        );
        assert_eq!(exact.len(), MAX_TOOL_ARGUMENT_BYTES);
        assert!(parse_arguments(Some(&Value::String(exact.clone()))).is_ok());
        let oversized = format!("{exact} ");
        assert_eq!(oversized.len(), MAX_TOOL_ARGUMENT_BYTES + 1);
        assert!(parse_arguments(Some(&Value::String(oversized))).is_err());

        let output = vec![json!({"type":"ignored"}); MAX_CONTENT_BLOCKS + 1];
        assert!(
            parse_response(
                ApiFormat::Responses,
                json!({"id":"resp","status":"completed","output":output}),
            )
            .is_err()
        );

        let mut missing_index = StreamDecoder::new(ApiFormat::Responses).unwrap();
        missing_index
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        assert!(
            missing_index
                .apply(
                    json!({"type":"response.output_text.delta","delta":"x"}),
                    None,
                )
                .is_err()
        );

        let mut messages = StreamDecoder::new(ApiFormat::Messages).unwrap();
        messages
            .apply(
                json!({"type":"message_start","message":{
                    "type":"message","role":"assistant","id":"msg","content":[],"usage":null
                }}),
                None,
            )
            .unwrap();
        messages
            .apply(
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
                None,
            )
            .unwrap();
        assert!(
            messages
                .apply(
                    json!({"type":"content_block_start","index":0,"content_block":{
                        "type":"text","text":"late"
                    }}),
                    None,
                )
                .is_err()
        );
    }

    #[test]
    fn responses_stream_uses_completed_response_as_authoritative_result() {
        let mut stream = StreamDecoder::new(ApiFormat::Responses).unwrap();
        stream
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp-1","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_item.added","output_index":0,"item":{
                    "type":"message","id":"msg-1","status":"in_progress",
                    "role":"assistant","content":[]
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg-1","part":{
                    "type":"output_text","text":""
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg-1","delta":"hello"}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.output_item.done","output_index":0,"item":{
                    "type":"message","id":"msg-1","status":"completed","role":"assistant",
                    "content":[{"type":"output_text","text":"hello"}]
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"response.completed","response":{
                    "id":"resp-1","status":"completed",
                    "output":[{"type":"message","id":"msg-1","status":"completed","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],
                    "usage":{"input_tokens":4,"output_tokens":1}
                }}),
                None,
            )
            .unwrap();
        let response = stream.finish().unwrap();
        assert_eq!(response.id, "resp-1");
        assert!(
            response
                .content
                .iter()
                .any(|block| block["type"] == "text" && block["text"] == "hello")
        );
        assert_eq!(response.usage.unwrap().input_tokens, 4);
    }

    #[test]
    fn messages_stream_accepts_null_usage_without_clobbering_known_counters() {
        let mut stream = StreamDecoder::new(ApiFormat::Messages).unwrap();
        stream
            .apply(
                json!({"type":"message_start","message":{
                    "type":"message","role":"assistant","id":"msg-1","content":[],
                    "usage":{"input_tokens":7,"output_tokens":null}
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(
                json!({"type":"content_block_start","index":0,"content_block":{
                    "type":"text","text":"done"
                }}),
                None,
            )
            .unwrap();
        stream
            .apply(json!({"type":"content_block_stop","index":0}), None)
            .unwrap();
        stream
            .apply(
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{
                    "input_tokens":null,"output_tokens":3,"cache_read_input_tokens":2
                }}),
                None,
            )
            .unwrap();
        stream.apply(json!({"type":"message_stop"}), None).unwrap();
        let usage = stream.finish().unwrap().usage.unwrap();
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(usage.cache_read_input_tokens, 2);
    }

    #[test]
    fn every_stream_protocol_rejects_truncation_or_incomplete_completion() {
        let mut messages = StreamDecoder::new(ApiFormat::Messages).unwrap();
        messages
            .apply(
                json!({"type":"message_start","message":{
                    "type":"message","role":"assistant","id":"msg","content":[],"usage":null
                }}),
                None,
            )
            .unwrap();
        assert!(messages.finish().is_err());

        let mut chat = StreamDecoder::new(ApiFormat::ChatCompletions).unwrap();
        chat.apply(
            json!({"id":"chat","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":"stop"}]}),
            None,
        )
        .unwrap();
        assert!(chat.finish().is_err());

        let mut responses = StreamDecoder::new(ApiFormat::Responses).unwrap();
        responses
            .apply(
                json!({"type":"response.created","response":{
                    "id":"resp","status":"in_progress"
                }}),
                None,
            )
            .unwrap();
        let error = responses
            .apply(
                json!({"type":"response.incomplete","response":{
                    "id":"resp","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}
                }}),
                None,
            )
            .unwrap_err();
        assert!(error.to_string().contains("max_output_tokens"));
    }
}
