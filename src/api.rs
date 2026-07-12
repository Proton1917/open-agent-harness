use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use serde_json::{Map, Value, json};
use tokio::time::sleep;

use crate::{
    config::EndpointConfig,
    types::{Message, ModelResponse},
};

#[derive(Clone)]
pub struct ModelClient {
    http: Client,
    endpoint: EndpointConfig,
    messages_url: reqwest::Url,
}

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_SSE_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(60);

pub struct MessageResult {
    pub response: ModelResponse,
    pub streamed_text: bool,
}

impl ModelClient {
    pub fn new(endpoint: EndpointConfig) -> Result<Self> {
        let messages_url = build_messages_url(&endpoint)?;
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600))
            .redirect(reqwest::redirect::Policy::none());
        if !endpoint.allow_env_proxy {
            builder = builder.no_proxy();
        }
        let http = builder.build().context("无法创建 HTTP client")?;
        Ok(Self {
            http,
            endpoint,
            messages_url,
        })
    }

    pub async fn messages(
        &self,
        model: &str,
        max_tokens: u32,
        system: &str,
        messages: &[Message],
        tools: &[Value],
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    ) -> Result<MessageResult> {
        let body = json!({
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
            "tools": tools,
            "stream": true,
        });
        let encoded_body = serde_json::to_vec(&body).context("无法编码 model request")?;
        if encoded_body.len() > MAX_REQUEST_BYTES {
            bail!("model request 超过 {MAX_REQUEST_BYTES} 字节限制")
        }
        let mut last_error = None;
        for attempt in 0..4u32 {
            let mut request = self
                .http
                .post(self.messages_url.clone())
                .header("content-type", "application/json")
                .body(encoded_body.clone());
            if let Some(token) = &self.endpoint.token {
                request = request.bearer_auth(token);
            }
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    last_error = Some(anyhow::anyhow!(error));
                    if attempt < 3 {
                        sleep(Duration::from_secs(1 << attempt)).await;
                        continue;
                    }
                    break;
                }
            };
            let status = response.status();
            let retry_after = retry_after(response.headers());
            if status.is_success() {
                let is_sse = response
                    .headers()
                    .get("content-type")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.contains("text/event-stream"));
                if is_sse {
                    return parse_sse(response, on_text_delta, self.endpoint.token.as_deref())
                        .await;
                }
                let bytes = read_body_limited(response, MAX_RESPONSE_BYTES, "API 响应").await?;
                let mut response: ModelResponse =
                    serde_json::from_slice(&bytes).with_context(|| {
                        format!(
                            "API 返回了无法解析的消息响应: {}",
                            truncate(
                                &redact_text(
                                    &String::from_utf8_lossy(&bytes),
                                    self.endpoint.token.as_deref()
                                ),
                                1000
                            )
                        )
                    })?;
                redact_response(&mut response, self.endpoint.token.as_deref());
                return Ok(MessageResult {
                    response,
                    streamed_text: false,
                });
            }
            let bytes = read_body_limited(response, MAX_ERROR_BYTES, "API 错误响应").await?;
            let text = String::from_utf8_lossy(&bytes);
            let error = api_error(status, &text, self.endpoint.token.as_deref());
            if retryable(status) && attempt < 3 {
                last_error = Some(error);
                sleep(
                    retry_after
                        .unwrap_or_else(|| Duration::from_secs(1 << attempt))
                        .min(MAX_RETRY_DELAY),
                )
                .await;
                continue;
            }
            return Err(error);
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("API 请求失败")))
    }
}

async fn parse_sse(
    response: reqwest::Response,
    on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    secret: Option<&str>,
) -> Result<MessageResult> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        bail!("SSE 响应超过 {MAX_RESPONSE_BYTES} 字节限制")
    }
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut received = 0usize;
    let mut accumulator = StreamAccumulator::default();
    let mut streamed_text = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取 SSE chunk 失败")?;
        received = received
            .checked_add(chunk.len())
            .context("SSE 响应大小溢出")?;
        if received > MAX_RESPONSE_BYTES {
            bail!("SSE 响应超过 {MAX_RESPONSE_BYTES} 字节限制")
        }
        buffer.extend_from_slice(&chunk);
        while let Some((frame_end, separator_len)) = find_frame_end(&buffer) {
            if frame_end > MAX_SSE_FRAME_BYTES {
                bail!("SSE frame 超过 {MAX_SSE_FRAME_BYTES} 字节限制")
            }
            let frame = buffer.drain(..frame_end).collect::<Vec<_>>();
            buffer.drain(..separator_len);
            if let Some(data) = frame_data(&frame)? {
                if data == "[DONE]" {
                    continue;
                }
                let event: Value = serde_json::from_str(&data)
                    .with_context(|| format!("无法解析 SSE data: {}", truncate(&data, 1000)))?;
                streamed_text |= accumulator.apply(event, on_text_delta, secret)?;
            }
        }
        if buffer.len() > MAX_SSE_FRAME_BYTES {
            bail!("SSE frame 超过 {MAX_SSE_FRAME_BYTES} 字节限制")
        }
    }
    if !buffer.iter().all(u8::is_ascii_whitespace)
        && let Some(data) = frame_data(&buffer)?
        && data != "[DONE]"
    {
        let event: Value = serde_json::from_str(&data)?;
        streamed_text |= accumulator.apply(event, on_text_delta, secret)?;
    }
    let mut response = accumulator.finish()?;
    redact_response(&mut response, secret);
    Ok(MessageResult {
        response,
        streamed_text,
    })
}

fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
        return Some((index, 4));
    }
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2))
}

fn frame_data(frame: &[u8]) -> Result<Option<String>> {
    let text = std::str::from_utf8(frame).context("SSE frame 不是有效 UTF-8")?;
    let parts = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>();
    Ok((!parts.is_empty()).then(|| parts.join("\n")))
}

#[derive(Default)]
struct StreamAccumulator {
    id: Option<String>,
    blocks: std::collections::BTreeMap<usize, Value>,
    partial_json: std::collections::HashMap<usize, String>,
    stop_reason: Option<String>,
    usage: Option<crate::types::Usage>,
}

impl StreamAccumulator {
    fn apply(
        &mut self,
        event: Value,
        on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
        secret: Option<&str>,
    ) -> Result<bool> {
        let event_type = event.get("type").and_then(Value::as_str).unwrap_or("");
        match event_type {
            "message_start" => {
                let message = &event["message"];
                self.id = message
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                self.usage = message
                    .get("usage")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()?;
            }
            "content_block_start" => {
                let index = event_index(&event)?;
                let mut block = event
                    .get("content_block")
                    .cloned()
                    .context("content_block_start 缺少 content_block")?;
                let initial_text = (block.get("type").and_then(Value::as_str) == Some("text"))
                    .then(|| block.get("text").and_then(Value::as_str).unwrap_or(""))
                    .map(|text| redact_text(text, secret))
                    .unwrap_or_default();
                if let Some(object) = block.as_object_mut()
                    && !initial_text.is_empty()
                {
                    object.insert("text".into(), Value::String(initial_text.clone()));
                }
                self.blocks.insert(index, block);
                if !initial_text.is_empty() {
                    if let Some(callback) = on_text_delta {
                        callback(&initial_text);
                    }
                    return Ok(true);
                }
            }
            "content_block_delta" => {
                let index = event_index(&event)?;
                let delta = event
                    .get("delta")
                    .context("content_block_delta 缺少 delta")?;
                match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text_delta" => {
                        let text = redact_text(
                            delta.get("text").and_then(Value::as_str).unwrap_or(""),
                            secret,
                        );
                        append_string(self.blocks.get_mut(&index), "text", &text)?;
                        if let Some(callback) = on_text_delta {
                            callback(&text);
                        }
                        return Ok(!text.is_empty());
                    }
                    "input_json_delta" => {
                        self.partial_json.entry(index).or_default().push_str(
                            delta
                                .get("partial_json")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                        );
                    }
                    "thinking_delta" => append_string(
                        self.blocks.get_mut(&index),
                        "thinking",
                        delta.get("thinking").and_then(Value::as_str).unwrap_or(""),
                    )?,
                    "signature_delta" => append_string(
                        self.blocks.get_mut(&index),
                        "signature",
                        delta.get("signature").and_then(Value::as_str).unwrap_or(""),
                    )?,
                    _ => {}
                }
            }
            "content_block_stop" => {
                let index = event_index(&event)?;
                if let Some(partial) = self.partial_json.remove(&index) {
                    let input: Value = serde_json::from_str(&partial).with_context(|| {
                        format!("tool input JSON 拼接失败: {}", truncate(&partial, 1000))
                    })?;
                    self.blocks
                        .get_mut(&index)
                        .and_then(Value::as_object_mut)
                        .context("tool_use content block 不是 object")?
                        .insert("input".into(), input);
                }
            }
            "message_delta" => {
                self.stop_reason = event
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if let Some(usage) = event.get("usage") {
                    let output = usage
                        .get("output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    self.usage.get_or_insert_with(default_usage).output_tokens = output;
                }
            }
            "error" => {
                let message = redact_text(
                    event
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("未知 SSE error"),
                    secret,
                );
                anyhow::bail!("Model stream error: {message}")
            }
            "ping" | "message_stop" => {}
            _ => {}
        }
        Ok(false)
    }

    fn finish(self) -> Result<ModelResponse> {
        if !self.partial_json.is_empty() {
            bail!("SSE 在工具输入 JSON 完成前中断")
        }
        Ok(ModelResponse {
            id: self.id.context("SSE 流缺少 message_start.id")?,
            content: self.blocks.into_values().collect(),
            stop_reason: self.stop_reason,
            usage: self.usage,
        })
    }
}

fn event_index(event: &Value) -> Result<usize> {
    event
        .get("index")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .context("SSE content event 缺少 index")
}

fn append_string(block: Option<&mut Value>, field: &str, delta: &str) -> Result<()> {
    let object: &mut Map<String, Value> = block
        .and_then(Value::as_object_mut)
        .context("SSE delta 对应的 content block 不存在")?;
    let target = object
        .entry(field)
        .or_insert_with(|| Value::String(String::new()));
    target
        .as_str()
        .context("SSE content block 字段不是 string")?;
    if let Value::String(value) = target {
        value.push_str(delta);
    }
    Ok(())
}

fn default_usage() -> crate::types::Usage {
    crate::types::Usage {
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    }
}

fn retryable(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || status.as_u16() == 529
        || status.is_server_error()
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get("retry-after")
        .and_then(|value: &HeaderValue| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn build_messages_url(endpoint: &EndpointConfig) -> Result<reqwest::Url> {
    let base = reqwest::Url::parse(&endpoint.base_url)
        .with_context(|| format!("HARNESS_BASE_URL 无效: {}", endpoint.base_url))?;
    if !matches!(base.scheme(), "http" | "https") || base.host_str().is_none() {
        bail!("HARNESS_BASE_URL 只支持带 host 的 http/https URL")
    }
    if !base.username().is_empty() || base.password().is_some() {
        bail!("HARNESS_BASE_URL 不得内嵌用户名或密码")
    }
    if base.query().is_some() || base.fragment().is_some() {
        bail!("HARNESS_BASE_URL 不得包含 query 或 fragment")
    }
    if endpoint.messages_path.contains('#') {
        bail!("HARNESS_MESSAGES_PATH 不得包含 fragment")
    }
    let separator = if endpoint.messages_path.starts_with('/') {
        ""
    } else {
        "/"
    };
    let candidate = format!(
        "{}{}{}",
        endpoint.base_url.trim_end_matches('/'),
        separator,
        endpoint.messages_path
    );
    let url = reqwest::Url::parse(&candidate)
        .with_context(|| format!("messages endpoint 无效: {candidate}"))?;
    let same_origin = url.scheme() == base.scheme()
        && url.host_str() == base.host_str()
        && url.port_or_known_default() == base.port_or_known_default()
        && url.username().is_empty()
        && url.password().is_none();
    if !same_origin {
        bail!("HARNESS_MESSAGES_PATH 不得改变 endpoint origin")
    }
    Ok(url)
}

async fn read_body_limited(
    response: reqwest::Response,
    limit: usize,
    label: &str,
) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        bail!("{label}超过 {limit} 字节限制")
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("读取{label}失败"))?;
        if body.len().saturating_add(chunk.len()) > limit {
            bail!("{label}超过 {limit} 字节限制")
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn api_error(status: StatusCode, body: &str, secret: Option<&str>) -> anyhow::Error {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| truncate(body, 2000));
    let message = redact_text(&message, secret);
    anyhow::anyhow!("Model endpoint {}: {}", status.as_u16(), message)
}

fn redact_response(response: &mut ModelResponse, secret: Option<&str>) {
    response.id = redact_text(&response.id, secret);
    if let Some(reason) = &mut response.stop_reason {
        *reason = redact_text(reason, secret);
    }
    for block in &mut response.content {
        redact_value(block, secret);
    }
}

fn redact_value(value: &mut Value, secret: Option<&str>) {
    match value {
        Value::String(text) => *text = redact_text(text, secret),
        Value::Array(values) => {
            for value in values {
                redact_value(value, secret);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact_value(value, secret);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_text(value: &str, secret: Option<&str>) -> String {
    match secret.filter(|secret| !secret.is_empty()) {
        Some(secret) => {
            let redacted = value.replace(secret, "<redacted-endpoint-token>");
            let encoded = serde_json::to_string(secret).unwrap_or_default();
            let escaped = encoded
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or("");
            if escaped.is_empty() || escaped == secret {
                redacted
            } else {
                redacted.replace(escaped, "<redacted-endpoint-token>")
            }
        }
        None => value.to_owned(),
    }
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    value.chars().take(max).collect::<String>() + "…"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(base_url: &str, messages_path: &str) -> EndpointConfig {
        EndpointConfig {
            token: None,
            base_url: base_url.into(),
            messages_path: messages_path.into(),
            allow_env_proxy: false,
        }
    }

    #[test]
    fn endpoint_validation_preserves_prefix_and_rejects_credentials() {
        let url =
            build_messages_url(&endpoint("https://example.invalid/root", "/messages")).unwrap();
        assert_eq!(url.as_str(), "https://example.invalid/root/messages");
        assert!(build_messages_url(&endpoint("file:///tmp/socket", "/messages")).is_err());
        assert!(
            build_messages_url(&endpoint(
                "https://user:secret@example.invalid",
                "/messages"
            ))
            .is_err()
        );
    }

    #[test]
    fn endpoint_token_is_redacted_from_responses_and_errors() {
        let secret = "token-\"private\"";
        let mut response = ModelResponse {
            id: secret.into(),
            content: vec![json!({"type":"text","text":format!("echo {secret}")})],
            stop_reason: Some(secret.into()),
            usage: None,
        };
        redact_response(&mut response, Some(secret));
        assert!(!serde_json::to_string(&response).unwrap().contains(secret));

        let body = json!({"error":{"message":format!("reflected {secret}")}}).to_string();
        let error = api_error(StatusCode::BAD_REQUEST, &body, Some(secret));
        assert!(!error.to_string().contains(secret));
        assert!(error.to_string().contains("redacted-endpoint-token"));
    }

    #[test]
    fn initial_sse_text_is_streamed_before_later_deltas() {
        let captured = std::sync::Mutex::new(String::new());
        let callback = |text: &str| captured.lock().unwrap().push_str(text);
        let mut accumulator = StreamAccumulator::default();
        assert!(
            accumulator
                .apply(
                    json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"hello"}}),
                    Some(&callback),
                    None,
                )
                .unwrap()
        );
        assert!(
            accumulator
                .apply(
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" world"}}),
                    Some(&callback),
                    None,
                )
                .unwrap()
        );
        assert_eq!(*captured.lock().unwrap(), "hello world");
        assert_eq!(accumulator.blocks[&0]["text"], "hello world");
    }
}
