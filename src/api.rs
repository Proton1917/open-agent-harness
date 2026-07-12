use std::time::Duration;

use anyhow::{Context, Result};
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
}

pub struct MessageResult {
    pub response: ModelResponse,
    pub streamed_text: bool,
}

impl ModelClient {
    pub fn new(endpoint: EndpointConfig) -> Result<Self> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600))
            .build()
            .context("无法创建 HTTP client")?;
        Ok(Self { http, endpoint })
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
        let path = if self.endpoint.messages_path.starts_with('/') {
            self.endpoint.messages_path.clone()
        } else {
            format!("/{}", self.endpoint.messages_path)
        };
        let url = format!("{}{path}", self.endpoint.base_url);
        let mut last_error = None;
        for attempt in 0..4u32 {
            let mut request = self
                .http
                .post(&url)
                .header("content-type", "application/json")
                .json(&body);
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
                    return parse_sse(response, on_text_delta).await;
                }
                let text = response.text().await.context("读取 API 响应失败")?;
                let response = serde_json::from_str(&text).with_context(|| {
                    format!("API 返回了无法解析的消息响应: {}", truncate(&text, 1000))
                })?;
                return Ok(MessageResult {
                    response,
                    streamed_text: false,
                });
            }
            let text = response.text().await.context("读取 API 错误响应失败")?;
            let error = api_error(status, &text);
            if retryable(status) && attempt < 3 {
                last_error = Some(error);
                sleep(retry_after.unwrap_or_else(|| Duration::from_secs(1 << attempt))).await;
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
) -> Result<MessageResult> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut accumulator = StreamAccumulator::default();
    let mut streamed_text = false;
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk.context("读取 SSE chunk 失败")?);
        while let Some((frame_end, separator_len)) = find_frame_end(&buffer) {
            let frame = buffer.drain(..frame_end).collect::<Vec<_>>();
            buffer.drain(..separator_len);
            if let Some(data) = frame_data(&frame)? {
                if data == "[DONE]" {
                    continue;
                }
                let event: Value = serde_json::from_str(&data)
                    .with_context(|| format!("无法解析 SSE data: {}", truncate(&data, 1000)))?;
                streamed_text |= accumulator.apply(event, on_text_delta)?;
            }
        }
    }
    if !buffer.iter().all(u8::is_ascii_whitespace)
        && let Some(data) = frame_data(&buffer)?
    {
        let event: Value = serde_json::from_str(&data)?;
        streamed_text |= accumulator.apply(event, on_text_delta)?;
    }
    Ok(MessageResult {
        response: accumulator.finish()?,
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
                let block = event
                    .get("content_block")
                    .cloned()
                    .context("content_block_start 缺少 content_block")?;
                self.blocks.insert(index, block);
            }
            "content_block_delta" => {
                let index = event_index(&event)?;
                let delta = event
                    .get("delta")
                    .context("content_block_delta 缺少 delta")?;
                match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text_delta" => {
                        let text = delta.get("text").and_then(Value::as_str).unwrap_or("");
                        append_string(self.blocks.get_mut(&index), "text", text)?;
                        if let Some(callback) = on_text_delta {
                            callback(text);
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
                let message = event
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("未知 SSE error");
                anyhow::bail!("Model stream error: {message}")
            }
            "ping" | "message_stop" => {}
            _ => {}
        }
        Ok(false)
    }

    fn finish(self) -> Result<ModelResponse> {
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

fn api_error(status: StatusCode, body: &str) -> anyhow::Error {
    let message = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| truncate(body, 2000));
    anyhow::anyhow!("Model endpoint {}: {}", status.as_u16(), message)
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    value.chars().take(max).collect::<String>() + "…"
}
