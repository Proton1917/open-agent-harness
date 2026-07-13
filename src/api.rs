use std::{ops::Range, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use serde_json::Value;
use tokio::time::sleep;

use crate::{
    config::EndpointConfig,
    protocol::{ApiFormat, RequestParts, StreamDecoder, encode_request, parse_response},
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
        let mut endpoint = endpoint;
        endpoint.api_format = endpoint.api_format.infer(&endpoint.messages_path);
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
        let body = encode_request(
            self.endpoint.api_format,
            RequestParts {
                model,
                max_tokens,
                system,
                messages,
                tools,
                stream: self.endpoint.stream,
                chat_tokens_field: self.endpoint.chat_tokens_field,
                include_stream_usage: self.endpoint.include_stream_usage,
            },
        )?;
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
                let is_sse = response_is_sse(response.headers());
                if is_sse {
                    return parse_sse(
                        response,
                        self.endpoint.api_format,
                        on_text_delta,
                        self.endpoint.token.as_deref(),
                    )
                    .await;
                }
                let bytes = read_body_limited(response, MAX_RESPONSE_BYTES, "API 响应").await?;
                let mut value: Value = serde_json::from_slice(&bytes).with_context(|| {
                    format!(
                        "API 返回了无法解析的 JSON 响应: {}",
                        truncate(
                            &redact_text(
                                &String::from_utf8_lossy(&bytes),
                                self.endpoint.token.as_deref()
                            ),
                            1000
                        )
                    )
                })?;
                redact_value(&mut value, self.endpoint.token.as_deref());
                let mut response = parse_response(self.endpoint.api_format, value)
                    .context("API 返回了无效的 model response")?;
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
    api_format: ApiFormat,
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
    let mut frames = SseFrameCursor::default();
    let mut received = 0usize;
    let mut decoder = StreamDecoder::new(api_format)?;
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
        while let Some(frame) = frames.next_frame(&buffer, false) {
            if frame.len() > MAX_SSE_FRAME_BYTES {
                bail!("SSE frame 超过 {MAX_SSE_FRAME_BYTES} 字节限制")
            }
            streamed_text |= decode_sse_frame(&buffer[frame], &mut decoder, on_text_delta, secret)?;
        }
        if frames.pending_len(buffer.len()) > MAX_SSE_FRAME_BYTES {
            bail!("SSE frame 超过 {MAX_SSE_FRAME_BYTES} 字节限制")
        }
    }

    // A trailing CR is ambiguous until the next byte arrives. Resolve it as a
    // standalone line ending only after EOF, while keeping the incremental scan
    // cursor so every response byte is inspected a constant number of times.
    while let Some(frame) = frames.next_frame(&buffer, true) {
        if frame.len() > MAX_SSE_FRAME_BYTES {
            bail!("SSE frame 超过 {MAX_SSE_FRAME_BYTES} 字节限制")
        }
        streamed_text |= decode_sse_frame(&buffer[frame], &mut decoder, on_text_delta, secret)?;
    }
    let pending = frames.pending(&buffer);
    if !pending.iter().all(u8::is_ascii_whitespace) {
        streamed_text |= decode_sse_frame(pending, &mut decoder, on_text_delta, secret)?;
    }
    let mut response = decoder.finish()?;
    redact_response(&mut response, secret);
    Ok(MessageResult {
        response,
        streamed_text,
    })
}

fn decode_sse_frame(
    frame: &[u8],
    decoder: &mut StreamDecoder,
    on_text_delta: Option<&(dyn Fn(&str) + Send + Sync)>,
    secret: Option<&str>,
) -> Result<bool> {
    let Some(data) = frame_data(frame)? else {
        return Ok(false);
    };
    if data == "[DONE]" {
        decoder.mark_done()?;
        return Ok(false);
    }
    let mut event: Value = serde_json::from_str(&data).with_context(|| {
        format!(
            "无法解析 SSE data: {}",
            truncate(&redact_text(&data, secret), 1000)
        )
    })?;
    redact_value(&mut event, secret);
    decoder.apply(event, on_text_delta)
}

#[derive(Default)]
struct SseFrameCursor {
    frame_start: usize,
    scan_index: usize,
    #[cfg(test)]
    inspected: usize,
}

impl SseFrameCursor {
    fn next_frame(&mut self, buffer: &[u8], eof: bool) -> Option<Range<usize>> {
        while self.scan_index < buffer.len() {
            #[cfg(test)]
            {
                self.inspected += 1;
            }
            let index = self.scan_index;
            let first = match line_ending(buffer, index, eof) {
                LineEnding::Complete(length) => length,
                LineEnding::Incomplete => return None,
                LineEnding::Absent => {
                    self.scan_index += 1;
                    continue;
                }
            };
            let second_index = index + first;
            match line_ending(buffer, second_index, eof) {
                LineEnding::Complete(second) => {
                    let frame = self.frame_start..index;
                    self.frame_start = second_index + second;
                    self.scan_index = self.frame_start;
                    return Some(frame);
                }
                LineEnding::Incomplete => return None,
                LineEnding::Absent => {
                    // The first line ending cannot begin a separator. Resume
                    // immediately after it; bytes before that point never need
                    // to be scanned again.
                    self.scan_index = second_index;
                }
            }
        }
        None
    }

    fn pending_len(&self, buffer_len: usize) -> usize {
        buffer_len.saturating_sub(self.frame_start)
    }

    fn pending<'a>(&self, buffer: &'a [u8]) -> &'a [u8] {
        &buffer[self.frame_start..]
    }
}

enum LineEnding {
    Complete(usize),
    Incomplete,
    Absent,
}

fn line_ending(buffer: &[u8], index: usize, eof: bool) -> LineEnding {
    match buffer.get(index) {
        Some(b'\r') if buffer.get(index + 1) == Some(&b'\n') => LineEnding::Complete(2),
        Some(b'\r') if buffer.get(index + 1).is_none() && !eof => LineEnding::Incomplete,
        Some(b'\r' | b'\n') => LineEnding::Complete(1),
        None if !eof => LineEnding::Incomplete,
        _ => LineEnding::Absent,
    }
}

#[cfg(test)]
fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let mut cursor = SseFrameCursor::default();
    cursor.next_frame(buffer, true).map(|frame| {
        let separator_len = cursor.frame_start - frame.end;
        (frame.end, separator_len)
    })
}

fn frame_data(frame: &[u8]) -> Result<Option<String>> {
    let text = std::str::from_utf8(frame).context("SSE frame 不是有效 UTF-8")?;
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let parts = normalized
        .split('\n')
        .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>();
    let data = parts.join("\n");
    Ok((!data.is_empty()).then_some(data))
}

fn response_is_sse(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"))
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
        bail!("HARNESS_API_PATH/HARNESS_MESSAGES_PATH 不得包含 fragment")
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
        bail!("HARNESS_API_PATH/HARNESS_MESSAGES_PATH 不得改变 endpoint origin")
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
    use serde_json::json;

    fn endpoint(base_url: &str, messages_path: &str) -> EndpointConfig {
        EndpointConfig {
            token: None,
            base_url: base_url.into(),
            messages_path: messages_path.into(),
            api_format: ApiFormat::Auto,
            stream: true,
            chat_tokens_field: crate::protocol::ChatTokensField::MaxCompletionTokens,
            include_stream_usage: true,
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
    fn sse_framing_uses_the_earliest_mixed_line_ending() {
        let buffer = b"data: one\n\ndata: two\r\n\r\n";
        assert_eq!(find_frame_end(buffer), Some((9, 2)));
        assert_eq!(frame_data(&buffer[..9]).unwrap().as_deref(), Some("one"));
        assert_eq!(find_frame_end(b"data: one\r\r"), Some((9, 2)));
    }

    #[test]
    fn single_byte_sse_chunks_are_scanned_linearly() {
        const PAYLOAD_BYTES: usize = 256 * 1024;
        let mut buffer = vec![b'x'; PAYLOAD_BYTES];
        let mut cursor = SseFrameCursor::default();

        for length in 1..=PAYLOAD_BYTES {
            assert!(cursor.next_frame(&buffer[..length], false).is_none());
        }
        for byte in b"\r\n\r\n" {
            buffer.push(*byte);
            if buffer.len() < PAYLOAD_BYTES + 4 {
                assert!(cursor.next_frame(&buffer, false).is_none());
            }
        }

        assert_eq!(cursor.next_frame(&buffer, false), Some(0..PAYLOAD_BYTES));
        assert_eq!(cursor.pending_len(buffer.len()), 0);
        assert!(
            cursor.inspected <= buffer.len() + 4,
            "{} bytes caused {} scan steps",
            buffer.len(),
            cursor.inspected
        );
    }

    #[test]
    fn empty_data_and_comment_frames_are_ignored() {
        assert_eq!(frame_data(b"data:").unwrap(), None);
        assert_eq!(frame_data(b": keepalive").unwrap(), None);
    }

    #[test]
    fn sse_media_type_is_case_insensitive_and_ignores_parameters() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            HeaderValue::from_static("Text/Event-Stream; charset=UTF-8"),
        );
        assert!(response_is_sse(&headers));
    }
}
