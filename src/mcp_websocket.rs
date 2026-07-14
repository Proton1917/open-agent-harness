use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::HeaderMap;
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    sync::{Mutex, broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::HeaderValue,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use url::Url;

use crate::{
    mcp::{McpRpc, TokenCredentialProvider, authorized_headers},
    rpc::RpcServerRequestHandler,
    web_tools::resolve_target,
};

const MAX_WS_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_WS_FRAME_BYTES: usize = 1024 * 1024;
const MAX_WS_PENDING_REQUESTS: usize = 64;
const MAX_WS_COMMANDS: usize = 128;
const MAX_WS_ERROR_BYTES: usize = 2048;
const MAX_WS_RECONNECT_ATTEMPTS: usize = 3;
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const WS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct WebSocketMcpConfig {
    pub label: String,
    pub url: Url,
    pub headers: HeaderMap,
    pub configured_secrets: Vec<String>,
    pub credential: Option<TokenCredentialProvider>,
    pub allow_private_network: bool,
    pub request_timeout: Duration,
    pub server_request_handler: RpcServerRequestHandler,
}

pub(crate) struct WebSocketMcpRpc {
    label: String,
    request_timeout: Duration,
    commands: mpsc::Sender<Command>,
    events: broadcast::Sender<Value>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    diagnostics: Arc<StdRwLock<String>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

enum Command {
    Request {
        id: u64,
        message: Value,
        response: oneshot::Sender<std::result::Result<Value, String>>,
    },
    Notify {
        message: Value,
        written: oneshot::Sender<std::result::Result<(), String>>,
    },
    Cancel {
        id: u64,
    },
    Shutdown {
        finished: oneshot::Sender<()>,
    },
}

type Pending = HashMap<String, oneshot::Sender<std::result::Result<Value, String>>>;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct ConnectedSocket {
    socket: Socket,
    dynamic_secret: Option<String>,
}

enum ConnectionEnd {
    Retryable(String),
    Fatal(String),
    Shutdown,
}

impl WebSocketMcpRpc {
    pub(crate) async fn connect(config: WebSocketMcpConfig) -> Result<Self> {
        validate_ws_url(&config.url)?;
        let connected = connect_socket(&config).await?;
        let label = config.label.clone();
        let request_timeout = config.request_timeout;
        let (commands, receiver) = mpsc::channel(MAX_WS_COMMANDS);
        let (events, _) = broadcast::channel(128);
        let closed = Arc::new(AtomicBool::new(false));
        let diagnostics = Arc::new(StdRwLock::new(String::new()));
        let task = tokio::spawn(run_actor(
            config,
            connected,
            receiver,
            events.clone(),
            Arc::clone(&closed),
            Arc::clone(&diagnostics),
        ));
        Ok(Self {
            label,
            request_timeout,
            commands,
            events,
            next_id: AtomicU64::new(1),
            closed,
            diagnostics,
            task: Mutex::new(Some(task)),
        })
    }

    async fn send_command(&self, command: Command) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            bail!("{} WebSocket transport 已关闭", self.label)
        }
        self.commands
            .send(command)
            .await
            .map_err(|_| anyhow::anyhow!("{} WebSocket transport 已关闭", self.label))
    }

    async fn request_bounded(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            bail!("{} WebSocket request id 已耗尽", self.label)
        }
        let mut message = json!({"jsonrpc":"2.0", "id":id, "method":method});
        if let Some(params) = params {
            message["params"] = params;
        }
        validate_outbound_message(&message)?;
        let (sender, receiver) = oneshot::channel();
        self.send_command(Command::Request {
            id,
            message,
            response: sender,
        })
        .await?;
        match timeout(request_timeout, receiver).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => bail!("{error}"),
            Ok(Err(_)) => bail!("{} WebSocket transport 在响应前关闭", self.label),
            Err(_) => {
                let _ = self.commands.send(Command::Cancel { id }).await;
                bail!(
                    "{} WebSocket request {method} 超过 {}ms timeout",
                    self.label,
                    request_timeout.as_millis()
                )
            }
        }
    }
}

#[async_trait]
impl McpRpc for WebSocketMcpRpc {
    async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        self.request_bounded(method, params, self.request_timeout)
            .await
    }

    async fn request_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        self.request_bounded(method, params, request_timeout).await
    }

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        let mut message = json!({"jsonrpc":"2.0", "method":method});
        if let Some(params) = params {
            message["params"] = params;
        }
        validate_outbound_message(&message)?;
        let (sender, receiver) = oneshot::channel();
        self.send_command(Command::Notify {
            message,
            written: sender,
        })
        .await?;
        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => bail!("{error}"),
            Ok(Err(_)) => bail!("{} WebSocket transport 在写入前关闭", self.label),
            Err(_) => bail!("{} WebSocket notification 写入 timeout", self.label),
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    async fn set_protocol_version(&self, _: &str) {}

    async fn start_notifications(&self) {}

    async fn diagnostic_excerpt(&self) -> String {
        self.diagnostics
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let (sender, receiver) = oneshot::channel();
        let _ = self
            .commands
            .send(Command::Shutdown { finished: sender })
            .await;
        let _ = timeout(WS_SHUTDOWN_TIMEOUT, receiver).await;
        if let Some(mut task) = self.task.lock().await.take() {
            let _ = timeout(WS_SHUTDOWN_TIMEOUT, &mut task).await;
            task.abort();
        }
    }
}

impl Drop for WebSocketMcpRpc {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Some(task) = self.task.get_mut().take() {
            task.abort();
        }
    }
}

async fn run_actor(
    config: WebSocketMcpConfig,
    mut connected: ConnectedSocket,
    mut commands: mpsc::Receiver<Command>,
    events: broadcast::Sender<Value>,
    closed: Arc<AtomicBool>,
    diagnostics: Arc<StdRwLock<String>>,
) {
    let mut pending = HashMap::new();
    loop {
        let outcome = run_connection(
            &config,
            &mut connected,
            &mut commands,
            &events,
            &mut pending,
        )
        .await;
        fail_pending(&mut pending, connection_reason(&outcome));
        match outcome {
            ConnectionEnd::Shutdown => break,
            ConnectionEnd::Fatal(reason) => {
                set_diagnostic(&diagnostics, &reason);
                break;
            }
            ConnectionEnd::Retryable(reason) => {
                set_diagnostic(&diagnostics, &reason);
                let mut replacement = None;
                for attempt in 0..MAX_WS_RECONNECT_ATTEMPTS {
                    if closed.load(Ordering::Acquire) {
                        break;
                    }
                    if attempt > 0 {
                        sleep(Duration::from_millis(100 * (1u64 << attempt))).await;
                    }
                    match connect_socket(&config).await {
                        Ok(socket) => {
                            replacement = Some(socket);
                            break;
                        }
                        Err(error) => set_diagnostic(
                            &diagnostics,
                            &format!("WebSocket reconnect attempt failed: {}", safe_error(&error)),
                        ),
                    }
                }
                let Some(socket) = replacement else {
                    break;
                };
                connected = socket;
            }
        }
    }
    closed.store(true, Ordering::Release);
    while let Ok(command) = commands.try_recv() {
        fail_command(command, "WebSocket transport 已关闭");
    }
}

async fn run_connection(
    config: &WebSocketMcpConfig,
    connected: &mut ConnectedSocket,
    commands: &mut mpsc::Receiver<Command>,
    events: &broadcast::Sender<Value>,
    pending: &mut Pending,
) -> ConnectionEnd {
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else {
                    let _ = connected.socket.close(None).await;
                    return ConnectionEnd::Shutdown;
                };
                match command {
                    Command::Request { id, message, response } => {
                        if pending.len() >= MAX_WS_PENDING_REQUESTS {
                            let _ = response.send(Err(format!(
                                "{} WebSocket pending request 超过 {MAX_WS_PENDING_REQUESTS} 项限制",
                                config.label
                            )));
                            continue;
                        }
                        let key = format!("n:{id}");
                        let text = match serialize_message(&message) {
                            Ok(text) => text,
                            Err(error) => {
                                let _ = response.send(Err(safe_error(&error)));
                                continue;
                            }
                        };
                        if let Err(error) = connected.socket.send(Message::text(text)).await {
                            let _ = response.send(Err("WebSocket request write 失败".to_owned()));
                            return ConnectionEnd::Retryable(safe_ws_error(&error));
                        }
                        pending.insert(key, response);
                    }
                    Command::Notify { message, written } => {
                        let text = match serialize_message(&message) {
                            Ok(text) => text,
                            Err(error) => {
                                let _ = written.send(Err(safe_error(&error)));
                                continue;
                            }
                        };
                        match connected.socket.send(Message::text(text)).await {
                            Ok(()) => { let _ = written.send(Ok(())); }
                            Err(error) => {
                                let _ = written.send(Err("WebSocket notification write 失败".to_owned()));
                                return ConnectionEnd::Retryable(safe_ws_error(&error));
                            }
                        }
                    }
                    Command::Cancel { id } => {
                        pending.remove(&format!("n:{id}"));
                        let cancellation = json!({
                            "jsonrpc":"2.0",
                            "method":"notifications/cancelled",
                            "params":{"requestId":id, "reason":"client timeout"}
                        });
                        if let Ok(text) = serialize_message(&cancellation) {
                            let _ = connected.socket.send(Message::text(text)).await;
                        }
                    }
                    Command::Shutdown { finished } => {
                        let _ = connected.socket.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Normal,
                            reason: "client shutdown".into(),
                        }))).await;
                        let _ = timeout(WS_SHUTDOWN_TIMEOUT, connected.socket.flush()).await;
                        let _ = finished.send(());
                        return ConnectionEnd::Shutdown;
                    }
                }
            }
            frame = connected.socket.next() => {
                match frame {
                    Some(Ok(Message::Text(text))) => {
                        if text.len() > MAX_WS_MESSAGE_BYTES {
                            let _ = close_protocol(&mut connected.socket, CloseCode::Size, "message too large").await;
                            return ConnectionEnd::Fatal("WebSocket message 超过大小限制".to_owned());
                        }
                        let mut message = match parse_inbound_message(text.as_bytes()) {
                            Ok(message) => message,
                            Err(error) => {
                                let _ = close_protocol(&mut connected.socket, CloseCode::Protocol, "invalid JSON-RPC").await;
                                return ConnectionEnd::Fatal(safe_error(&error));
                            }
                        };
                        let mut secrets = config.configured_secrets.clone();
                        if let Some(secret) = &connected.dynamic_secret {
                            secrets.push(secret.clone());
                        }
                        redact_json_secrets(&mut message, &secrets);
                        if let Some(method) = message.get("method").and_then(Value::as_str) {
                            let _ = events.send(message.clone());
                            if let Some(id) = message.get("id") {
                                let response = match (config.server_request_handler)(method, message.get("params")) {
                                    Some(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
                                    None => json!({
                                        "jsonrpc":"2.0", "id":id,
                                        "error":{"code":-32601,"message":"Client method not supported"}
                                    }),
                                };
                                match serialize_message(&response) {
                                    Ok(text) => {
                                        if let Err(error) = connected.socket.send(Message::text(text)).await {
                                            return ConnectionEnd::Retryable(safe_ws_error(&error));
                                        }
                                    }
                                    Err(error) => return ConnectionEnd::Fatal(safe_error(&error)),
                                }
                            }
                            continue;
                        }
                        let id = match message.get("id") {
                            Some(id) => id,
                            None => return ConnectionEnd::Fatal("WebSocket JSON-RPC response 缺少 id".to_owned()),
                        };
                        let key = match id_key(id) {
                            Ok(key) => key,
                            Err(error) => return ConnectionEnd::Fatal(safe_error(&error)),
                        };
                        let Some(sender) = pending.remove(&key) else {
                            continue;
                        };
                        let _ = sender.send(parse_response(&message, &config.label));
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(error) = connected.socket.send(Message::Pong(payload)).await {
                            return ConnectionEnd::Retryable(safe_ws_error(&error));
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        if frame.as_ref().is_some_and(|frame| frame.code == CloseCode::Normal) {
                            return ConnectionEnd::Fatal("WebSocket server 正常关闭连接".to_owned());
                        }
                        return ConnectionEnd::Retryable("WebSocket server 异常关闭连接".to_owned());
                    }
                    Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {
                        let _ = close_protocol(&mut connected.socket, CloseCode::Unsupported, "text JSON-RPC required").await;
                        return ConnectionEnd::Fatal("WebSocket 只接受 text JSON-RPC message".to_owned());
                    }
                    Some(Err(error)) => return ConnectionEnd::Retryable(safe_ws_error(&error)),
                    None => return ConnectionEnd::Retryable("WebSocket stream 意外结束".to_owned()),
                }
            }
        }
    }
}

async fn connect_socket(config: &WebSocketMcpConfig) -> Result<ConnectedSocket> {
    validate_ws_url(&config.url)?;
    let mut lookup_url = config.url.clone();
    lookup_url
        .set_scheme(if config.url.scheme() == "wss" {
            "https"
        } else {
            "http"
        })
        .map_err(|_| anyhow::anyhow!("WebSocket URL scheme 无效"))?;
    let target = resolve_target(&lookup_url, config.allow_private_network).await?;
    let stream = timeout(WS_CONNECT_TIMEOUT, TcpStream::connect(target))
        .await
        .map_err(|_| anyhow::anyhow!("{} WebSocket connect timeout", config.label))?
        .map_err(|_| anyhow::anyhow!("{} WebSocket TCP connect 失败", config.label))?;
    stream.set_nodelay(true).ok();
    let mut request = config
        .url
        .as_str()
        .into_client_request()
        .context("WebSocket handshake request 无效")?;
    let (headers, dynamic_secret) =
        authorized_headers(&config.headers, config.credential.as_ref()).await?;
    for (name, value) in &headers {
        request.headers_mut().insert(name, value.clone());
    }
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static("mcp"));
    request.headers_mut().insert(
        "origin",
        HeaderValue::from_str(&websocket_origin(&config.url))
            .context("WebSocket Origin header 无效")?,
    );
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_FRAME_BYTES))
        .max_write_buffer_size(MAX_WS_MESSAGE_BYTES * 2);
    let (socket, response) = timeout(
        WS_CONNECT_TIMEOUT,
        client_async_tls_with_config(request, stream, Some(websocket_config), None),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{} WebSocket handshake timeout", config.label))?
    .map_err(|_| anyhow::anyhow!("{} WebSocket handshake 失败", config.label))?;
    let selected = response
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|value| value.to_str().ok());
    if selected != Some("mcp") {
        bail!("{} WebSocket server 未协商 mcp subprotocol", config.label)
    }
    Ok(ConnectedSocket {
        socket,
        dynamic_secret,
    })
}

fn validate_ws_url(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        bail!("MCP WebSocket URL 必须是无凭据、无 fragment 的 ws(s) URL")
    }
    if url.as_str().len() > 16 * 1024 {
        bail!("MCP WebSocket URL 超过大小限制")
    }
    for (key, _) in url.query_pairs() {
        if sensitive_query_key(&key) {
            bail!("MCP WebSocket URL 不允许在 query 中携带凭据参数")
        }
    }
    Ok(())
}

fn validate_outbound_message(message: &Value) -> Result<()> {
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || message.get("method").and_then(Value::as_str).is_none()
    {
        bail!("outbound WebSocket message 不是有效 JSON-RPC 2.0 request/notification")
    }
    let bytes = serde_json::to_vec(message)?;
    if bytes.len() > MAX_WS_MESSAGE_BYTES {
        bail!("outbound WebSocket message 超过大小限制")
    }
    Ok(())
}

fn parse_inbound_message(bytes: &[u8]) -> Result<Value> {
    if bytes.len() > MAX_WS_MESSAGE_BYTES {
        bail!("WebSocket message 超过大小限制")
    }
    let message: Value =
        serde_json::from_slice(bytes).context("WebSocket message 不是有效 JSON")?;
    let object = message
        .as_object()
        .context("WebSocket JSON-RPC message 必须是 object")?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        bail!("WebSocket message 缺少 jsonrpc=2.0")
    }
    if object.contains_key("method") {
        let method = object
            .get("method")
            .and_then(Value::as_str)
            .context("WebSocket JSON-RPC method 必须是 string")?;
        if method.is_empty() || method.len() > 1024 || method.contains('\0') {
            bail!("WebSocket JSON-RPC method 无效或超过限制")
        }
        if let Some(id) = object.get("id") {
            id_key(id)?;
        }
    } else {
        let id = object
            .get("id")
            .context("WebSocket JSON-RPC response 缺少 id")?;
        id_key(id)?;
        match (object.get("result"), object.get("error")) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => bail!("WebSocket JSON-RPC response 必须且只能包含 result 或 error"),
        }
    }
    Ok(message)
}

fn serialize_message(message: &Value) -> Result<String> {
    let text = serde_json::to_string(message)?;
    if text.len() > MAX_WS_MESSAGE_BYTES {
        bail!("WebSocket message 超过大小限制")
    }
    Ok(text)
}

fn parse_response(message: &Value, label: &str) -> std::result::Result<Value, String> {
    match (message.get("result"), message.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
            let text = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown RPC error");
            let safe = sanitize_text(text, MAX_WS_ERROR_BYTES);
            Err(format!("{label} WebSocket RPC error {code}: {safe}"))
        }
        _ => Err(format!(
            "{label} WebSocket response 必须且只能包含 result 或 error"
        )),
    }
}

fn id_key(id: &Value) -> Result<String> {
    match id {
        Value::String(value) if !value.is_empty() && value.len() <= 1024 => {
            Ok(format!("s:{value}"))
        }
        Value::Number(value) => Ok(format!("n:{value}")),
        _ => bail!("WebSocket JSON-RPC id 必须是 bounded string 或 number"),
    }
}

fn fail_pending(pending: &mut Pending, reason: &str) {
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(reason.to_owned()));
    }
}

fn fail_command(command: Command, reason: &str) {
    match command {
        Command::Request { response, .. } => {
            let _ = response.send(Err(reason.to_owned()));
        }
        Command::Notify { written, .. } => {
            let _ = written.send(Err(reason.to_owned()));
        }
        Command::Shutdown { finished } => {
            let _ = finished.send(());
        }
        Command::Cancel { .. } => {}
    }
}

async fn close_protocol(socket: &mut Socket, code: CloseCode, reason: &str) -> Result<()> {
    socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await
        .context("WebSocket close frame 发送失败")
}

fn connection_reason(outcome: &ConnectionEnd) -> &str {
    match outcome {
        ConnectionEnd::Retryable(reason) | ConnectionEnd::Fatal(reason) => reason,
        ConnectionEnd::Shutdown => "WebSocket transport 正在关闭",
    }
}

fn set_diagnostic(target: &StdRwLock<String>, value: &str) {
    *target
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
        sanitize_text(value, MAX_WS_ERROR_BYTES);
}

fn safe_error(error: &anyhow::Error) -> String {
    sanitize_text(&error.to_string(), MAX_WS_ERROR_BYTES)
}

fn safe_ws_error(_: &tokio_tungstenite::tungstenite::Error) -> String {
    "WebSocket transport I/O 失败".to_owned()
}

fn sanitize_text(value: &str, maximum: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'))
        .take(maximum)
        .collect()
}

fn redact_json_secrets(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => {
            for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
                *text = text.replace(secret, "[REDACTED]");
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_json_secrets(value, secrets);
            }
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                redact_json_secrets(value, secrets);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn websocket_origin(url: &Url) -> String {
    let scheme = if url.scheme() == "wss" {
        "https"
    } else {
        "http"
    };
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

fn sensitive_query_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let normalized = String::from_utf8_lossy(&normalized);
    normalized == "key"
        || normalized == "sig"
        || normalized == "jwt"
        || normalized == "code"
        || normalized.ends_with("key")
        || normalized.contains("auth")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("session")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::handshake::server::{Request, Response},
    };

    #[test]
    fn websocket_json_rpc_frames_are_strict_and_bounded() {
        assert!(parse_inbound_message(br#"{"jsonrpc":"2.0","id":1,"result":{}}"#).is_ok());
        assert!(parse_inbound_message(br#"{"jsonrpc":"1.0","id":1,"result":{}}"#).is_err());
        assert!(
            parse_inbound_message(br#"{"jsonrpc":"2.0","id":1,"result":{},"error":{}}"#).is_err()
        );
        assert!(parse_inbound_message(br#"{"jsonrpc":"2.0","method":1}"#).is_err());
        assert!(validate_ws_url(&Url::parse("ws://example.invalid/mcp?token=x").unwrap()).is_err());
        assert_eq!(
            websocket_origin(&Url::parse("wss://example.invalid/mcp").unwrap()),
            "https://example.invalid"
        );
    }

    #[test]
    fn websocket_response_errors_are_bounded_and_secrets_are_redacted() {
        let mut value = json!({"text":"token private-value"});
        redact_json_secrets(&mut value, &["private-value".to_owned()]);
        assert!(!value.to_string().contains("private-value"));
        let response = json!({
            "jsonrpc":"2.0", "id":1,
            "error":{"code":-32000,"message":"x".repeat(MAX_WS_ERROR_BYTES * 2)}
        });
        let error = parse_response(&response, "mock").unwrap_err();
        assert!(error.len() < MAX_WS_ERROR_BYTES + 128);
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_mock_server_round_trip_enforces_origin_and_subprotocol() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket =
                accept_hdr_async(stream, |request: &Request, mut response: Response| {
                    assert_eq!(request.uri().path(), "/mcp");
                    assert_eq!(
                        request.headers().get("origin").unwrap(),
                        &format!("http://{address}")
                    );
                    assert_eq!(
                        request.headers().get("sec-websocket-protocol").unwrap(),
                        "mcp"
                    );
                    response
                        .headers_mut()
                        .insert("sec-websocket-protocol", HeaderValue::from_static("mcp"));
                    Ok(response)
                })
                .await
                .unwrap();
            let request = socket.next().await.unwrap().unwrap();
            let Message::Text(request) = request else {
                panic!("expected text request")
            };
            let request: Value = serde_json::from_str(request.as_str()).unwrap();
            socket
                .send(Message::text(
                    json!({
                        "jsonrpc":"2.0",
                        "id":request["id"],
                        "result":{"ok":true}
                    })
                    .to_string(),
                ))
                .await
                .unwrap();
            let _ = socket.next().await;
        });
        let rpc = WebSocketMcpRpc::connect(WebSocketMcpConfig {
            label: "MCP/mock".to_owned(),
            url: Url::parse(&format!("ws://{address}/mcp")).unwrap(),
            headers: HeaderMap::new(),
            configured_secrets: Vec::new(),
            credential: None,
            allow_private_network: true,
            request_timeout: Duration::from_secs(2),
            server_request_handler: Arc::new(|_, _| None),
        })
        .await
        .unwrap();
        assert_eq!(
            rpc.request("test/echo", Some(json!({"value":1})))
                .await
                .unwrap(),
            json!({"ok":true})
        );
        rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_mock_server_rejects_missing_protocol() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _socket = accept_hdr_async(stream, |_: &Request, response: Response| Ok(response))
                .await
                .unwrap();
        });
        let result = WebSocketMcpRpc::connect(WebSocketMcpConfig {
            label: "MCP/mock".to_owned(),
            url: Url::parse(&format!("ws://{address}/mcp")).unwrap(),
            headers: HeaderMap::new(),
            configured_secrets: Vec::new(),
            credential: None,
            allow_private_network: true,
            request_timeout: Duration::from_secs(1),
            server_request_handler: Arc::new(|_, _| None),
        })
        .await;
        let error = match result {
            Ok(client) => {
                client.shutdown().await;
                panic!("missing mcp subprotocol must be rejected")
            }
            Err(error) => error,
        };
        assert!(error.to_string().contains("handshake"));
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_mock_server_malformed_jsonrpc_closes_fail_closed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, |_: &Request, mut response: Response| {
                response
                    .headers_mut()
                    .insert("sec-websocket-protocol", HeaderValue::from_static("mcp"));
                Ok(response)
            })
            .await
            .unwrap();
            let _ = socket.next().await;
            socket.send(Message::text("not-json")).await.unwrap();
            let _ = socket.next().await;
        });
        let rpc = WebSocketMcpRpc::connect(WebSocketMcpConfig {
            label: "MCP/mock".to_owned(),
            url: Url::parse(&format!("ws://{address}/mcp")).unwrap(),
            headers: HeaderMap::new(),
            configured_secrets: Vec::new(),
            credential: None,
            allow_private_network: true,
            request_timeout: Duration::from_secs(1),
            server_request_handler: Arc::new(|_, _| None),
        })
        .await
        .unwrap();
        let error = rpc.request("test/fail", None).await.unwrap_err();
        assert!(error.to_string().contains("有效 JSON"));
        rpc.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_reconnect_fails_inflight_without_replay_then_accepts_new_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_hdr_async(stream, |_: &Request, mut response: Response| {
                    response
                        .headers_mut()
                        .insert("sec-websocket-protocol", HeaderValue::from_static("mcp"));
                    Ok(response)
                })
                .await
                .unwrap();
                let request = socket.next().await.unwrap().unwrap();
                let Message::Text(request) = request else {
                    panic!("expected text request")
                };
                if connection == 0 {
                    drop(socket);
                    continue;
                }
                let request: Value = serde_json::from_str(request.as_str()).unwrap();
                socket
                    .send(Message::text(
                        json!({
                            "jsonrpc":"2.0",
                            "id":request["id"],
                            "result":{"connection":2}
                        })
                        .to_string(),
                    ))
                    .await
                    .unwrap();
                let _ = socket.next().await;
            }
        });
        let rpc = WebSocketMcpRpc::connect(WebSocketMcpConfig {
            label: "MCP/mock".to_owned(),
            url: Url::parse(&format!("ws://{address}/mcp")).unwrap(),
            headers: HeaderMap::new(),
            configured_secrets: Vec::new(),
            credential: None,
            allow_private_network: true,
            request_timeout: Duration::from_secs(2),
            server_request_handler: Arc::new(|_, _| None),
        })
        .await
        .unwrap();
        let first = rpc.request("test/first", None).await.unwrap_err();
        assert!(first.to_string().contains("WebSocket"));
        let second = rpc.request("test/second", None).await.unwrap();
        assert_eq!(second, json!({"connection":2}));
        rpc.shutdown().await;
        server.await.unwrap();
    }
}
