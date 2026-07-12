use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, broadcast},
    task::JoinHandle,
    time::{sleep, timeout},
};
use url::Url;

use crate::{
    config::Settings,
    rpc::{RpcFraming, StdioRpcClient, StdioRpcConfig},
    tools::{
        Tool, ToolContext, ToolDiscovery, ToolOutput, ToolRefresh, ToolService, object_schema,
    },
    web_tools::secure_client_for_url,
};

const CURRENT_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 120_000;
const MIN_REQUEST_TIMEOUT_MS: u64 = 1_000;
const MAX_REQUEST_TIMEOUT_MS: u64 = 600_000;
const MAX_SERVERS: usize = 32;
const MAX_SERVER_NAME_BYTES: usize = 128;
const MAX_COMMAND_BYTES: usize = 4096;
const MAX_ARGS: usize = 128;
const MAX_ARG_BYTES: usize = 32 * 1024;
const MAX_ENV_VARS: usize = 256;
const MAX_ENV_VALUE_BYTES: usize = 256 * 1024;
const MAX_LIST_PAGES: usize = 32;
const MAX_TOOLS_PER_SERVER: usize = 256;
const MAX_PROMPTS_PER_SERVER: usize = 256;
const MAX_RESOURCES: usize = 1024;
const MAX_RESOURCE_URI_BYTES: usize = 16 * 1024;
const MAX_DESCRIPTION_BYTES: usize = 8 * 1024;
const MAX_TOOL_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_HTTP_HEADER_VALUE_BYTES: usize = 16 * 1024;
const MAX_HTTP_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONCURRENT_SERVER_STARTS: usize = 4;

pub struct McpIntegration {
    pub active_tools: Vec<Arc<dyn Tool>>,
    pub deferred_tools: Vec<Arc<dyn Tool>>,
    pub service: Arc<dyn ToolService>,
    pub discovery: Arc<dyn ToolDiscovery>,
    pub server_count: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServerConfig {
    command: Option<String>,
    url: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cwd: Option<String>,
    #[serde(default)]
    disabled: bool,
    #[serde(rename = "timeoutMs")]
    timeout_ms: Option<u64>,
    #[serde(rename = "type")]
    transport_type: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(rename = "allowPrivateNetwork", default)]
    allow_private_network: bool,
}

#[derive(Debug, Clone)]
struct ServerConfig {
    name: String,
    namespace: String,
    transport: ServerTransport,
    request_timeout: Duration,
}

#[derive(Debug, Clone)]
enum ServerTransport {
    Stdio {
        command: String,
        args: Vec<String>,
        env: BTreeMap<String, String>,
        cwd: PathBuf,
    },
    Http {
        url: Url,
        headers: HeaderMap,
        secrets: Vec<String>,
        allow_private_network: bool,
    },
}

#[async_trait]
trait McpRpc: Send + Sync {
    async fn request(&self, method: &str, params: Option<Value>) -> Result<Value>;
    async fn notify(&self, method: &str, params: Option<Value>) -> Result<()>;
    fn subscribe(&self) -> broadcast::Receiver<Value>;
    async fn set_protocol_version(&self, version: &str);
    async fn start_notifications(&self);
    async fn diagnostic_excerpt(&self) -> String;
    async fn shutdown(&self);
}

#[async_trait]
impl McpRpc for StdioRpcClient {
    async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        StdioRpcClient::request(self, method, params).await
    }

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        StdioRpcClient::notify(self, method, params).await
    }

    fn subscribe(&self) -> broadcast::Receiver<Value> {
        StdioRpcClient::subscribe(self)
    }

    async fn set_protocol_version(&self, _: &str) {}

    async fn start_notifications(&self) {}

    async fn diagnostic_excerpt(&self) -> String {
        self.stderr_excerpt().await
    }

    async fn shutdown(&self) {
        StdioRpcClient::shutdown(self).await;
    }
}

struct HttpMcpRpc {
    url: Url,
    headers: HeaderMap,
    secrets: Vec<String>,
    allow_private_network: bool,
    request_timeout: Duration,
    next_id: AtomicU64,
    session_id: Arc<Mutex<Option<String>>>,
    protocol_version: Arc<Mutex<String>>,
    events: broadcast::Sender<Value>,
    listener_started: AtomicBool,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    closing: Arc<AtomicBool>,
}

struct HttpNotificationConfig {
    url: Url,
    headers: HeaderMap,
    session: String,
    protocol_version: Arc<Mutex<String>>,
    allow_private_network: bool,
    secrets: Vec<String>,
}

struct McpClient {
    name: String,
    namespace: String,
    rpc: Arc<dyn McpRpc>,
    supports_tools: bool,
    supports_resources: bool,
    supports_prompts: bool,
    tools_changed: Arc<AtomicBool>,
    resources_changed: Arc<AtomicBool>,
    event_task: Mutex<Option<JoinHandle<()>>>,
}

struct McpManager {
    clients: Vec<Arc<McpClient>>,
    known_tools: Mutex<HashMap<String, HashSet<String>>>,
    strict: bool,
    debug: bool,
}

pub async fn connect_mcp(
    settings: &Settings,
    workspace: &Path,
    debug: bool,
) -> Result<Option<McpIntegration>> {
    let configs = parse_server_configs(settings, workspace)?;
    if configs.is_empty() {
        return Ok(None);
    }
    let strict = settings
        .raw
        .get("strictMcpConfig")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut attempts = stream::iter(configs.into_iter().enumerate())
        .map(|(index, config)| async move { (index, McpClient::connect(config).await) })
        .buffer_unordered(MAX_CONCURRENT_SERVER_STARTS)
        .collect::<Vec<_>>()
        .await;
    attempts.sort_by_key(|(index, _)| *index);
    let mut clients = Vec::new();
    for (_, attempt) in attempts {
        match attempt {
            Ok(client) => clients.push(client),
            Err(error) if !strict => eprintln!("MCP server skipped: {error:#}"),
            Err(error) => {
                for client in &clients {
                    client.shutdown().await;
                }
                return Err(error);
            }
        }
    }
    if clients.is_empty() {
        return Ok(None);
    }
    let manager = Arc::new(McpManager {
        clients,
        known_tools: Mutex::new(HashMap::new()),
        strict,
        debug,
    });
    let deferred_tools = match manager.discover_initial_tools().await {
        Ok(tools) => tools,
        Err(error) => {
            for client in &manager.clients {
                client.shutdown().await;
            }
            return Err(error);
        }
    };
    let mut active_tools: Vec<Arc<dyn Tool>> = Vec::new();
    if manager
        .clients
        .iter()
        .any(|client| client.supports_resources)
    {
        active_tools.push(Arc::new(ListMcpResourcesTool {
            manager: Arc::clone(&manager),
        }));
        active_tools.push(Arc::new(ListMcpResourceTemplatesTool {
            manager: Arc::clone(&manager),
        }));
        active_tools.push(Arc::new(ReadMcpResourceTool {
            manager: Arc::clone(&manager),
        }));
    }
    if manager.clients.iter().any(|client| client.supports_prompts) {
        active_tools.push(Arc::new(ListMcpPromptsTool {
            manager: Arc::clone(&manager),
        }));
        active_tools.push(Arc::new(GetMcpPromptTool {
            manager: Arc::clone(&manager),
        }));
    }
    let server_count = manager.clients.len();
    let service: Arc<dyn ToolService> = manager.clone();
    let discovery: Arc<dyn ToolDiscovery> = manager;
    Ok(Some(McpIntegration {
        active_tools,
        deferred_tools,
        service,
        discovery,
        server_count,
    }))
}

fn parse_server_configs(settings: &Settings, workspace: &Path) -> Result<Vec<ServerConfig>> {
    let Some(raw_servers) = settings.raw.get("mcpServers") else {
        return Ok(Vec::new());
    };
    let raw_servers = raw_servers
        .as_object()
        .context("mcpServers 必须是 JSON object")?;
    if raw_servers.len() > MAX_SERVERS {
        bail!("mcpServers 超过 {MAX_SERVERS} 个限制")
    }
    let mut namespaces = HashSet::new();
    let mut configs = Vec::new();
    for (name, value) in raw_servers {
        if name.is_empty() || name.len() > MAX_SERVER_NAME_BYTES {
            bail!("MCP server 名称长度无效: {name:?}")
        }
        let raw: RawServerConfig = serde_json::from_value(value.clone())
            .with_context(|| format!("MCP server {name} 配置无效"))?;
        if raw.disabled {
            continue;
        }
        validate_server_config(name, &raw)?;
        let namespace = namespace_component(name, 48);
        if !namespaces.insert(namespace.clone()) {
            bail!("MCP server 名称规范化后冲突: {name}")
        }
        let transport = match (raw.command, raw.url) {
            (Some(command), None) => {
                if raw
                    .transport_type
                    .as_deref()
                    .is_some_and(|kind| kind != "stdio")
                {
                    bail!("MCP server {name} command transport 必须使用 type=stdio")
                }
                if !raw.headers.is_empty() || raw.allow_private_network {
                    bail!("MCP server {name} 的 headers/allowPrivateNetwork 仅适用于 HTTP")
                }
                let cwd = match raw.cwd {
                    Some(value) => {
                        let path = PathBuf::from(value);
                        let path = if path.is_absolute() {
                            path
                        } else {
                            workspace.join(path)
                        };
                        std::fs::canonicalize(&path).with_context(|| {
                            format!("无法解析 MCP server {name} cwd: {}", path.display())
                        })?
                    }
                    None => std::fs::canonicalize(workspace).with_context(|| {
                        format!("无法解析 MCP workspace cwd: {}", workspace.display())
                    })?,
                };
                if !cwd.is_dir() {
                    bail!("MCP server {name} cwd 不是目录: {}", cwd.display())
                }
                ServerTransport::Stdio {
                    command,
                    args: raw.args,
                    env: raw.env,
                    cwd,
                }
            }
            (None, Some(url)) => {
                if raw
                    .transport_type
                    .as_deref()
                    .is_some_and(|kind| !matches!(kind, "http" | "streamable-http"))
                {
                    bail!("MCP server {name} URL transport type 无效")
                }
                if !raw.args.is_empty() || !raw.env.is_empty() || raw.cwd.is_some() {
                    bail!("MCP server {name} 的 args/env/cwd 仅适用于 stdio")
                }
                let url = Url::parse(&url).context("MCP HTTP URL 无效")?;
                if !matches!(url.scheme(), "http" | "https")
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.host_str().is_none()
                {
                    bail!("MCP HTTP URL 必须是无凭据的 http(s) URL")
                }
                let (headers, secrets) = parse_http_headers(raw.headers)?;
                ServerTransport::Http {
                    url,
                    headers,
                    secrets,
                    allow_private_network: raw.allow_private_network,
                }
            }
            _ => bail!("MCP server {name} 必须且只能配置 command 或 url 之一"),
        };
        let timeout_ms = raw
            .timeout_ms
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
            .clamp(MIN_REQUEST_TIMEOUT_MS, MAX_REQUEST_TIMEOUT_MS);
        configs.push(ServerConfig {
            name: name.clone(),
            namespace,
            transport,
            request_timeout: Duration::from_millis(timeout_ms),
        });
    }
    Ok(configs)
}

fn validate_server_config(name: &str, config: &RawServerConfig) -> Result<()> {
    if config.command.as_ref().is_some_and(|command| {
        command.trim().is_empty() || command.len() > MAX_COMMAND_BYTES || command.contains('\0')
    }) {
        bail!("MCP server {name} command 为空、过长或包含 NUL")
    }
    if config.args.len() > MAX_ARGS {
        bail!("MCP server {name} args 超过 {MAX_ARGS} 项限制")
    }
    for argument in &config.args {
        if argument.len() > MAX_ARG_BYTES || argument.contains('\0') {
            bail!("MCP server {name} argument 过长或包含 NUL")
        }
    }
    if config.env.len() > MAX_ENV_VARS {
        bail!("MCP server {name} env 超过 {MAX_ENV_VARS} 项限制")
    }
    for (key, value) in &config.env {
        let valid_key = !key.is_empty()
            && key.bytes().enumerate().all(|(index, byte)| {
                matches!(
                    (index, byte),
                    (0, b'A'..=b'Z' | b'a'..=b'z' | b'_')
                        | (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
                )
            });
        if !valid_key || value.len() > MAX_ENV_VALUE_BYTES || value.contains('\0') {
            bail!("MCP server {name} env entry 无效: {key:?}")
        }
    }
    if config
        .url
        .as_ref()
        .is_some_and(|url| url.is_empty() || url.len() > 16 * 1024)
    {
        bail!("MCP server {name} URL 为空或过长")
    }
    Ok(())
}

fn parse_http_headers(raw: BTreeMap<String, String>) -> Result<(HeaderMap, Vec<String>)> {
    if raw.len() > MAX_HTTP_HEADERS {
        bail!("MCP HTTP headers 超过 {MAX_HTTP_HEADERS} 项限制")
    }
    let mut headers = HeaderMap::new();
    let mut secrets = Vec::new();
    for (name, value) in raw {
        if value.len() > MAX_HTTP_HEADER_VALUE_BYTES {
            bail!("MCP HTTP header value 过长")
        }
        let name = HeaderName::from_bytes(name.as_bytes()).context("MCP HTTP header name 无效")?;
        if matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "connection"
                | "transfer-encoding"
                | "accept"
                | "content-type"
                | "mcp-session-id"
                | "mcp-protocol-version"
        ) {
            bail!("MCP HTTP 不允许覆盖 header {name}")
        }
        let value = HeaderValue::from_str(&value).context("MCP HTTP header value 无效")?;
        if !value.as_bytes().is_empty() {
            secrets.push(String::from_utf8_lossy(value.as_bytes()).into_owned());
        }
        headers.insert(name, value);
    }
    Ok((headers, secrets))
}

impl HttpMcpRpc {
    fn new(
        url: Url,
        headers: HeaderMap,
        secrets: Vec<String>,
        allow_private_network: bool,
        request_timeout: Duration,
    ) -> Self {
        let (events, _) = broadcast::channel(128);
        Self {
            url,
            headers,
            secrets,
            allow_private_network,
            request_timeout,
            next_id: AtomicU64::new(1),
            session_id: Arc::new(Mutex::new(None)),
            protocol_version: Arc::new(Mutex::new(CURRENT_PROTOCOL_VERSION.to_owned())),
            events,
            listener_started: AtomicBool::new(false),
            listener_task: Mutex::new(None),
            closing: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn send_message(
        &self,
        message: Value,
        expected_id: Option<u64>,
    ) -> Result<Option<Value>> {
        let body = serde_json::to_vec(&message)?;
        if body.len() > 4 * 1024 * 1024 {
            bail!("MCP HTTP request 超过 4 MiB 限制")
        }
        timeout(self.request_timeout, async {
            let client = secure_client_for_url(&self.url, self.allow_private_network).await?;
            let mut request = client
                .post(self.url.clone())
                .headers(self.headers.clone())
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header(
                    "mcp-protocol-version",
                    self.protocol_version.lock().await.as_str(),
                )
                .body(body);
            if let Some(session) = self.session_id.lock().await.as_ref() {
                request = request.header("mcp-session-id", session);
            }
            let response = request.send().await.context("MCP HTTP POST 失败")?;
            if let Some(session) = response
                .headers()
                .get("mcp-session-id")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty() && value.len() <= 4096)
            {
                *self.session_id.lock().await = Some(session.to_owned());
            }
            let status = response.status();
            if status.as_u16() == 202 {
                return if expected_id.is_none() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(
                        "MCP HTTP request 只返回了 202，无 JSON-RPC response"
                    ))
                };
            }
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();
            let response_body = read_http_body_limited(response, MAX_HTTP_RESPONSE_BYTES).await?;
            if !status.is_success() {
                let text = redact_secrets(&String::from_utf8_lossy(&response_body), &self.secrets);
                bail!(
                    "MCP HTTP {}: {}",
                    status.as_u16(),
                    truncate_text(&text, 4096)
                )
            }
            let messages = if content_type.contains("text/event-stream") {
                parse_http_sse_messages(&response_body)?
            } else {
                vec![
                    serde_json::from_slice(&response_body)
                        .context("MCP HTTP response 不是有效 JSON")?,
                ]
            };
            let mut matching = None;
            let mut server_requests = Vec::new();
            for mut value in messages {
                redact_json_secrets(&mut value, &self.secrets);
                if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                    bail!("MCP HTTP message 缺少 jsonrpc=2.0")
                }
                if value.get("method").is_some() {
                    let _ = self.events.send(value.clone());
                    if value.get("id").is_some() {
                        server_requests.push(value);
                    }
                    continue;
                }
                if expected_id.is_some_and(|id| value.get("id") == Some(&json!(id))) {
                    matching = Some(parse_rpc_result(&value)?);
                }
            }
            for request in server_requests {
                self.reject_server_request(&request).await;
            }
            if expected_id.is_some() && matching.is_none() {
                bail!("MCP HTTP response 中没有匹配的 JSON-RPC id")
            }
            Ok(matching)
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "MCP HTTP request 超过 {}ms timeout",
                self.request_timeout.as_millis()
            )
        })?
    }

    async fn reject_server_request(&self, request: &Value) {
        let Some(id) = request.get("id") else {
            return;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let response = if method == "ping" {
            json!({"jsonrpc": "2.0", "id": id, "result": {}})
        } else {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": "Client method not supported"}
            })
        };
        let Ok(body) = serde_json::to_vec(&response) else {
            return;
        };
        let Ok(client) = secure_client_for_url(&self.url, self.allow_private_network).await else {
            return;
        };
        let mut request = client
            .post(self.url.clone())
            .headers(self.headers.clone())
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header(
                "mcp-protocol-version",
                self.protocol_version.lock().await.as_str(),
            )
            .body(body);
        if let Some(session) = self.session_id.lock().await.as_ref() {
            request = request.header("mcp-session-id", session);
        }
        let _ = request.send().await;
    }
}

#[async_trait]
impl McpRpc for HttpMcpRpc {
    async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            bail!("MCP HTTP request id 已耗尽")
        }
        let mut message = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.send_message(message, Some(id))
            .await?
            .context("MCP HTTP request 没有 response")
    }

    async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        let mut message = json!({"jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        let _ = self.send_message(message, None).await?;
        Ok(())
    }

    fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    async fn set_protocol_version(&self, version: &str) {
        *self.protocol_version.lock().await = version.to_owned();
    }

    async fn start_notifications(&self) {
        if self.listener_started.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(session) = self.session_id.lock().await.clone() else {
            self.listener_started.store(false, Ordering::Release);
            return;
        };
        let task = tokio::spawn(http_notification_loop(
            HttpNotificationConfig {
                url: self.url.clone(),
                headers: self.headers.clone(),
                session,
                protocol_version: Arc::clone(&self.protocol_version),
                allow_private_network: self.allow_private_network,
                secrets: self.secrets.clone(),
            },
            self.events.clone(),
            Arc::clone(&self.closing),
        ));
        *self.listener_task.lock().await = Some(task);
    }

    async fn diagnostic_excerpt(&self) -> String {
        String::new()
    }

    async fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        if let Some(task) = self.listener_task.lock().await.take() {
            task.abort();
        }
        let Some(session) = self.session_id.lock().await.clone() else {
            return;
        };
        let Ok(client) = secure_client_for_url(&self.url, self.allow_private_network).await else {
            return;
        };
        let _ = timeout(
            Duration::from_secs(2),
            client
                .delete(self.url.clone())
                .headers(self.headers.clone())
                .header(
                    "mcp-protocol-version",
                    self.protocol_version.lock().await.as_str(),
                )
                .header("mcp-session-id", session)
                .send(),
        )
        .await;
    }
}

impl Drop for HttpMcpRpc {
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
        if let Some(task) = self.listener_task.get_mut().take() {
            task.abort();
        }
    }
}

async fn http_notification_loop(
    config: HttpNotificationConfig,
    events: broadcast::Sender<Value>,
    closing: Arc<AtomicBool>,
) {
    let HttpNotificationConfig {
        url,
        headers,
        session,
        protocol_version,
        allow_private_network,
        secrets,
    } = config;
    for attempt in 0..3u64 {
        if closing.load(Ordering::Acquire) {
            return;
        }
        let client = match secure_client_for_url(&url, allow_private_network).await {
            Ok(client) => client,
            Err(_) => return,
        };
        let response = client
            .get(url.clone())
            .headers(headers.clone())
            .header("accept", "text/event-stream")
            .header("mcp-session-id", &session)
            .header(
                "mcp-protocol-version",
                protocol_version.lock().await.as_str(),
            )
            .send()
            .await;
        let Ok(response) = response else {
            sleep(Duration::from_secs(1 << attempt)).await;
            continue;
        };
        if matches!(response.status().as_u16(), 404 | 405) {
            return;
        }
        if !response.status().is_success() {
            sleep(Duration::from_secs(1 << attempt)).await;
            continue;
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !content_type.contains("text/event-stream") {
            return;
        }
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut received = 0usize;
        while let Some(chunk) = stream.next().await {
            if closing.load(Ordering::Acquire) {
                return;
            }
            let Ok(chunk) = chunk else {
                break;
            };
            received = received.saturating_add(chunk.len());
            if received > 64 * 1024 * 1024 {
                return;
            }
            buffer.extend_from_slice(&chunk);
            while let Some((end, separator)) = sse_frame_end(&buffer) {
                let frame = buffer.drain(..end).collect::<Vec<_>>();
                buffer.drain(..separator);
                if let Ok(Some(mut value)) = parse_sse_frame(&frame) {
                    redact_json_secrets(&mut value, &secrets);
                    let valid_notification = value.get("jsonrpc").and_then(Value::as_str)
                        == Some("2.0")
                        && value.get("method").and_then(Value::as_str).is_some()
                        && value.get("id").is_none();
                    if valid_notification {
                        let _ = events.send(value);
                    }
                }
            }
            if buffer.len() > 1024 * 1024 {
                return;
            }
        }
        sleep(Duration::from_secs(1 << attempt)).await;
    }
}

fn sse_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
        return Some((index, 4));
    }
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| (index, 2))
}

fn parse_sse_frame(frame: &[u8]) -> Result<Option<Value>> {
    let text = std::str::from_utf8(frame).context("MCP notification SSE 不是 UTF-8")?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
        .collect::<Vec<_>>();
    if data.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&data.join("\n"))
        .map(Some)
        .context("MCP notification SSE data 不是有效 JSON")
}

async fn read_http_body_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        bail!("MCP HTTP response 超过 {limit} 字节限制")
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取 MCP HTTP response 失败")?;
        if body.len().saturating_add(chunk.len()) > limit {
            bail!("MCP HTTP response 超过 {limit} 字节限制")
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn parse_http_sse_messages(body: &[u8]) -> Result<Vec<Value>> {
    let text = std::str::from_utf8(body).context("MCP HTTP SSE 不是 UTF-8")?;
    let normalized = text.replace("\r\n", "\n");
    normalized
        .split("\n\n")
        .filter_map(|frame| {
            let data = frame
                .lines()
                .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
                .collect::<Vec<_>>();
            (!data.is_empty()).then(|| data.join("\n"))
        })
        .map(|data| serde_json::from_str(&data).context("MCP HTTP SSE data 不是有效 JSON"))
        .collect()
}

fn parse_rpc_result(value: &Value) -> Result<Value> {
    match (value.get("result"), value.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown RPC error");
            bail!("MCP HTTP RPC error {code}: {message}")
        }
        _ => bail!("MCP HTTP response 必须且只能包含 result 或 error"),
    }
}

fn redact_secrets(value: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|secret| !secret.is_empty())
        .fold(value.to_owned(), |text, secret| {
            text.replace(secret, "[REDACTED]")
        })
}

fn redact_json_secrets(value: &mut Value, secrets: &[String]) {
    match value {
        Value::String(text) => *text = redact_secrets(text, secrets),
        Value::Array(values) => {
            for value in values {
                redact_json_secrets(value, secrets);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                redact_json_secrets(value, secrets);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

impl McpClient {
    async fn connect(config: ServerConfig) -> Result<Arc<Self>> {
        let rpc: Arc<dyn McpRpc> = match &config.transport {
            ServerTransport::Stdio {
                command,
                args,
                env,
                cwd,
            } => Arc::new(
                StdioRpcClient::spawn(StdioRpcConfig {
                    label: format!("MCP/{}", config.name),
                    command: command.clone(),
                    args: args.clone(),
                    env: env.clone(),
                    cwd: cwd.clone(),
                    framing: RpcFraming::Newline,
                    request_timeout: config.request_timeout,
                    server_request_handler: None,
                })
                .await?,
            ),
            ServerTransport::Http {
                url,
                headers,
                secrets,
                allow_private_network,
            } => Arc::new(HttpMcpRpc::new(
                url.clone(),
                headers.clone(),
                secrets.clone(),
                *allow_private_network,
                config.request_timeout,
            )),
        };
        let initialize = match rpc
            .request(
                "initialize",
                Some(json!({
                    "protocolVersion": CURRENT_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {
                        "name": "open-agent-harness",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                })),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                let stderr = rpc.diagnostic_excerpt().await;
                rpc.shutdown().await;
                if stderr.trim().is_empty() {
                    return Err(error)
                        .with_context(|| format!("MCP server {} initialize 失败", config.name));
                }
                return Err(error).with_context(|| {
                    format!(
                        "MCP server {} initialize 失败; stderr: {}",
                        config.name,
                        truncate_text(&stderr, 2048)
                    )
                });
            }
        };
        let version = initialize
            .get("protocolVersion")
            .and_then(Value::as_str)
            .context("MCP initialize response 缺少 protocolVersion")?;
        if !SUPPORTED_PROTOCOL_VERSIONS.contains(&version) {
            rpc.shutdown().await;
            bail!("MCP server {} 返回不支持的协议版本 {version}", config.name)
        }
        rpc.set_protocol_version(version).await;
        let capabilities = initialize
            .get("capabilities")
            .and_then(Value::as_object)
            .context("MCP initialize response 缺少 capabilities object")?;
        let supports_tools = capabilities.contains_key("tools");
        let supports_resources = capabilities.contains_key("resources");
        let supports_prompts = capabilities.contains_key("prompts");
        rpc.notify("notifications/initialized", None).await?;

        let client = Arc::new(Self {
            name: config.name,
            namespace: config.namespace,
            rpc,
            supports_tools,
            supports_resources,
            supports_prompts,
            tools_changed: Arc::new(AtomicBool::new(false)),
            resources_changed: Arc::new(AtomicBool::new(false)),
            event_task: Mutex::new(None),
        });
        let mut events = client.rpc.subscribe();
        let weak = Arc::downgrade(&client);
        let task = tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(event) => event,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if let Some(client) = weak.upgrade() {
                            client.tools_changed.store(true, Ordering::Release);
                            client.resources_changed.store(true, Ordering::Release);
                            continue;
                        }
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                let Some(client) = weak.upgrade() else {
                    return;
                };
                match event.get("method").and_then(Value::as_str) {
                    Some("notifications/tools/list_changed") => {
                        client.tools_changed.store(true, Ordering::Release);
                    }
                    Some("notifications/resources/list_changed")
                    | Some("notifications/resources/updated") => {
                        client.resources_changed.store(true, Ordering::Release);
                    }
                    _ => {}
                }
            }
        });
        *client.event_task.lock().await = Some(task);
        client.rpc.start_notifications().await;
        Ok(client)
    }

    async fn list_tools(&self) -> Result<Vec<Arc<dyn Tool>>> {
        if !self.supports_tools {
            return Ok(Vec::new());
        }
        let values = self
            .list_paginated("tools/list", "tools", MAX_TOOLS_PER_SERVER)
            .await?;
        let mut names = HashSet::new();
        let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
        let handle = Arc::new(McpClientHandle {
            name: self.name.clone(),
            rpc: Arc::clone(&self.rpc),
        });
        for value in values {
            let object = value
                .as_object()
                .context("MCP tool definition 必须是 object")?;
            let original_name = object
                .get("name")
                .and_then(Value::as_str)
                .context("MCP tool definition 缺少 name")?;
            if original_name.is_empty() || original_name.len() > 1024 {
                bail!("MCP tool name 为空或超过 1024 字节限制")
            }
            let component = namespace_component(original_name, 64);
            let public_name = format!("mcp__{}__{component}", self.namespace);
            if !names.insert(public_name.clone()) {
                bail!("MCP server {} 的工具名称规范化后冲突", self.name)
            }
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("No description supplied by the configured server");
            let description = format!(
                "External tool from user-configured MCP server {}. {}",
                self.name,
                sanitize_text(description, MAX_DESCRIPTION_BYTES)
            );
            let input_schema = object
                .get("inputSchema")
                .cloned()
                .unwrap_or_else(|| object_schema(json!({}), &[]));
            if !input_schema.is_object() {
                bail!("MCP tool {original_name} inputSchema 必须是 object")
            }
            if serde_json::to_vec(&input_schema)?.len() > MAX_TOOL_SCHEMA_BYTES {
                bail!("MCP tool {original_name} inputSchema 超过 {MAX_TOOL_SCHEMA_BYTES} 字节限制")
            }
            let validator = jsonschema::validator_for(&input_schema)
                .with_context(|| format!("MCP tool {original_name} inputSchema 无效"))?;
            tools.push(Arc::new(McpTool {
                public_name,
                original_name: original_name.to_owned(),
                description,
                input_schema,
                validator,
                client: Arc::clone(&handle),
            }));
        }
        Ok(tools)
    }

    async fn list_paginated(
        &self,
        method: &str,
        field: &str,
        maximum: usize,
    ) -> Result<Vec<Value>> {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut collected = Vec::new();
        for _ in 0..MAX_LIST_PAGES {
            let params = cursor.as_ref().map(|cursor| json!({"cursor": cursor}));
            let result = self.rpc.request(method, params).await?;
            let values = result
                .get(field)
                .and_then(Value::as_array)
                .with_context(|| format!("{method} response 缺少 {field} array"))?;
            if collected.len().saturating_add(values.len()) > maximum {
                bail!("MCP server {} 的 {field} 超过 {maximum} 项限制", self.name)
            }
            collected.extend(values.iter().cloned());
            let next = result
                .get("nextCursor")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let Some(next) = next else {
                return Ok(collected);
            };
            if !seen_cursors.insert(next.clone()) {
                bail!("MCP server {} 返回重复 cursor", self.name)
            }
            cursor = Some(next);
        }
        bail!(
            "MCP server {} pagination 超过 {MAX_LIST_PAGES} 页限制",
            self.name
        )
    }

    async fn shutdown(&self) {
        if let Some(task) = self.event_task.lock().await.take() {
            task.abort();
        }
        self.rpc.shutdown().await;
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        if let Some(task) = self.event_task.get_mut().take() {
            task.abort();
        }
    }
}

struct McpClientHandle {
    name: String,
    rpc: Arc<dyn McpRpc>,
}

impl McpClientHandle {
    async fn call_tool(&self, name: &str, arguments: Value) -> Result<ToolOutput> {
        let result = self
            .rpc
            .request(
                "tools/call",
                Some(json!({"name": name, "arguments": arguments})),
            )
            .await?;
        let is_error = result
            .get("isError")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let rendered = serde_json::to_string_pretty(&remove_reserved_metadata(result))?;
        Ok(if is_error {
            ToolOutput::error(rendered)
        } else {
            ToolOutput::success(rendered)
        })
    }
}

struct McpTool {
    public_name: String,
    original_name: String,
    description: String,
    input_schema: Value,
    validator: jsonschema::Validator,
    client: Arc<McpClientHandle>,
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.public_name
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> Value {
        self.input_schema.clone()
    }

    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        self.validator
            .validate(input)
            .map_err(|error| format!("{}: {}", error.instance_path(), error))
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, _: &Value) -> bool {
        true
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, _: &Value) -> String {
        format!("{}/{}", self.client.name, self.original_name)
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        self.client.call_tool(&self.original_name, input).await
    }
}

impl McpManager {
    async fn discover_initial_tools(&self) -> Result<Vec<Arc<dyn Tool>>> {
        let mut attempts = stream::iter(self.clients.iter().cloned().enumerate())
            .map(|(index, client)| async move {
                let result = client.list_tools().await;
                (index, client, result)
            })
            .buffer_unordered(MAX_CONCURRENT_SERVER_STARTS)
            .collect::<Vec<_>>()
            .await;
        attempts.sort_by_key(|(index, _, _)| *index);
        let mut discovered = Vec::new();
        let mut known = self.known_tools.lock().await;
        for (_, client, result) in attempts {
            match result {
                Ok(tools) => {
                    known.insert(
                        client.name.clone(),
                        tools.iter().map(|tool| tool.name().to_owned()).collect(),
                    );
                    discovered.extend(tools);
                }
                Err(error) if !self.strict => {
                    eprintln!("MCP tools skipped for {}: {error:#}", client.name);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(discovered)
    }

    fn client(&self, name: &str) -> Result<Arc<McpClient>> {
        self.clients
            .iter()
            .find(|client| client.name == name || client.namespace == name)
            .cloned()
            .with_context(|| {
                format!(
                    "MCP server {name:?} 不存在；可用: {}",
                    self.clients
                        .iter()
                        .map(|client| client.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
    }

    fn resource_clients(&self, server: Option<&str>) -> Result<Vec<Arc<McpClient>>> {
        match server {
            Some(name) => {
                let client = self.client(name)?;
                if !client.supports_resources {
                    bail!("MCP server {} 未声明 resources capability", client.name)
                }
                Ok(vec![client])
            }
            None => Ok(self
                .clients
                .iter()
                .filter(|client| client.supports_resources)
                .cloned()
                .collect()),
        }
    }

    async fn list_resources(&self, server: Option<&str>, templates: bool) -> Result<Value> {
        let mut output = Vec::new();
        for client in self.resource_clients(server)? {
            let (method, field) = if templates {
                ("resources/templates/list", "resourceTemplates")
            } else {
                ("resources/list", "resources")
            };
            let values = client.list_paginated(method, field, MAX_RESOURCES).await?;
            for value in values {
                let mut value = remove_reserved_metadata(value);
                value
                    .as_object_mut()
                    .with_context(|| format!("{method} item 必须是 object"))?
                    .insert("server".into(), Value::String(client.name.clone()));
                output.push(value);
            }
            client.resources_changed.store(false, Ordering::Release);
        }
        Ok(Value::Array(output))
    }

    async fn read_resource(&self, server: &str, uri: &str) -> Result<Value> {
        if uri.is_empty() || uri.len() > MAX_RESOURCE_URI_BYTES {
            bail!("resource URI 为空或超过 {MAX_RESOURCE_URI_BYTES} 字节限制")
        }
        let client = self.client(server)?;
        if !client.supports_resources {
            bail!("MCP server {} 未声明 resources capability", client.name)
        }
        let value = client
            .rpc
            .request("resources/read", Some(json!({"uri": uri})))
            .await?;
        Ok(remove_reserved_metadata(value))
    }

    async fn list_prompts(&self, server: Option<&str>) -> Result<Value> {
        let clients = match server {
            Some(name) => {
                let client = self.client(name)?;
                if !client.supports_prompts {
                    bail!("MCP server {} 未声明 prompts capability", client.name)
                }
                vec![client]
            }
            None => self
                .clients
                .iter()
                .filter(|client| client.supports_prompts)
                .cloned()
                .collect(),
        };
        let mut output = Vec::new();
        for client in clients {
            let prompts = client
                .list_paginated("prompts/list", "prompts", MAX_PROMPTS_PER_SERVER)
                .await?;
            for prompt in prompts {
                let mut prompt = remove_reserved_metadata(prompt);
                prompt
                    .as_object_mut()
                    .context("prompts/list item 必须是 object")?
                    .insert("server".into(), Value::String(client.name.clone()));
                output.push(prompt);
            }
        }
        Ok(Value::Array(output))
    }

    async fn get_prompt(
        &self,
        server: &str,
        name: &str,
        arguments: Option<Value>,
    ) -> Result<Value> {
        if name.is_empty() || name.len() > 256 {
            bail!("MCP prompt name 为空或过长")
        }
        let client = self.client(server)?;
        if !client.supports_prompts {
            bail!("MCP server {} 未声明 prompts capability", client.name)
        }
        let mut params = json!({"name": name});
        if let Some(arguments) = arguments {
            if !arguments.is_object() {
                bail!("MCP prompt arguments 必须是 object")
            }
            params["arguments"] = arguments;
        }
        client
            .rpc
            .request("prompts/get", Some(params))
            .await
            .map(remove_reserved_metadata)
    }
}

#[async_trait]
impl ToolService for McpManager {
    async fn shutdown(&self) {
        for client in &self.clients {
            client.shutdown().await;
        }
    }
}

#[async_trait]
impl ToolDiscovery for McpManager {
    async fn refresh(&self) -> Result<ToolRefresh> {
        let changed = self
            .clients
            .iter()
            .filter(|client| client.tools_changed.load(Ordering::Acquire))
            .cloned()
            .collect::<Vec<_>>();
        if changed.is_empty() {
            return Ok(ToolRefresh {
                upsert: Vec::new(),
                remove: Vec::new(),
            });
        }
        let mut known = self.known_tools.lock().await;
        let mut upsert = Vec::new();
        let mut remove = Vec::new();
        for client in changed {
            match client.list_tools().await {
                Ok(tools) => {
                    let names = tools
                        .iter()
                        .map(|tool| tool.name().to_owned())
                        .collect::<HashSet<_>>();
                    if let Some(previous) = known.insert(client.name.clone(), names.clone()) {
                        remove.extend(previous.difference(&names).cloned());
                    }
                    upsert.extend(tools);
                    client.tools_changed.store(false, Ordering::Release);
                }
                Err(error) if !self.strict => {
                    if self.debug {
                        eprintln!("MCP tool refresh failed for {}: {error:#}", client.name);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(ToolRefresh { upsert, remove })
    }
}

struct ListMcpResourcesTool {
    manager: Arc<McpManager>,
}

struct ListMcpResourceTemplatesTool {
    manager: Arc<McpManager>,
}

struct ReadMcpResourceTool {
    manager: Arc<McpManager>,
}

struct ListMcpPromptsTool {
    manager: Arc<McpManager>,
}

struct GetMcpPromptTool {
    manager: Arc<McpManager>,
}

fn list_resource_schema() -> Value {
    object_schema(
        json!({"server": {"type": "string", "maxLength": MAX_SERVER_NAME_BYTES}}),
        &[],
    )
}

#[async_trait]
impl Tool for ListMcpResourcesTool {
    fn name(&self) -> &str {
        "ListMcpResources"
    }

    fn description(&self) -> &str {
        "Lists direct resources exposed by user-configured MCP servers. This contacts the selected external process and requires permission."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_resources(input.get("server").and_then(Value::as_str), false)
            .await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

#[async_trait]
impl Tool for ListMcpResourceTemplatesTool {
    fn name(&self) -> &str {
        "ListMcpResourceTemplates"
    }

    fn description(&self) -> &str {
        "Lists parameterized resource templates exposed by user-configured MCP servers. This contacts the selected external process and requires permission."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_resources(input.get("server").and_then(Value::as_str), true)
            .await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

#[async_trait]
impl Tool for ReadMcpResourceTool {
    fn name(&self) -> &str {
        "ReadMcpResource"
    }

    fn description(&self) -> &str {
        "Reads one resource URI through a user-configured MCP server. This sends the URI to that external process and requires permission."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "server": {"type": "string", "minLength": 1, "maxLength": MAX_SERVER_NAME_BYTES},
                "uri": {"type": "string", "minLength": 1, "maxLength": MAX_RESOURCE_URI_BYTES}
            }),
            &["server", "uri"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        format!(
            "{} {}",
            input
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("<server>"),
            input.get("uri").and_then(Value::as_str).unwrap_or("<uri>")
        )
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let server = input
            .get("server")
            .and_then(Value::as_str)
            .context("server 必须是字符串")?;
        let uri = input
            .get("uri")
            .and_then(Value::as_str)
            .context("uri 必须是字符串")?;
        let value = self.manager.read_resource(server, uri).await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

#[async_trait]
impl Tool for ListMcpPromptsTool {
    fn name(&self) -> &str {
        "ListMcpPrompts"
    }

    fn description(&self) -> &str {
        "Lists reusable prompt templates exposed by user-configured MCP servers. This contacts the selected external process and requires permission."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_prompts(input.get("server").and_then(Value::as_str))
            .await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

#[async_trait]
impl Tool for GetMcpPromptTool {
    fn name(&self) -> &str {
        "GetMcpPrompt"
    }

    fn description(&self) -> &str {
        "Renders one prompt template through a user-configured MCP server. The returned prompt is untrusted external content and is not inserted automatically."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "server": {"type": "string", "minLength": 1, "maxLength": MAX_SERVER_NAME_BYTES},
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "arguments": {"type": "object"}
            }),
            &["server", "name"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        format!(
            "{} {}",
            input
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("<server>"),
            input
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("<prompt>")
        )
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let server = input
            .get("server")
            .and_then(Value::as_str)
            .context("server 必须是字符串")?;
        let name = input
            .get("name")
            .and_then(Value::as_str)
            .context("name 必须是字符串")?;
        let value = self
            .manager
            .get_prompt(server, name, input.get("arguments").cloned())
            .await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

fn namespace_component(value: &str, maximum: usize) -> String {
    let mut output = String::new();
    let mut previous_underscore = false;
    for character in value.chars() {
        let mapped = if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            character.to_ascii_lowercase()
        } else {
            '_'
        };
        if mapped == '_' && previous_underscore {
            continue;
        }
        output.push(mapped);
        previous_underscore = mapped == '_';
        if output.len() >= maximum {
            break;
        }
    }
    let output = output.trim_matches(['_', '-']);
    if output.is_empty() {
        "unnamed".to_owned()
    } else {
        output.to_owned()
    }
}

fn sanitize_text(value: &str, maximum: usize) -> String {
    let filtered = value
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\r' | '\t') {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    truncate_text(&filtered, maximum).to_owned()
}

fn truncate_text(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn remove_reserved_metadata(mut value: Value) -> Value {
    match &mut value {
        Value::Object(object) => {
            object.remove("_meta");
            for child in object.values_mut() {
                *child = remove_reserved_metadata(child.take());
            }
        }
        Value::Array(values) => {
            for child in values {
                *child = remove_reserved_metadata(child.take());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    value
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::{ToolContext, ToolRegistry},
    };

    #[test]
    fn trusted_server_settings_are_bounded_and_normalized() {
        let temp = tempfile::tempdir().unwrap();
        let settings = Settings {
            raw: json!({
                "mcpServers": {
                    "Local Server": {"command": "server", "args": ["--stdio"]}
                }
            }),
        };
        let configs = parse_server_configs(&settings, temp.path()).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].namespace, "local_server");
    }

    #[test]
    fn reserved_metadata_is_removed_recursively() {
        let cleaned = remove_reserved_metadata(json!({
            "_meta": {"secret": true},
            "content": [{"type": "text", "text": "ok", "_meta": {"x": 1}}]
        }));
        assert!(cleaned.get("_meta").is_none());
        assert!(cleaned["content"][0].get("_meta").is_none());
        assert_eq!(cleaned["content"][0]["text"], "ok");
    }

    #[test]
    fn configured_http_secrets_are_removed_from_nested_responses() {
        let mut response = json!({
            "error": {"message": "token=private-header-value"},
            "content": [{"text": "private-header-value"}]
        });
        redact_json_secrets(&mut response, &["private-header-value".to_owned()]);
        let rendered = response.to_string();
        assert!(!rendered.contains("private-header-value"));
        assert!(rendered.contains("[REDACTED]"));
    }

    #[test]
    fn streamable_http_notification_frames_are_parsed_without_event_metadata() {
        let parsed = parse_sse_frame(
            b"id: 7\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}",
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed["method"], "notifications/tools/list_changed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_tools_and_resources_join_the_registry() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("mock-mcp.sh");
        std::fs::write(
            &script,
            r#"tool_lists=0
while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{},"resources":{},"prompts":{}},"serverInfo":{"name":"mock","version":"1"}}}' ;;
  *'"method":"tools/list"'*)
    tool_lists=$((tool_lists + 1))
    if [ "$tool_lists" -eq 1 ]; then
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo input","inputSchema":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}}]}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}'
    else
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"dynamic","description":"Dynamic input","inputSchema":{"type":"object","additionalProperties":false}}]}}'
    fi ;;
  *'"method":"tools/call"'*) printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"called-dynamic"}],"isError":false}}' ;;
  *'"method":"resources/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":5,"result":{"resources":[{"uri":"mock://one","name":"one"}]}}' ;;
  *'"method":"resources/read"'*) printf '%s\n' '{"jsonrpc":"2.0","id":6,"result":{"contents":[{"uri":"mock://one","text":"resource-body"}]}}' ;;
  *'"method":"resources/templates/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"resourceTemplates":[{"uriTemplate":"mock://{name}","name":"by-name"}]}}' ;;
  *'"method":"prompts/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":8,"result":{"prompts":[{"name":"review","description":"Review input"}]}}' ;;
  *'"method":"prompts/get"'*) printf '%s\n' '{"jsonrpc":"2.0","id":9,"result":{"description":"Rendered","messages":[{"role":"user","content":{"type":"text","text":"review this"}}]}}' ;;
esac
done
"#,
        )
        .unwrap();
        let settings = Settings {
            raw: json!({
                "strictMcpConfig": true,
                "mcpServers": {
                    "mock": {"command": "/bin/sh", "args": [script]}
                }
            }),
        };
        let integration = connect_mcp(&settings, temp.path(), false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(integration.server_count, 1);
        let registry = ToolRegistry::with_integrations(
            integration.active_tools,
            integration.deferred_tools,
            vec![integration.service],
            vec![integration.discovery],
        )
        .unwrap();
        assert!(
            registry
                .definitions()
                .iter()
                .any(|tool| tool["name"] == "ToolSearch")
        );
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        let mut selected = ToolOutput::error("dynamic MCP tool did not refresh");
        for _ in 0..20 {
            selected = registry
                .execute(
                    &context,
                    "ToolSearch",
                    json!({"query": "select:mcp__mock__dynamic"}),
                )
                .await;
            if selected.content.contains("mcp__mock__dynamic")
                && selected.content.contains("\"loaded\"")
                && !selected
                    .content
                    .contains("\"missing\": [\n    \"mcp__mock__dynamic\"")
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
        assert!(!selected.is_error, "{}", selected.content);
        assert!(selected.content.contains("mcp__mock__dynamic"));
        let called = registry
            .execute(&context, "mcp__mock__dynamic", json!({}))
            .await;
        assert!(!called.is_error, "{}", called.content);
        assert!(called.content.contains("called-dynamic"));
        let resources = registry
            .execute(&context, "ListMcpResources", json!({"server": "mock"}))
            .await;
        assert!(!resources.is_error, "{}", resources.content);
        assert!(resources.content.contains("mock://one"));
        let resource = registry
            .execute(
                &context,
                "ReadMcpResource",
                json!({"server":"mock","uri":"mock://one"}),
            )
            .await;
        assert!(!resource.is_error, "{}", resource.content);
        assert!(resource.content.contains("resource-body"));
        let templates = registry
            .execute(
                &context,
                "ListMcpResourceTemplates",
                json!({"server":"mock"}),
            )
            .await;
        assert!(!templates.is_error, "{}", templates.content);
        assert!(templates.content.contains("uriTemplate"));
        let prompts = registry
            .execute(&context, "ListMcpPrompts", json!({"server":"mock"}))
            .await;
        assert!(!prompts.is_error, "{}", prompts.content);
        assert!(prompts.content.contains("review"));
        let prompt = registry
            .execute(
                &context,
                "GetMcpPrompt",
                json!({"server":"mock","name":"review"}),
            )
            .await;
        assert!(!prompt.is_error, "{}", prompt.content);
        assert!(prompt.content.contains("review this"));
        registry.shutdown().await;
    }

    #[tokio::test]
    async fn streamable_http_server_negotiates_session_and_calls_tool() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut saw_get = false;
            let mut saw_delete = false;
            let mut post_step = 0usize;
            while !saw_delete || !saw_get {
                let (mut stream, _) = listener.accept().unwrap();
                let (request_line, body) = read_http_request(&mut stream);
                if request_line.starts_with("GET ") {
                    saw_get = true;
                    write!(stream, "HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").unwrap();
                    continue;
                }
                if request_line.starts_with("DELETE ") {
                    saw_delete = true;
                    write!(
                        stream,
                        "HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    )
                    .unwrap();
                    continue;
                }
                assert!(request_line.starts_with("POST "));
                match post_step {
                    0 => {
                        assert!(body.to_string().contains("initialize"));
                        write_json_response(
                            &mut stream,
                            200,
                            Some("test-session"),
                            &json!({"jsonrpc":"2.0","id":1,"result":{
                                "protocolVersion":"2025-11-25",
                                "capabilities":{"tools":{}},
                                "serverInfo":{"name":"http-mock","version":"1"}
                            }}),
                        );
                    }
                    1 => {
                        assert!(body.to_string().contains("notifications/initialized"));
                        write!(stream, "HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").unwrap();
                    }
                    2 => {
                        assert!(body.to_string().contains("tools/list"));
                        write_json_response(
                            &mut stream,
                            200,
                            None,
                            &json!({"jsonrpc":"2.0","id":2,"result":{"tools":[{
                                "name":"echo","description":"Echo","inputSchema":{"type":"object"}
                            }]}}),
                        );
                    }
                    3 => {
                        assert!(body.to_string().contains("tools/call"));
                        write_json_response(
                            &mut stream,
                            200,
                            None,
                            &json!({"jsonrpc":"2.0","id":3,"result":{
                                "content":[{"type":"text","text":"http-called"}],"isError":false
                            }}),
                        );
                    }
                    _ => unreachable!(),
                }
                post_step += 1;
            }
            assert_eq!(post_step, 4);
        });
        let temp = tempfile::tempdir().unwrap();
        let settings = Settings {
            raw: json!({
                "strictMcpConfig": true,
                "mcpServers": {"http": {
                    "type": "streamable-http",
                    "url": format!("http://{address}/mcp"),
                    "allowPrivateNetwork": true
                }}
            }),
        };
        let integration = connect_mcp(&settings, temp.path(), false)
            .await
            .unwrap()
            .unwrap();
        let registry = ToolRegistry::with_integrations(
            integration.active_tools,
            integration.deferred_tools,
            vec![integration.service],
            vec![integration.discovery],
        )
        .unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query": "select:mcp__http__echo"}),
            )
            .await;
        let output = registry
            .execute(&context, "mcp__http__echo", json!({}))
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("http-called"));
        registry.shutdown().await;
        server.join().unwrap();
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> (String, Value) {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0);
            buffer.extend_from_slice(&chunk[..count]);
            if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]);
        let request_line = headers.lines().next().unwrap().to_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        while buffer.len() < header_end + content_length {
            let count = stream.read(&mut chunk).unwrap();
            assert!(count > 0);
            buffer.extend_from_slice(&chunk[..count]);
        }
        let body = &buffer[header_end..header_end + content_length];
        let body = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(body).unwrap()
        };
        (request_line, body)
    }

    fn write_json_response(
        stream: &mut std::net::TcpStream,
        status: u16,
        session: Option<&str>,
        value: &Value,
    ) {
        let body = value.to_string();
        let session = session.map_or_else(String::new, |session| {
            format!("mcp-session-id: {session}\r\n")
        });
        write!(
            stream,
            "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\n{session}content-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    }
}
