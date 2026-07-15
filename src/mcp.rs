use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{self, IsTerminal, Read as _, Write},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, RwLock as StdRwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::io::BufRead;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    terminal,
};
use futures_util::{StreamExt, stream};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    sync::{Mutex, broadcast, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};
use url::Url;

use crate::{
    config::Settings,
    mcp_oauth::{OAuthCredentialProvider, RawOAuthConfig},
    mcp_websocket::{WebSocketMcpConfig, WebSocketMcpRpc},
    process::{SecretEnvScrubber, resolve_trusted_executable, spawn_managed},
    rpc::{RpcFraming, RpcServerRequestHandler, StdioRpcClient, StdioRpcConfig},
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
const MAX_RESOURCE_HANDLES: usize = 4096;
const MAX_RESOURCE_HANDLE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RESOURCE_TEMPLATE_VARIABLES: usize = 64;
const MAX_RESOURCE_TEMPLATE_ARGUMENT_BYTES: usize = 16 * 1024;
const MAX_ROOTS: usize = 32;
const MAX_ROOT_PATH_BYTES: usize = 16 * 1024;
const MAX_DESCRIPTION_BYTES: usize = 8 * 1024;
const MAX_TOOL_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_HTTP_HEADER_VALUE_BYTES: usize = 16 * 1024;
const MAX_HTTP_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOOL_CONTENT_BLOCKS: usize = 256;
const MAX_TOOL_PREVIEW_BYTES: usize = 128 * 1024;
const MAX_TOOL_MEDIA_RAW_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOOL_MEDIA_BASE64_BYTES: usize = (MAX_TOOL_MEDIA_RAW_BYTES / 3) * 4 + 4;
const MAX_CONCURRENT_SERVER_STARTS: usize = 4;
const DEFAULT_ELICITATION_TIMEOUT_MS: u64 = 90_000;
const MIN_ELICITATION_TIMEOUT_MS: u64 = 1_000;
const MAX_ELICITATION_TIMEOUT_MS: u64 = 120_000;
const MAX_ELICITATION_REQUEST_BYTES: usize = 256 * 1024;
const MAX_ELICITATION_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ELICITATION_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_ELICITATION_FIELDS: usize = 64;
const MAX_AUTH_TOKEN_BYTES: usize = 64 * 1024;
const DEFAULT_AUTH_COMMAND_TIMEOUT_MS: u64 = 10_000;
const MIN_AUTH_COMMAND_TIMEOUT_MS: u64 = 1_000;
const MAX_AUTH_COMMAND_TIMEOUT_MS: u64 = 60_000;
const MAX_LEGACY_SSE_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_LEGACY_SSE_STREAM_BYTES: usize = 64 * 1024 * 1024;
const MAX_LEGACY_PENDING_REQUESTS: usize = 64;
const WAIT_FOR_MCP_SERVERS_TIMEOUT: Duration = Duration::from_secs(5);

pub struct McpIntegration {
    pub active_tools: Vec<Arc<dyn Tool>>,
    pub deferred_tools: Vec<Arc<dyn Tool>>,
    pub service: Arc<dyn ToolService>,
    pub discovery: Arc<dyn ToolDiscovery>,
    pub hook_invoker: Arc<dyn McpHookInvoker>,
    pub control: Arc<dyn McpControl>,
    pub server_count: usize,
}

#[async_trait]
pub trait McpControl: Send + Sync {
    fn status(&self) -> Vec<McpServerStatus>;
    async fn reconnect(&self, server: &str) -> Result<()>;
    async fn list_prompts(&self, context: &ToolContext) -> Result<Value>;
    async fn get_prompt(
        &self,
        context: &ToolContext,
        server: &str,
        name: &str,
        arguments: Option<Value>,
    ) -> Result<Value>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpServerStatus {
    pub name: String,
    pub status: McpServerStatusKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpServerStatusKind {
    Connected,
    Pending,
    Failed,
    NeedsAuth,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct McpHookCall {
    pub server: String,
    pub tool: String,
    pub input: Value,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHookResult {
    pub output: String,
    pub is_error: bool,
}

/// The narrow MCP surface exposed to declarative hooks. Implementations must
/// resolve calls only against already-connected, trusted `mcpServers` entries.
#[async_trait]
pub trait McpHookInvoker: Send + Sync {
    async fn invoke(&self, call: McpHookCall) -> Result<McpHookResult>;
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
    #[serde(default)]
    roots: Vec<String>,
    auth: Option<RawAuthConfig>,
    #[serde(rename = "elicitationTimeoutMs")]
    elicitation_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum RawAuthConfig {
    #[serde(rename = "bearer-env")]
    Env { env: String },
    #[serde(rename = "bearer-file")]
    File { path: String },
    #[serde(rename = "bearer-command")]
    Command {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
    },
    #[serde(rename = "oauth")]
    OAuth {
        #[serde(flatten)]
        config: RawOAuthConfig,
    },
}

#[derive(Debug, Clone)]
struct ServerConfig {
    name: String,
    namespace: String,
    transport: ServerTransport,
    request_timeout: Duration,
    elicitation_timeout: Duration,
    roots: Vec<McpRoot>,
    secret_env_scrubber: SecretEnvScrubber,
}

#[derive(Debug, Clone)]
struct McpRoot {
    uri: String,
    name: String,
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
        headers: Box<HeaderMap>,
        secrets: Vec<String>,
        allow_private_network: bool,
        credential: Option<TokenCredentialProvider>,
        legacy_sse: bool,
    },
    WebSocket {
        url: Url,
        headers: Box<HeaderMap>,
        secrets: Vec<String>,
        allow_private_network: bool,
        credential: Option<TokenCredentialProvider>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum TokenCredentialProvider {
    Env {
        name: String,
    },
    File {
        path: PathBuf,
    },
    Command {
        command: PathBuf,
        args: Vec<String>,
        cwd: PathBuf,
        timeout: Duration,
        secret_env_scrubber: SecretEnvScrubber,
    },
    OAuth(OAuthCredentialProvider),
}

#[derive(Default)]
struct ElicitationBridge {
    active: StdRwLock<Option<ToolContext>>,
    timeout: Duration,
}

struct ElicitationScope<'a> {
    bridge: &'a ElicitationBridge,
}

impl Drop for ElicitationScope<'_> {
    fn drop(&mut self) {
        *self
            .bridge
            .active
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

#[derive(Clone)]
struct McpClientRequestHandler {
    server_name: String,
    roots: Vec<McpRoot>,
    elicitation: Arc<ElicitationBridge>,
}

#[async_trait]
pub(crate) trait McpRpc: Send + Sync {
    async fn request(&self, method: &str, params: Option<Value>) -> Result<Value>;
    async fn request_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        timeout(request_timeout, self.request(method, params))
            .await
            .with_context(|| {
                format!(
                    "MCP RPC request {method} 超过 {}ms timeout",
                    request_timeout.as_millis()
                )
            })?
    }
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

    async fn request_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        StdioRpcClient::request_with_timeout(self, method, params, request_timeout).await
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
    roots: Vec<McpRoot>,
    credential: Option<TokenCredentialProvider>,
    elicitation: Arc<ElicitationBridge>,
    server_name: String,
}

struct HttpMcpConfig {
    server_name: String,
    url: Url,
    headers: HeaderMap,
    secrets: Vec<String>,
    credential: Option<TokenCredentialProvider>,
    allow_private_network: bool,
    request_timeout: Duration,
    roots: Vec<McpRoot>,
    elicitation: Arc<ElicitationBridge>,
}

struct HttpNotificationConfig {
    url: Url,
    headers: HeaderMap,
    session: String,
    protocol_version: Arc<Mutex<String>>,
    allow_private_network: bool,
    secrets: Vec<String>,
    roots: Vec<McpRoot>,
    credential: Option<TokenCredentialProvider>,
    elicitation: Arc<ElicitationBridge>,
    server_name: String,
}

struct LegacySseMcpRpc {
    post_url: Arc<Mutex<Option<Url>>>,
    headers: HeaderMap,
    secrets: Vec<String>,
    credential: Option<TokenCredentialProvider>,
    allow_private_network: bool,
    request_timeout: Duration,
    next_id: AtomicU64,
    pending: LegacyPending,
    events: broadcast::Sender<Value>,
    listener_task: Mutex<Option<JoinHandle<()>>>,
    closing: Arc<AtomicBool>,
}

type LegacyPending =
    Arc<Mutex<HashMap<String, oneshot::Sender<std::result::Result<Value, String>>>>>;

struct LegacySseListenerConfig {
    endpoint: Url,
    post_url: Arc<Mutex<Option<Url>>>,
    headers: HeaderMap,
    secrets: Vec<String>,
    credential: Option<TokenCredentialProvider>,
    allow_private_network: bool,
    pending: LegacyPending,
    events: broadcast::Sender<Value>,
    closing: Arc<AtomicBool>,
    request_handler: McpClientRequestHandler,
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
    elicitation: Arc<ElicitationBridge>,
    call_lock: Arc<Mutex<()>>,
}

struct McpManager {
    clients: StdRwLock<Vec<Arc<McpClient>>>,
    reconnect_configs: StdRwLock<HashMap<String, ServerConfig>>,
    reconnect_lock: Mutex<()>,
    known_tools: Mutex<HashMap<String, HashSet<String>>>,
    resource_handles: Arc<Mutex<ResourceHandleStore>>,
    server_states: Arc<McpServerStates>,
    connection_task: Mutex<Option<JoinHandle<()>>>,
    strict: bool,
    debug: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpServerStateKind {
    Pending,
    Connected,
    Failed,
    NeedsAuth,
    Disabled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct McpServerState {
    name: String,
    kind: McpServerStateKind,
}

struct McpServerStates {
    values: StdRwLock<Vec<McpServerState>>,
    generation: watch::Sender<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct WaitForMcpServersResult {
    ready: bool,
    connected: Vec<String>,
    failed: Vec<String>,
    still_pending: Vec<String>,
    needs_auth: Vec<String>,
    disabled: Vec<String>,
    unknown: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitForMcpServersInput {
    servers: Option<Vec<String>>,
}

impl McpServerStates {
    fn new(values: Vec<McpServerState>) -> Self {
        let (generation, _) = watch::channel(0_u64);
        Self {
            values: StdRwLock::new(values),
            generation,
        }
    }

    fn set(&self, name: &str, kind: McpServerStateKind) {
        let changed = {
            let mut values = self
                .values
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(state) = values
                .iter_mut()
                .find(|state| state.name.eq_ignore_ascii_case(name))
            else {
                return;
            };
            if state.kind == kind {
                false
            } else {
                state.kind = kind;
                true
            }
        };
        if changed {
            let next = (*self.generation.borrow()).wrapping_add(1);
            self.generation.send_replace(next);
        }
    }

    fn pending_names(&self) -> Vec<String> {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|state| state.kind == McpServerStateKind::Pending)
            .map(|state| state.name.clone())
            .collect()
    }

    fn enabled_count(&self) -> usize {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|state| state.kind != McpServerStateKind::Disabled)
            .count()
    }

    fn order_of(&self, name: &str) -> usize {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .position(|state| state.name.eq_ignore_ascii_case(name))
            .unwrap_or(usize::MAX)
    }

    fn status(&self) -> Vec<McpServerStatus> {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .take(MAX_SERVERS)
            .map(|state| McpServerStatus {
                name: state.name.clone(),
                status: match state.kind {
                    McpServerStateKind::Connected => McpServerStatusKind::Connected,
                    McpServerStateKind::Pending => McpServerStatusKind::Pending,
                    McpServerStateKind::Failed => McpServerStatusKind::Failed,
                    McpServerStateKind::NeedsAuth => McpServerStatusKind::NeedsAuth,
                    McpServerStateKind::Disabled => McpServerStatusKind::Disabled,
                },
            })
            .collect()
    }

    fn snapshot(&self, requested: &[String]) -> WaitForMcpServersResult {
        let requested_keys = requested
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let values = self
            .values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut result = WaitForMcpServersResult::default();
        let mut matched = HashSet::new();
        for state in values
            .iter()
            .filter(|state| requested_keys.contains(&state.name.to_ascii_lowercase()))
        {
            matched.insert(state.name.to_ascii_lowercase());
            match state.kind {
                McpServerStateKind::Pending => result.still_pending.push(state.name.clone()),
                McpServerStateKind::Connected => result.connected.push(state.name.clone()),
                McpServerStateKind::Failed => result.failed.push(state.name.clone()),
                McpServerStateKind::NeedsAuth => result.needs_auth.push(state.name.clone()),
                McpServerStateKind::Disabled => result.disabled.push(state.name.clone()),
            }
        }
        result.unknown = requested
            .iter()
            .filter(|name| !matched.contains(&name.to_ascii_lowercase()))
            .cloned()
            .collect();
        result.ready = result.still_pending.is_empty()
            && result.failed.is_empty()
            && result.needs_auth.is_empty()
            && result.disabled.is_empty()
            && result.unknown.is_empty();
        result
    }

    async fn wait_for(
        &self,
        requested: Vec<String>,
        maximum_wait: Duration,
    ) -> WaitForMcpServersResult {
        let mut changes = self.generation.subscribe();
        let deadline = Instant::now() + maximum_wait;
        loop {
            let current = self.snapshot(&requested);
            if current.still_pending.is_empty() {
                return current;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return current;
            };
            if remaining.is_zero() {
                return current;
            }
            match timeout(remaining, changes.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => return self.snapshot(&requested),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResourceHandleKind {
    Direct,
    Template,
    Linked,
}

#[derive(Clone, Debug)]
struct ResourceHandleEntry {
    server: String,
    raw: String,
    kind: ResourceHandleKind,
    variables: Vec<String>,
}

#[derive(Default)]
struct ResourceHandleStore {
    entries: HashMap<String, ResourceHandleEntry>,
    raw_bytes: usize,
}

pub async fn connect_mcp(
    settings: &Settings,
    workspace: &Path,
    debug: bool,
) -> Result<Option<McpIntegration>> {
    let configured_states = configured_server_states(settings)?;
    if configured_states.is_empty() {
        return Ok(None);
    }
    let configs = parse_server_configs(settings, workspace)?;
    let reconnect_configs = configs
        .iter()
        .map(|config| (config.name.to_ascii_lowercase(), config.clone()))
        .collect();
    let has_enabled_servers = !configs.is_empty();
    let strict = settings
        .raw
        .get("strictMcpConfig")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let server_states = Arc::new(McpServerStates::new(configured_states));
    let mut clients = Vec::new();
    let mut background_configs = configs;
    if strict {
        let mut attempts = stream::iter(background_configs.into_iter().enumerate())
            .map(|(index, config)| async move {
                let name = config.name.clone();
                let auth_configured = server_config_uses_auth(&config);
                (
                    index,
                    name,
                    auth_configured,
                    McpClient::connect(config).await,
                )
            })
            .buffer_unordered(MAX_CONCURRENT_SERVER_STARTS)
            .collect::<Vec<_>>()
            .await;
        attempts.sort_by_key(|(index, _, _, _)| *index);
        background_configs = Vec::new();
        for (_, name, auth_configured, attempt) in attempts {
            match attempt {
                Ok(client) => {
                    server_states.set(&name, McpServerStateKind::Connected);
                    clients.push(client);
                }
                Err(error) => {
                    server_states.set(&name, classify_connection_failure(&error, auth_configured));
                    for client in &clients {
                        client.shutdown().await;
                    }
                    return Err(error);
                }
            }
        }
    }
    let manager = Arc::new(McpManager {
        clients: StdRwLock::new(clients),
        reconnect_configs: StdRwLock::new(reconnect_configs),
        reconnect_lock: Mutex::new(()),
        known_tools: Mutex::new(HashMap::new()),
        resource_handles: Arc::new(Mutex::new(ResourceHandleStore::default())),
        server_states,
        connection_task: Mutex::new(None),
        strict,
        debug,
    });
    if !strict && !background_configs.is_empty() {
        manager
            .start_background_connections(background_configs)
            .await;
    }
    let deferred_tools = if strict {
        match manager.discover_initial_tools().await {
            Ok(tools) => tools,
            Err(error) => {
                for client in manager.clients_snapshot() {
                    client.shutdown().await;
                }
                return Err(error);
            }
        }
    } else {
        Vec::new()
    };
    let mut active_tools: Vec<Arc<dyn Tool>> = Vec::new();
    active_tools.push(Arc::new(WaitForMcpServersTool {
        states: Arc::clone(&manager.server_states),
        wait_timeout: WAIT_FOR_MCP_SERVERS_TIMEOUT,
    }));
    let clients = manager.clients_snapshot();
    if (!strict && has_enabled_servers) || clients.iter().any(|client| client.supports_resources) {
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
    if (!strict && has_enabled_servers) || clients.iter().any(|client| client.supports_prompts) {
        active_tools.push(Arc::new(ListMcpPromptsTool {
            manager: Arc::clone(&manager),
        }));
        active_tools.push(Arc::new(GetMcpPromptTool {
            manager: Arc::clone(&manager),
        }));
    }
    let server_count = manager.server_states.enabled_count();
    let service: Arc<dyn ToolService> = manager.clone();
    let discovery: Arc<dyn ToolDiscovery> = manager.clone();
    let hook_invoker: Arc<dyn McpHookInvoker> = manager.clone();
    let control: Arc<dyn McpControl> = manager;
    Ok(Some(McpIntegration {
        active_tools,
        deferred_tools,
        service,
        discovery,
        hook_invoker,
        control,
        server_count,
    }))
}

fn configured_server_states(settings: &Settings) -> Result<Vec<McpServerState>> {
    let Some(raw_servers) = settings.raw.get("mcpServers") else {
        return Ok(Vec::new());
    };
    let raw_servers = raw_servers
        .as_object()
        .context("mcpServers 必须是 JSON object")?;
    if raw_servers.len() > MAX_SERVERS {
        bail!("mcpServers 超过 {MAX_SERVERS} 个限制")
    }
    let mut normalized_names = HashSet::new();
    let mut states = Vec::with_capacity(raw_servers.len());
    for (name, value) in raw_servers {
        if name.is_empty() || name.len() > MAX_SERVER_NAME_BYTES {
            bail!("MCP server 名称长度无效: {name:?}")
        }
        if !normalized_names.insert(name.to_ascii_lowercase()) {
            bail!("MCP server 名称大小写归一后冲突: {name}")
        }
        let raw: RawServerConfig = serde_json::from_value(value.clone())
            .with_context(|| format!("MCP server {name} 配置无效"))?;
        states.push(McpServerState {
            name: name.clone(),
            kind: if raw.disabled {
                McpServerStateKind::Disabled
            } else {
                McpServerStateKind::Pending
            },
        });
    }
    Ok(states)
}

fn server_config_uses_auth(config: &ServerConfig) -> bool {
    match &config.transport {
        ServerTransport::Http { credential, .. }
        | ServerTransport::WebSocket { credential, .. } => credential.is_some(),
        ServerTransport::Stdio { .. } => false,
    }
}

fn classify_connection_failure(error: &anyhow::Error, auth_configured: bool) -> McpServerStateKind {
    let authentication_required = error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("MCP HTTP 401")
            || message.contains("MCP legacy SSE HTTP 401")
            || (message.contains("401") && message.to_ascii_lowercase().contains("unauthorized"))
            || message.contains("authorization required")
            || message.contains("authorization 尚未完成")
            || (auth_configured
                && (message.contains("MCP bearer token")
                    || message.contains("OAuth callback")
                    || message.contains("OAuth client secret")
                    || message.contains("OAuth authorization server 拒绝")))
    });
    if authentication_required {
        McpServerStateKind::NeedsAuth
    } else {
        McpServerStateKind::Failed
    }
}

fn parse_server_configs(settings: &Settings, workspace: &Path) -> Result<Vec<ServerConfig>> {
    let secret_env_scrubber = SecretEnvScrubber::from_settings(settings)?;
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
        let roots = resolve_mcp_roots(&raw.roots, workspace)
            .with_context(|| format!("MCP server {name} roots 无效"))?;
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
                if !raw.headers.is_empty() || raw.allow_private_network || raw.auth.is_some() {
                    bail!("MCP server {name} 的 headers/allowPrivateNetwork/auth 仅适用于 HTTP")
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
                let transport_type = raw.transport_type.as_deref().unwrap_or("http");
                if !matches!(
                    transport_type,
                    "http" | "streamable-http" | "sse" | "websocket" | "ws"
                ) {
                    bail!("MCP server {name} URL transport type 无效")
                }
                if !raw.args.is_empty() || !raw.env.is_empty() || raw.cwd.is_some() {
                    bail!("MCP server {name} 的 args/env/cwd 仅适用于 stdio")
                }
                let websocket = matches!(transport_type, "websocket" | "ws");
                let url = Url::parse(&url).context("MCP URL 无效")?;
                let valid_scheme = if websocket {
                    matches!(url.scheme(), "ws" | "wss")
                } else {
                    matches!(url.scheme(), "http" | "https")
                };
                if !valid_scheme
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.host_str().is_none()
                    || url.fragment().is_some()
                {
                    bail!("MCP URL scheme/host 无效，或包含凭据/fragment")
                }
                for (key, _) in url.query_pairs() {
                    if sensitive_query_key(&key) {
                        bail!("MCP HTTP URL 不允许在 query 中携带凭据参数 {key:?}；请改用 headers")
                    }
                }
                let (headers, mut secrets) = parse_http_headers(raw.headers)?;
                let credential = raw
                    .auth
                    .map(|auth| {
                        parse_auth_config(
                            name,
                            auth,
                            workspace,
                            &url,
                            raw.allow_private_network,
                            &secret_env_scrubber,
                        )
                    })
                    .transpose()?;
                if credential.is_some() && headers.contains_key(AUTHORIZATION) {
                    bail!("MCP server {name} 不能同时配置 auth 与 Authorization header")
                }
                if websocket
                    && matches!(credential.as_ref(), Some(TokenCredentialProvider::OAuth(_)))
                {
                    bail!("MCP OAuth 仅适用于 HTTP/SSE transport")
                }
                secrets.extend(
                    url.query_pairs()
                        .filter_map(|(_, value)| (!value.is_empty()).then(|| value.into_owned())),
                );
                if websocket {
                    ServerTransport::WebSocket {
                        url,
                        headers: Box::new(headers),
                        secrets,
                        allow_private_network: raw.allow_private_network,
                        credential,
                    }
                } else {
                    ServerTransport::Http {
                        url,
                        headers: Box::new(headers),
                        secrets,
                        allow_private_network: raw.allow_private_network,
                        credential,
                        legacy_sse: transport_type == "sse",
                    }
                }
            }
            _ => bail!("MCP server {name} 必须且只能配置 command 或 url 之一"),
        };
        let timeout_ms = raw
            .timeout_ms
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
            .clamp(MIN_REQUEST_TIMEOUT_MS, MAX_REQUEST_TIMEOUT_MS);
        let elicitation_timeout_ms = raw
            .elicitation_timeout_ms
            .unwrap_or(DEFAULT_ELICITATION_TIMEOUT_MS)
            .clamp(MIN_ELICITATION_TIMEOUT_MS, MAX_ELICITATION_TIMEOUT_MS)
            .min(
                timeout_ms
                    .saturating_sub(100)
                    .max(MIN_ELICITATION_TIMEOUT_MS),
            );
        configs.push(ServerConfig {
            name: name.clone(),
            namespace,
            transport,
            request_timeout: Duration::from_millis(timeout_ms),
            elicitation_timeout: Duration::from_millis(elicitation_timeout_ms),
            roots,
            secret_env_scrubber: secret_env_scrubber.clone(),
        });
    }
    Ok(configs)
}

fn parse_auth_config(
    server_name: &str,
    raw: RawAuthConfig,
    workspace: &Path,
    server_url: &Url,
    allow_private_network: bool,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<TokenCredentialProvider> {
    match raw {
        RawAuthConfig::Env { env } => {
            validate_env_name(&env)
                .with_context(|| format!("MCP server {server_name} auth.env 无效"))?;
            Ok(TokenCredentialProvider::Env { name: env })
        }
        RawAuthConfig::File { path } => {
            if path.is_empty() || path.len() > MAX_ROOT_PATH_BYTES || path.contains('\0') {
                bail!("MCP server {server_name} auth.path 为空、过长或包含 NUL")
            }
            let path = PathBuf::from(path);
            if !path.is_absolute() {
                bail!("MCP server {server_name} auth.path 必须是绝对路径")
            }
            validate_private_token_file(&path)
                .with_context(|| format!("MCP server {server_name} auth.path 不安全"))?;
            Ok(TokenCredentialProvider::File { path })
        }
        RawAuthConfig::Command {
            command,
            args,
            timeout_ms,
        } => {
            if command.trim().is_empty()
                || command.len() > MAX_COMMAND_BYTES
                || command.contains('\0')
            {
                bail!("MCP server {server_name} auth.command 为空、过长或包含 NUL")
            }
            if args.len() > MAX_ARGS
                || args
                    .iter()
                    .any(|arg| arg.len() > MAX_ARG_BYTES || arg.contains('\0'))
            {
                bail!("MCP server {server_name} auth.args 无效或超过限制")
            }
            let cwd = std::fs::canonicalize(workspace).context("无法解析 MCP auth workspace")?;
            let command = resolve_trusted_executable(&command, &cwd).with_context(|| {
                format!("MCP server {server_name} auth.command executable 不可信")
            })?;
            let timeout = Duration::from_millis(
                timeout_ms
                    .unwrap_or(DEFAULT_AUTH_COMMAND_TIMEOUT_MS)
                    .clamp(MIN_AUTH_COMMAND_TIMEOUT_MS, MAX_AUTH_COMMAND_TIMEOUT_MS),
            );
            Ok(TokenCredentialProvider::Command {
                command,
                args,
                cwd,
                timeout,
                secret_env_scrubber: secret_env_scrubber.clone(),
            })
        }
        RawAuthConfig::OAuth { config } => Ok(TokenCredentialProvider::OAuth(
            OAuthCredentialProvider::from_raw(
                server_name,
                server_url,
                allow_private_network,
                config,
            )?,
        )),
    }
}

fn validate_env_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 256
        || !name.bytes().enumerate().all(|(index, byte)| {
            matches!(
                (index, byte),
                (0, b'A'..=b'Z' | b'a'..=b'z' | b'_')
                    | (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
            )
        })
    {
        bail!("environment variable name 必须是有效 identifier")
    }
    Ok(())
}

fn validate_private_token_file(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("无法检查 token file: {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("token file 必须是非 symlink regular file")
    }
    validate_private_token_metadata(&metadata)
}

fn validate_private_token_metadata(metadata: &std::fs::Metadata) -> Result<()> {
    if !metadata.is_file() {
        bail!("token file 必须是 regular file")
    }
    if metadata.len() > MAX_AUTH_TOKEN_BYTES as u64 {
        bail!("token file 超过 {MAX_AUTH_TOKEN_BYTES} 字节限制")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("token file 必须禁止 group/other 访问（建议 0600）")
        }
    }
    Ok(())
}

fn open_private_token_file(path: &Path) -> Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法安全打开 MCP bearer token file: {}", path.display()))?;
    let metadata = file
        .metadata()
        .context("无法检查已打开的 MCP bearer token file")?;
    validate_private_token_metadata(&metadata)?;
    #[cfg(not(unix))]
    {
        let path_metadata =
            std::fs::symlink_metadata(path).context("无法复查 MCP bearer token file 路径")?;
        if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
            bail!("MCP bearer token file 路径在打开后不再是 regular file")
        }
    }
    Ok(file)
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
    if config.roots.len() > MAX_ROOTS {
        bail!("MCP server {name} roots 超过 {MAX_ROOTS} 项限制")
    }
    for root in &config.roots {
        if root.is_empty() || root.len() > MAX_ROOT_PATH_BYTES || root.contains('\0') {
            bail!("MCP server {name} root 为空、过长或包含 NUL")
        }
    }
    Ok(())
}

fn resolve_mcp_roots(values: &[String], workspace: &Path) -> Result<Vec<McpRoot>> {
    let mut seen = HashSet::new();
    let mut roots = Vec::with_capacity(values.len());
    for value in values {
        let path = PathBuf::from(value);
        let path = if path.is_absolute() {
            path
        } else {
            workspace.join(path)
        };
        let path = std::fs::canonicalize(&path)
            .with_context(|| format!("无法解析 MCP root: {}", path.display()))?;
        if !path.is_dir() {
            bail!("MCP root 不是目录: {}", path.display())
        }
        if !seen.insert(path.clone()) {
            continue;
        }
        let uri = Url::from_directory_path(&path)
            .map(String::from)
            .map_err(|_| anyhow::anyhow!("无法将 MCP root 转换为 file URI"))?;
        if uri.len() > MAX_RESOURCE_URI_BYTES {
            bail!("MCP root URI 超过 {MAX_RESOURCE_URI_BYTES} 字节限制")
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("workspace");
        roots.push(McpRoot {
            uri,
            name: sanitize_text(name, 256),
        });
    }
    Ok(roots)
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
                | "upgrade"
                | "origin"
                | "sec-websocket-accept"
                | "sec-websocket-extensions"
                | "sec-websocket-key"
                | "sec-websocket-protocol"
                | "sec-websocket-version"
        ) {
            bail!("MCP HTTP 不允许覆盖 header {name}")
        }
        let mut value = HeaderValue::from_str(&value).context("MCP HTTP header value 无效")?;
        if !value.as_bytes().is_empty() {
            secrets.push(String::from_utf8_lossy(value.as_bytes()).into_owned());
        }
        value.set_sensitive(true);
        headers.insert(name, value);
    }
    Ok((headers, secrets))
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
        || normalized.contains("apikey")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("signature")
        || normalized.contains("session")
}

impl TokenCredentialProvider {
    async fn bearer_header(&self) -> Result<(HeaderValue, String)> {
        let token = match self {
            Self::Env { name } => std::env::var(name)
                .map_err(|_| anyhow::anyhow!("MCP bearer token environment variable 未设置"))?,
            Self::File { path } => {
                let mut bytes = Vec::new();
                open_private_token_file(path)?
                    .take(MAX_AUTH_TOKEN_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > MAX_AUTH_TOKEN_BYTES {
                    bail!("MCP bearer token file 超过 {MAX_AUTH_TOKEN_BYTES} 字节限制")
                }
                String::from_utf8(bytes).context("MCP bearer token file 不是 UTF-8")?
            }
            Self::Command {
                command,
                args,
                cwd,
                timeout: command_timeout,
                secret_env_scrubber,
            } => {
                run_token_command(command, args, cwd, *command_timeout, secret_env_scrubber).await?
            }
            Self::OAuth(provider) => return provider.bearer_header().await,
        };
        let token = normalize_bearer_token(token)?;
        let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
            .context("MCP bearer token 不能编码为 Authorization header")?;
        header.set_sensitive(true);
        Ok((header, token))
    }

    async fn force_refresh_bearer_header(&self) -> Result<(HeaderValue, String)> {
        match self {
            Self::OAuth(provider) => provider.force_refresh_bearer_header().await,
            _ => self.bearer_header().await,
        }
    }

    fn is_oauth(&self) -> bool {
        matches!(self, Self::OAuth(_))
    }
}

fn normalize_bearer_token(token: String) -> Result<String> {
    let token = token.trim().to_owned();
    if token.is_empty() || token.len() > MAX_AUTH_TOKEN_BYTES {
        bail!("MCP bearer token 为空或超过 {MAX_AUTH_TOKEN_BYTES} 字节限制")
    }
    if token.chars().any(char::is_whitespace) || token.chars().any(char::is_control) {
        bail!("MCP bearer token 包含空白或控制字符")
    }
    Ok(token)
}

async fn run_token_command(
    command: &Path,
    args: &[String],
    cwd: &Path,
    command_timeout: Duration,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<String> {
    let mut process = Command::new(command);
    process
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    secret_env_scrubber.scrub_tokio(&mut process);
    let (mut child, process_guard) =
        spawn_managed(&mut process).context("无法启动 MCP bearer token command")?;
    let stdout = child
        .stdout
        .take()
        .context("无法读取 MCP bearer token command stdout")?;
    let mut reader = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout
            .take(MAX_AUTH_TOKEN_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        Ok::<Vec<u8>, std::io::Error>(bytes)
    });
    let outcome = timeout(command_timeout, async {
        let status = child.wait().await?;
        process_guard.terminate();
        let bytes = (&mut reader)
            .await
            .context("MCP bearer token reader task 失败")??;
        Ok::<_, anyhow::Error>((status, bytes))
    })
    .await;
    let (status, bytes) = match outcome {
        Ok(result) => result?,
        Err(_) => {
            process_guard.terminate();
            let _ = child.start_kill();
            let _ = child.wait().await;
            reader.abort();
            bail!(
                "MCP bearer token command 超过 {}ms timeout",
                command_timeout.as_millis()
            )
        }
    };
    if !status.success() {
        bail!("MCP bearer token command 失败（secret output 已隐藏）")
    }
    if bytes.len() > MAX_AUTH_TOKEN_BYTES {
        bail!("MCP bearer token command stdout 超过 {MAX_AUTH_TOKEN_BYTES} 字节限制")
    }
    String::from_utf8(bytes).context("MCP bearer token command stdout 不是 UTF-8")
}

pub(crate) async fn authorized_headers(
    base: &HeaderMap,
    credential: Option<&TokenCredentialProvider>,
) -> Result<(HeaderMap, Option<String>)> {
    let mut headers = base.clone();
    let secret = if let Some(credential) = credential {
        let (header, token) = credential.bearer_header().await?;
        headers.insert(AUTHORIZATION, header);
        Some(token)
    } else {
        None
    };
    Ok((headers, secret))
}

async fn force_refreshed_headers(
    base: &HeaderMap,
    credential: Option<&TokenCredentialProvider>,
) -> Result<(HeaderMap, Option<String>)> {
    let mut headers = base.clone();
    let secret = if let Some(credential) = credential {
        let (header, token) = credential.force_refresh_bearer_header().await?;
        headers.insert(AUTHORIZATION, header);
        Some(token)
    } else {
        None
    };
    Ok((headers, secret))
}

fn request_secrets(configured: &[String], dynamic: Option<String>) -> Vec<String> {
    let mut secrets = configured.to_vec();
    if let Some(secret) = dynamic {
        secrets.push(secret);
    }
    secrets
}

impl HttpMcpRpc {
    fn new(config: HttpMcpConfig) -> Self {
        let HttpMcpConfig {
            server_name,
            url,
            headers,
            secrets,
            credential,
            allow_private_network,
            request_timeout,
            roots,
            elicitation,
        } = config;
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
            roots,
            credential,
            elicitation,
            server_name,
        }
    }

    async fn send_message(
        &self,
        message: Value,
        expected_id: Option<u64>,
    ) -> Result<Option<Value>> {
        self.send_message_with_timeout(message, expected_id, self.request_timeout)
            .await
    }

    async fn send_message_with_timeout(
        &self,
        message: Value,
        expected_id: Option<u64>,
        request_timeout: Duration,
    ) -> Result<Option<Value>> {
        let body = serde_json::to_vec(&message)?;
        if body.len() > 4 * 1024 * 1024 {
            bail!("MCP HTTP request 超过 4 MiB 限制")
        }
        timeout(request_timeout, async {
            let mut force_refresh = false;
            let (response, secrets) = loop {
                let client = secure_client_for_url(&self.url, self.allow_private_network).await?;
                let (headers, dynamic_secret) = if force_refresh {
                    force_refreshed_headers(&self.headers, self.credential.as_ref()).await?
                } else {
                    authorized_headers(&self.headers, self.credential.as_ref()).await?
                };
                let secrets = request_secrets(&self.secrets, dynamic_secret);
                let mut request = client
                    .post(self.url.clone())
                    .headers(headers)
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .header(
                        "mcp-protocol-version",
                        self.protocol_version.lock().await.as_str(),
                    )
                    .body(body.clone());
                if let Some(session) = self.session_id.lock().await.as_ref() {
                    request = request.header("mcp-session-id", session);
                }
                // Reqwest errors can contain the complete endpoint URL. Keep sources opaque.
                let response = request
                    .send()
                    .await
                    .map_err(|_| anyhow::anyhow!("MCP HTTP POST 失败"))?;
                if response.status().as_u16() == 401
                    && !force_refresh
                    && self
                        .credential
                        .as_ref()
                        .is_some_and(TokenCredentialProvider::is_oauth)
                {
                    force_refresh = true;
                    continue;
                }
                break (response, secrets);
            };
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
            if !status.is_success() {
                let response_body =
                    read_http_body_limited(response, MAX_HTTP_RESPONSE_BYTES).await?;
                let text = redact_secrets(&String::from_utf8_lossy(&response_body), &secrets);
                bail!(
                    "MCP HTTP {}: {}",
                    status.as_u16(),
                    truncate_text(&text, 4096)
                )
            }
            if content_type.contains("text/event-stream") {
                return self
                    .read_streamable_sse_response(response, expected_id, &secrets)
                    .await;
            }
            let response_body = read_http_body_limited(response, MAX_HTTP_RESPONSE_BYTES).await?;
            let messages = vec![
                serde_json::from_slice(&response_body)
                    .context("MCP HTTP response 不是有效 JSON")?,
            ];
            let mut matching = None;
            let mut server_requests = Vec::new();
            for mut value in messages {
                redact_json_secrets(&mut value, &secrets);
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
                self.respond_to_server_request(&request).await;
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
                request_timeout.as_millis()
            )
        })?
    }

    async fn read_streamable_sse_response(
        &self,
        response: reqwest::Response,
        expected_id: Option<u64>,
        secrets: &[String],
    ) -> Result<Option<Value>> {
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut received = 0usize;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("读取 MCP streamable HTTP SSE 失败")?;
            received = received.saturating_add(chunk.len());
            if received > MAX_HTTP_RESPONSE_BYTES {
                bail!("MCP HTTP SSE response 超过 {MAX_HTTP_RESPONSE_BYTES} 字节限制")
            }
            buffer.extend_from_slice(&chunk);
            while let Some((end, separator)) = sse_frame_end(&buffer) {
                let frame = buffer.drain(..end).collect::<Vec<_>>();
                buffer.drain(..separator);
                let Some(mut value) = parse_sse_frame(&frame)? else {
                    continue;
                };
                if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                    bail!("MCP HTTP SSE message 缺少 jsonrpc=2.0")
                }
                if value.get("method").is_some() {
                    let _ = self.events.send(value.clone());
                    if value.get("id").is_some() {
                        self.respond_to_server_request(&value).await;
                    }
                    continue;
                }
                redact_json_secrets(&mut value, secrets);
                if expected_id.is_some_and(|id| value.get("id") == Some(&json!(id))) {
                    return parse_rpc_result(&value).map(Some);
                }
            }
            if buffer.len() > MAX_LEGACY_SSE_BUFFER_BYTES {
                bail!("MCP HTTP SSE frame 超过 {MAX_LEGACY_SSE_BUFFER_BYTES} 字节限制")
            }
        }
        if expected_id.is_some() {
            bail!("MCP HTTP SSE response 中没有匹配的 JSON-RPC id")
        }
        Ok(None)
    }

    async fn respond_to_server_request(&self, request: &Value) {
        let Some(id) = request.get("id") else {
            return;
        };
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let handler = McpClientRequestHandler {
            server_name: self.server_name.clone(),
            roots: self.roots.clone(),
            elicitation: Arc::clone(&self.elicitation),
        };
        let response = if let Some(result) = handler.result(method, request.get("params")) {
            json!({"jsonrpc": "2.0", "id": id, "result": result})
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
        let Ok((headers, _)) = authorized_headers(&self.headers, self.credential.as_ref()).await
        else {
            return;
        };
        let mut request = client
            .post(self.url.clone())
            .headers(headers)
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

    async fn request_with_timeout(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            bail!("MCP HTTP request id 已耗尽")
        }
        let mut message = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        self.send_message_with_timeout(message, Some(id), request_timeout)
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
                roots: self.roots.clone(),
                credential: self.credential.clone(),
                elicitation: Arc::clone(&self.elicitation),
                server_name: self.server_name.clone(),
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
        let Ok((headers, _)) = authorized_headers(&self.headers, self.credential.as_ref()).await
        else {
            return;
        };
        let _ = timeout(
            Duration::from_secs(2),
            client
                .delete(self.url.clone())
                .headers(headers)
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

impl LegacySseMcpRpc {
    async fn connect(
        endpoint: Url,
        headers: HeaderMap,
        secrets: Vec<String>,
        credential: Option<TokenCredentialProvider>,
        allow_private_network: bool,
        request_timeout: Duration,
        request_handler: McpClientRequestHandler,
    ) -> Result<Self> {
        let client = secure_client_for_url(&endpoint, allow_private_network).await?;
        let (authorized, dynamic_secret) =
            authorized_headers(&headers, credential.as_ref()).await?;
        let response = timeout(
            request_timeout,
            client
                .get(endpoint.clone())
                .headers(authorized)
                .header("accept", "text/event-stream")
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("MCP legacy SSE GET timeout"))?
        .map_err(|_| anyhow::anyhow!("MCP legacy SSE GET 失败"))?;
        let status = response.status();
        if !status.is_success() {
            let body = read_http_body_limited(response, 4096).await?;
            let request_secrets = request_secrets(&secrets, dynamic_secret);
            let text = redact_secrets(&String::from_utf8_lossy(&body), &request_secrets);
            bail!(
                "MCP legacy SSE HTTP {}: {}",
                status.as_u16(),
                truncate_text(&text, 4096)
            )
        }
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !content_type.contains("text/event-stream") {
            bail!("MCP legacy SSE endpoint 未返回 text/event-stream")
        }

        let post_url = Arc::new(Mutex::new(None));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let (events, _) = broadcast::channel(128);
        let closing = Arc::new(AtomicBool::new(false));
        let (endpoint_sender, endpoint_receiver) = oneshot::channel();
        let listener_task = tokio::spawn(legacy_sse_listener(
            response,
            LegacySseListenerConfig {
                endpoint: endpoint.clone(),
                post_url: Arc::clone(&post_url),
                headers: headers.clone(),
                secrets: request_secrets(&secrets, dynamic_secret),
                credential: credential.clone(),
                allow_private_network,
                pending: Arc::clone(&pending),
                events: events.clone(),
                closing: Arc::clone(&closing),
                request_handler: request_handler.clone(),
            },
            endpoint_sender,
        ));
        let endpoint_result = timeout(request_timeout, endpoint_receiver)
            .await
            .map_err(|_| anyhow::anyhow!("MCP legacy SSE endpoint event timeout"))?
            .context("MCP legacy SSE stream 在 endpoint event 前关闭")?;
        if let Err(error) = endpoint_result {
            listener_task.abort();
            bail!("{error}")
        }
        Ok(Self {
            post_url,
            headers,
            secrets,
            credential,
            allow_private_network,
            request_timeout,
            next_id: AtomicU64::new(1),
            pending,
            events,
            listener_task: Mutex::new(Some(listener_task)),
            closing,
        })
    }

    async fn post(&self, message: &Value) -> Result<()> {
        self.post_with_timeout(message, self.request_timeout).await
    }

    async fn post_with_timeout(&self, message: &Value, request_timeout: Duration) -> Result<()> {
        let url = self
            .post_url
            .lock()
            .await
            .clone()
            .context("MCP legacy SSE 尚未提供 POST endpoint")?;
        let mut secrets = self.secrets.clone();
        secrets.extend(
            url.query_pairs()
                .filter_map(|(_, value)| (!value.is_empty()).then(|| value.into_owned())),
        );
        legacy_post_json(
            &url,
            &self.headers,
            &secrets,
            self.credential.as_ref(),
            self.allow_private_network,
            message,
            request_timeout,
        )
        .await
    }

    async fn request_bounded(
        &self,
        method: &str,
        params: Option<Value>,
        request_timeout: Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            bail!("MCP legacy SSE request id 已耗尽")
        }
        let mut message = json!({"jsonrpc":"2.0", "id":id, "method":method});
        if let Some(params) = params {
            message["params"] = params;
        }
        let key = mcp_id_key(&json!(id))?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            if pending.len() >= MAX_LEGACY_PENDING_REQUESTS {
                bail!("MCP legacy SSE pending request 超过 {MAX_LEGACY_PENDING_REQUESTS} 项限制")
            }
            pending.insert(key.clone(), sender);
        }
        let outcome = timeout(request_timeout, async {
            self.post_with_timeout(&message, request_timeout).await?;
            match receiver.await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(error)) => bail!("{error}"),
                Err(_) => bail!("MCP legacy SSE stream 在响应前关闭"),
            }
        })
        .await;
        match outcome {
            Ok(result) => {
                if result.is_err() {
                    self.pending.lock().await.remove(&key);
                }
                result
            }
            Err(_) => {
                self.pending.lock().await.remove(&key);
                let _ = timeout(
                    Duration::from_secs(1),
                    self.notify(
                        "notifications/cancelled",
                        Some(json!({"requestId":id, "reason":"client timeout"})),
                    ),
                )
                .await;
                bail!(
                    "MCP legacy SSE request {method} 超过 {}ms timeout",
                    request_timeout.as_millis()
                )
            }
        }
    }
}

#[async_trait]
impl McpRpc for LegacySseMcpRpc {
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
        self.post(&message).await
    }

    fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    async fn set_protocol_version(&self, _: &str) {}

    async fn start_notifications(&self) {}

    async fn diagnostic_excerpt(&self) -> String {
        String::new()
    }

    async fn shutdown(&self) {
        self.closing.store(true, Ordering::Release);
        if let Some(task) = self.listener_task.lock().await.take() {
            task.abort();
        }
        fail_legacy_pending(&self.pending, "MCP legacy SSE 正在关闭").await;
    }
}

impl Drop for LegacySseMcpRpc {
    fn drop(&mut self) {
        self.closing.store(true, Ordering::Release);
        if let Some(task) = self.listener_task.get_mut().take() {
            task.abort();
        }
    }
}

async fn legacy_sse_listener(
    response: reqwest::Response,
    config: LegacySseListenerConfig,
    endpoint_sender: oneshot::Sender<std::result::Result<(), String>>,
) {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut received = 0usize;
    let mut endpoint_sender = Some(endpoint_sender);
    let mut response_secrets = config.secrets.clone();
    let mut reason = "MCP legacy SSE stream 已关闭".to_owned();
    'stream: while let Some(chunk) = stream.next().await {
        if config.closing.load(Ordering::Acquire) {
            reason = "MCP legacy SSE 正在关闭".to_owned();
            break;
        }
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(_) => {
                reason = "读取 MCP legacy SSE stream 失败".to_owned();
                break;
            }
        };
        received = received.saturating_add(chunk.len());
        if received > MAX_LEGACY_SSE_STREAM_BYTES {
            reason = format!("MCP legacy SSE stream 超过 {MAX_LEGACY_SSE_STREAM_BYTES} 字节限制");
            break;
        }
        buffer.extend_from_slice(&chunk);
        while let Some((end, separator)) = sse_frame_end(&buffer) {
            let frame = buffer.drain(..end).collect::<Vec<_>>();
            buffer.drain(..separator);
            let (event, data) = match parse_sse_event(&frame) {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(error) => {
                    reason = format!("MCP legacy SSE frame 无效: {error:#}");
                    break 'stream;
                }
            };
            if event.as_deref() == Some("endpoint") {
                let result = validate_legacy_post_url(&config.endpoint, &data)
                    .map_err(|error| format!("{error:#}"));
                match result {
                    Ok(url) => {
                        response_secrets.extend(url.query_pairs().filter_map(|(_, value)| {
                            (!value.is_empty()).then(|| value.into_owned())
                        }));
                        let mut current = config.post_url.lock().await;
                        if current.as_ref().is_some_and(|existing| existing != &url) {
                            reason = "MCP legacy SSE endpoint event 在连接中发生变化".to_owned();
                            break 'stream;
                        }
                        *current = Some(url);
                        if let Some(sender) = endpoint_sender.take() {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(error) => {
                        if let Some(sender) = endpoint_sender.take() {
                            let _ = sender.send(Err(error.clone()));
                        }
                        reason = error;
                        break 'stream;
                    }
                }
                continue;
            }
            if event.as_deref().is_some_and(|event| event != "message") {
                continue;
            }
            let mut message: Value = match serde_json::from_str(&data) {
                Ok(value) => value,
                Err(_) => {
                    reason = "MCP legacy SSE message 不是有效 JSON".to_owned();
                    break 'stream;
                }
            };
            if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                reason = "MCP legacy SSE message 缺少 jsonrpc=2.0".to_owned();
                break 'stream;
            }
            if let Some(method) = message.get("method").and_then(Value::as_str) {
                if let Some(id) = message.get("id") {
                    let result = config.request_handler.result(method, message.get("params"));
                    let response = result.map_or_else(
                        || json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32601,"message":"Client method not supported"}}),
                        |result| json!({"jsonrpc":"2.0", "id":id, "result":result}),
                    );
                    if let Some(url) = config.post_url.lock().await.clone() {
                        let _ = legacy_post_json(
                            &url,
                            &config.headers,
                            &response_secrets,
                            config.credential.as_ref(),
                            config.allow_private_network,
                            &response,
                            Duration::from_secs(5),
                        )
                        .await;
                    }
                } else {
                    redact_json_secrets(&mut message, &response_secrets);
                    let _ = config.events.send(message);
                }
                continue;
            }
            let Some(id) = message.get("id") else {
                reason = "MCP legacy SSE response 缺少 id".to_owned();
                break 'stream;
            };
            let key = match mcp_id_key(id) {
                Ok(key) => key,
                Err(error) => {
                    reason = format!("{error:#}");
                    break 'stream;
                }
            };
            redact_json_secrets(&mut message, &response_secrets);
            if let Some(sender) = config.pending.lock().await.remove(&key) {
                let result = parse_rpc_result(&message).map_err(|error| format!("{error:#}"));
                let _ = sender.send(result);
            }
        }
        if buffer.len() > MAX_LEGACY_SSE_BUFFER_BYTES {
            reason = format!("MCP legacy SSE frame 超过 {MAX_LEGACY_SSE_BUFFER_BYTES} 字节限制");
            break 'stream;
        }
    }
    if let Some(sender) = endpoint_sender.take() {
        let _ = sender.send(Err(reason.clone()));
    }
    fail_legacy_pending(&config.pending, &reason).await;
}

async fn legacy_post_json(
    url: &Url,
    headers: &HeaderMap,
    secrets: &[String],
    credential: Option<&TokenCredentialProvider>,
    allow_private_network: bool,
    message: &Value,
    request_timeout: Duration,
) -> Result<()> {
    let body = serde_json::to_vec(message)?;
    if body.len() > 4 * 1024 * 1024 {
        bail!("MCP legacy SSE request 超过 4 MiB 限制")
    }
    let client = secure_client_for_url(url, allow_private_network).await?;
    let (authorized, dynamic_secret) = authorized_headers(headers, credential).await?;
    let request_secrets = request_secrets(secrets, dynamic_secret);
    let response = timeout(
        request_timeout,
        client
            .post(url.clone())
            .headers(authorized)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("MCP legacy SSE POST timeout"))?
    .map_err(|_| anyhow::anyhow!("MCP legacy SSE POST 失败"))?;
    let status = response.status();
    let response_body = read_http_body_limited(response, 64 * 1024).await?;
    if !status.is_success() {
        let text = redact_secrets(&String::from_utf8_lossy(&response_body), &request_secrets);
        bail!(
            "MCP legacy SSE POST HTTP {}: {}",
            status.as_u16(),
            truncate_text(&text, 4096)
        )
    }
    Ok(())
}

fn validate_legacy_post_url(endpoint: &Url, data: &str) -> Result<Url> {
    if data.is_empty() || data.len() > MAX_RESOURCE_URI_BYTES {
        bail!("MCP legacy SSE endpoint data 为空或过长")
    }
    let url = endpoint
        .join(data)
        .context("MCP legacy SSE POST endpoint URL 无效")?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.scheme() != endpoint.scheme()
        || url.host_str() != endpoint.host_str()
        || url.port_or_known_default() != endpoint.port_or_known_default()
    {
        bail!("MCP legacy SSE POST endpoint 必须与 GET endpoint 同源且无凭据/fragment")
    }
    Ok(url)
}

fn parse_sse_event(frame: &[u8]) -> Result<Option<(Option<String>, String)>> {
    let text = std::str::from_utf8(frame).context("MCP SSE frame 不是 UTF-8")?;
    let mut event = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start());
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some((event, data.join("\n"))))
}

fn mcp_id_key(id: &Value) -> Result<String> {
    match id {
        Value::String(value) if value.len() <= 1024 => Ok(format!("s:{value}")),
        Value::Number(value) => Ok(format!("n:{value}")),
        _ => bail!("MCP JSON-RPC id 必须是 bounded string 或 number"),
    }
}

async fn fail_legacy_pending(pending: &LegacyPending, reason: &str) {
    let senders = pending
        .lock()
        .await
        .drain()
        .map(|(_, sender)| sender)
        .collect::<Vec<_>>();
    for sender in senders {
        let _ = sender.send(Err(reason.to_owned()));
    }
}

impl ElicitationBridge {
    fn new(timeout: Duration) -> Self {
        Self {
            active: StdRwLock::new(None),
            timeout,
        }
    }

    fn activate(&self, context: ToolContext) -> Result<ElicitationScope<'_>> {
        let mut active = self
            .active
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if active.is_some() {
            bail!("MCP elicitation bridge 已被另一个 tool call 占用")
        }
        *active = Some(context);
        Ok(ElicitationScope { bridge: self })
    }

    fn respond(&self, server_name: &str, params: Option<&Value>) -> Value {
        let response = self.try_respond(server_name, params);
        response.unwrap_or_else(|_| json!({"action":"cancel"}))
    }

    fn try_respond(&self, server_name: &str, params: Option<&Value>) -> Result<Value> {
        let params = params.context("MCP elicitation/create 缺少 params")?;
        let validated = validate_elicitation_request(server_name, params)?;
        let context = self
            .active
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .context("MCP elicitation 没有 active user interaction handler")?;
        let mut input = validated.interaction_input.clone();
        input["interaction_timeout_ms"] = json!(self.timeout.as_millis() as u64);
        let fallback_schema = validated.requested_schema.clone();
        let (sender, receiver) = std_mpsc::sync_channel(1);
        let worker_context = context.clone();
        let started = Instant::now();
        thread::Builder::new()
            .name("harness-mcp-elicitation".to_owned())
            .spawn(move || {
                let response =
                    worker_context.request_user_interaction("McpElicitation", input.clone());
                let _ = sender.send(response);
            })
            .context("无法启动 MCP elicitation interaction worker")?;
        let response = match receiver.recv_timeout(self.timeout) {
            Ok(Ok(Some(response))) => response,
            Ok(Ok(None)) => {
                let remaining = self.timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Ok(json!({"action":"cancel"}));
                }
                match prompt_tty_elicitation(
                    &context,
                    &validated.interaction_input,
                    fallback_schema.as_ref(),
                    remaining,
                )? {
                    Some(response) => response,
                    None => return Ok(json!({"action":"cancel"})),
                }
            }
            Ok(Err(_)) | Err(_) => return Ok(json!({"action":"cancel"})),
        };
        validate_elicitation_response(&response, validated.requested_schema.as_ref())
    }
}

fn prompt_tty_elicitation(
    context: &ToolContext,
    interaction: &Value,
    requested_schema: Option<&Value>,
    timeout: Duration,
) -> Result<Option<Value>> {
    if !context.permissions.interactive || context.agent_depth() != 0 || !io::stdin().is_terminal()
    {
        return Ok(None);
    }
    let server = interaction
        .get("mcp_server_name")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let message = interaction
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("MCP server requested input");
    let mode = interaction
        .get("mode")
        .and_then(Value::as_str)
        .context("MCP elicitation interaction 缺少 mode")?;
    eprintln!(
        "\n[MCP elicitation from {}] {}",
        sanitize_terminal_text(server, MAX_SERVER_NAME_BYTES),
        sanitize_terminal_text(message, MAX_ELICITATION_MESSAGE_BYTES)
    );
    match mode {
        "url" => {
            let uri = interaction
                .get("url")
                .and_then(Value::as_str)
                .context("MCP URL elicitation interaction 缺少 url")?;
            let metadata =
                safe_resource_uri_metadata(uri).context("MCP URL elicitation URL 无法安全显示")?;
            eprintln!("URL metadata: {}", serde_json::to_string(&metadata)?);
            eprintln!("URL (local terminal only): {}", serde_json::to_string(uri)?);
            eprint!("Respond with accept, decline, or cancel: ");
        }
        "form" => {
            let schema = requested_schema.context("MCP form elicitation 缺少 schema")?;
            eprintln!("Requested schema: {}", serde_json::to_string(schema)?);
            eprint!("Enter one-line JSON object, decline, or cancel: ");
        }
        _ => bail!("MCP elicitation interaction mode 无效"),
    }
    io::stderr().flush()?;
    let Some(line) = read_tty_line_with_timeout(MAX_ELICITATION_RESPONSE_BYTES, timeout)? else {
        eprintln!("\nMCP elicitation timed out; cancelling.");
        return Ok(Some(json!({"action":"cancel"})));
    };
    parse_tty_elicitation_response(mode, &line).map(Some)
}

fn parse_tty_elicitation_response(mode: &str, line: &str) -> Result<Value> {
    let trimmed = line.trim();
    let action = trimmed.to_ascii_lowercase();
    let simple_action = match action.as_str() {
        "accept" | "a" | "yes" | "y" if mode == "url" => Some("accept"),
        "decline" | "d" | "no" | "n" => Some("decline"),
        "cancel" | "c" | "quit" | "q" | "" => Some("cancel"),
        _ => None,
    };
    if let Some(action) = simple_action {
        return Ok(json!({"action":action}));
    }
    if mode != "form" {
        bail!("MCP URL elicitation 只接受 accept/decline/cancel")
    }
    let content: Value =
        serde_json::from_str(trimmed).context("MCP form elicitation 输入必须是单行 JSON object")?;
    if !content.is_object() {
        bail!("MCP form elicitation content 必须是 object")
    }
    Ok(json!({"action":"accept", "content":content}))
}

struct McpRawModeGuard {
    enabled_here: bool,
}

impl McpRawModeGuard {
    fn enter() -> Result<Self> {
        let enabled_here = !terminal::is_raw_mode_enabled()?;
        if enabled_here {
            terminal::enable_raw_mode()?;
        }
        Ok(Self { enabled_here })
    }
}

impl Drop for McpRawModeGuard {
    fn drop(&mut self) {
        if self.enabled_here {
            let _ = terminal::disable_raw_mode();
        }
    }
}

fn read_tty_line_with_timeout(maximum: usize, timeout: Duration) -> Result<Option<String>> {
    let _raw = McpRawModeGuard::enter()?;
    let deadline = Instant::now() + timeout;
    let mut line = String::new();
    let mut overflow = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || !event::poll(remaining)? {
            return Ok(None);
        }
        match event::read()? {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                match key.code {
                    KeyCode::Enter => {
                        eprintln!();
                        if overflow {
                            bail!("MCP elicitation terminal response 超过 {maximum} 字节限制")
                        }
                        return Ok(Some(line));
                    }
                    KeyCode::Esc => {
                        eprintln!();
                        return Ok(None);
                    }
                    KeyCode::Char('c' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        eprintln!();
                        return Ok(None);
                    }
                    KeyCode::Backspace => {
                        if !overflow {
                            if let Some((index, _)) = line.char_indices().next_back() {
                                line.truncate(index);
                                eprint!("\u{8} \u{8}");
                                io::stderr().flush()?;
                            }
                        }
                    }
                    KeyCode::Char(character)
                        if !character.is_control()
                            && !key.modifiers.intersects(
                                KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER,
                            ) =>
                    {
                        if line.len().saturating_add(character.len_utf8()) <= maximum {
                            line.push(character);
                            eprint!("{character}");
                            io::stderr().flush()?;
                        } else {
                            overflow = true;
                        }
                    }
                    _ => {}
                }
            }
            Event::Paste(text) => {
                let end = text.find(['\r', '\n']).unwrap_or(text.len());
                let fragment = &text[..end];
                if line.len().saturating_add(fragment.len()) <= maximum && !fragment.contains('\0')
                {
                    line.push_str(fragment);
                    eprint!("{}", sanitize_terminal_text(fragment, maximum));
                    io::stderr().flush()?;
                } else {
                    overflow = true;
                }
                if end < text.len() {
                    eprintln!();
                    if overflow {
                        bail!("MCP elicitation terminal response 超过 {maximum} 字节限制")
                    }
                    return Ok(Some(line));
                }
            }
            Event::FocusGained
            | Event::FocusLost
            | Event::Mouse(_)
            | Event::Resize(_, _)
            | Event::Key(_) => {}
        }
    }
}

#[cfg(test)]
fn read_bounded_line(reader: &mut impl BufRead, maximum: usize) -> Result<String> {
    let mut bytes = Vec::new();
    {
        let mut limited = reader.take(maximum.saturating_add(2) as u64);
        limited.read_until(b'\n', &mut bytes)?;
    }
    let terminated = matches!(bytes.last(), Some(b'\n'));
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    if bytes.len() > maximum {
        if !terminated {
            loop {
                let available = reader.fill_buf()?;
                if available.is_empty() {
                    break;
                }
                let consumed = available
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(available.len(), |index| index + 1);
                let found_newline = available.get(consumed.saturating_sub(1)) == Some(&b'\n');
                reader.consume(consumed);
                if found_newline {
                    break;
                }
            }
        }
        bail!("MCP elicitation terminal response 超过 {maximum} 字节限制")
    }
    let line = String::from_utf8(bytes).context("MCP elicitation terminal response 不是 UTF-8")?;
    if line.contains('\0') {
        bail!("MCP elicitation terminal response 包含 NUL")
    }
    Ok(line)
}

fn sanitize_terminal_text(value: &str, maximum: usize) -> String {
    let filtered = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    truncate_text(&filtered, maximum).to_owned()
}

struct ValidatedElicitation {
    interaction_input: Value,
    requested_schema: Option<Value>,
}

fn validate_elicitation_request(server_name: &str, params: &Value) -> Result<ValidatedElicitation> {
    if serde_json::to_vec(params)?.len() > MAX_ELICITATION_REQUEST_BYTES {
        bail!("MCP elicitation request 超过 {MAX_ELICITATION_REQUEST_BYTES} 字节限制")
    }
    let object = params
        .as_object()
        .context("MCP elicitation params 必须是 object")?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= MAX_ELICITATION_MESSAGE_BYTES)
        .context("MCP elicitation message 为空或过长")?;
    let mode = object.get("mode").and_then(Value::as_str).unwrap_or("form");
    let allowed_fields: &[&str] = if mode == "url" {
        &["mode", "message", "elicitationId", "url", "_meta"]
    } else {
        &["mode", "message", "requestedSchema", "_meta"]
    };
    if object
        .keys()
        .any(|key| !allowed_fields.contains(&key.as_str()))
    {
        bail!("MCP elicitation params 包含未知字段")
    }
    let mut interaction = json!({
        "subtype":"elicitation",
        "mcp_server_name":server_name,
        "message":sanitize_text(message, MAX_ELICITATION_MESSAGE_BYTES),
        "mode":mode,
    });
    let requested_schema = match mode {
        "form" => {
            let schema = object
                .get("requestedSchema")
                .context("MCP form elicitation 缺少 requestedSchema")?;
            validate_elicitation_schema(schema)?;
            interaction["requested_schema"] = schema.clone();
            Some(schema.clone())
        }
        "url" => {
            let elicitation_id =
                bounded_required_string(object.get("elicitationId"), "MCP URL elicitationId", 256)?;
            let url_text = bounded_required_string(
                object.get("url"),
                "MCP URL elicitation url",
                MAX_RESOURCE_URI_BYTES,
            )?;
            let url = Url::parse(url_text).context("MCP URL elicitation url 无效")?;
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.host_str().is_none()
            {
                bail!("MCP URL elicitation 只接受无凭据的 http(s) URL")
            }
            interaction["url"] = Value::String(url.into());
            interaction["elicitation_id"] = Value::String(elicitation_id.to_owned());
            None
        }
        _ => bail!("MCP elicitation mode 必须是 form 或 url"),
    };
    Ok(ValidatedElicitation {
        interaction_input: interaction,
        requested_schema,
    })
}

fn validate_elicitation_schema(schema: &Value) -> Result<()> {
    if serde_json::to_vec(schema)?.len() > MAX_ELICITATION_REQUEST_BYTES {
        bail!("MCP elicitation requestedSchema 超过限制")
    }
    let object = schema
        .as_object()
        .context("MCP elicitation requestedSchema 必须是 object")?;
    if object.get("type").and_then(Value::as_str) != Some("object") {
        bail!("MCP elicitation requestedSchema.type 必须是 object")
    }
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .context("MCP elicitation requestedSchema.properties 必须是 object")?;
    if properties.len() > MAX_ELICITATION_FIELDS {
        bail!("MCP elicitation requestedSchema properties 超过 {MAX_ELICITATION_FIELDS} 项限制")
    }
    if object
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.len() > MAX_ELICITATION_FIELDS)
    {
        bail!("MCP elicitation requestedSchema required 超过限制")
    }
    jsonschema::validator_for(schema).context("MCP elicitation requestedSchema 无效")?;
    Ok(())
}

fn validate_elicitation_response(
    response: &Value,
    requested_schema: Option<&Value>,
) -> Result<Value> {
    if serde_json::to_vec(response)?.len() > MAX_ELICITATION_RESPONSE_BYTES {
        bail!("MCP elicitation response 超过 {MAX_ELICITATION_RESPONSE_BYTES} 字节限制")
    }
    let object = response
        .as_object()
        .context("MCP elicitation response 必须是 object")?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "action" | "content"))
    {
        bail!("MCP elicitation response 包含未知字段")
    }
    let action = object
        .get("action")
        .and_then(Value::as_str)
        .context("MCP elicitation response 缺少 action")?;
    if !matches!(action, "accept" | "decline" | "cancel") {
        bail!("MCP elicitation action 必须是 accept/decline/cancel")
    }
    let content = object.get("content");
    if action != "accept" && content.is_some() {
        bail!("MCP elicitation decline/cancel 不得携带 content")
    }
    if action == "accept" && requested_schema.is_some() && content.is_none() {
        bail!("MCP form elicitation accept 必须携带 content")
    }
    if let Some(content) = content {
        if !content.is_object() {
            bail!("MCP elicitation content 必须是 object")
        }
        if let Some(schema) = requested_schema {
            jsonschema::validator_for(schema)
                .context("MCP elicitation requestedSchema 无效")?
                .validate(content)
                .map_err(|error| {
                    anyhow::anyhow!(
                        "MCP elicitation response 不符合 requestedSchema: {}: {}",
                        error.instance_path(),
                        error
                    )
                })?;
        }
    }
    Ok(remove_reserved_metadata(response.clone()))
}

fn bounded_required_string<'a>(
    value: Option<&'a Value>,
    label: &str,
    maximum: usize,
) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= maximum && !value.contains('\0'))
        .with_context(|| format!("{label} 为空、过长或包含 NUL"))
}

impl McpClientRequestHandler {
    fn result(&self, method: &str, params: Option<&Value>) -> Option<Value> {
        match method {
            "ping" => Some(json!({})),
            "roots/list" => Some(json!({
                "roots": self.roots
                    .iter()
                    .map(|root| json!({"uri": root.uri, "name": root.name}))
                    .collect::<Vec<_>>()
            })),
            "elicitation/create" => Some(self.elicitation.respond(&self.server_name, params)),
            _ => None,
        }
    }
}

#[cfg(test)]
fn mcp_client_request_result(method: &str, roots: &[McpRoot]) -> Option<Value> {
    match method {
        "ping" => Some(json!({})),
        "roots/list" => Some(json!({
            "roots": roots
                .iter()
                .map(|root| json!({"uri": root.uri, "name": root.name}))
                .collect::<Vec<_>>()
        })),
        _ => None,
    }
}

fn mcp_server_request_handler(handler: McpClientRequestHandler) -> RpcServerRequestHandler {
    Arc::new(move |method, params| handler.result(method, params))
}

async fn http_notification_loop(
    config: HttpNotificationConfig,
    events: broadcast::Sender<Value>,
    closing: Arc<AtomicBool>,
) {
    for attempt in 0..3u64 {
        if closing.load(Ordering::Acquire) {
            return;
        }
        let client = match secure_client_for_url(&config.url, config.allow_private_network).await {
            Ok(client) => client,
            Err(_) => return,
        };
        let (authorized, dynamic_secret) =
            match authorized_headers(&config.headers, config.credential.as_ref()).await {
                Ok(value) => value,
                Err(_) => return,
            };
        let request_secrets = request_secrets(&config.secrets, dynamic_secret);
        let response = client
            .get(config.url.clone())
            .headers(authorized)
            .header("accept", "text/event-stream")
            .header("mcp-session-id", &config.session)
            .header(
                "mcp-protocol-version",
                config.protocol_version.lock().await.as_str(),
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
                    let is_server_request = value.get("jsonrpc").and_then(Value::as_str)
                        == Some("2.0")
                        && value.get("method").and_then(Value::as_str).is_some()
                        && value.get("id").is_some();
                    if is_server_request {
                        respond_to_http_server_request(&config, &value).await;
                        continue;
                    }
                    redact_json_secrets(&mut value, &request_secrets);
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

async fn respond_to_http_server_request(config: &HttpNotificationConfig, request: &Value) {
    let Some(id) = request.get("id") else {
        return;
    };
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let handler = McpClientRequestHandler {
        server_name: config.server_name.clone(),
        roots: config.roots.clone(),
        elicitation: Arc::clone(&config.elicitation),
    };
    let response = handler.result(method, request.get("params")).map_or_else(
        || {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {"code": -32601, "message": "Client method not supported"}
            })
        },
        |result| json!({"jsonrpc": "2.0", "id": id, "result": result}),
    );
    let Ok(body) = serde_json::to_vec(&response) else {
        return;
    };
    let Ok(client) = secure_client_for_url(&config.url, config.allow_private_network).await else {
        return;
    };
    let Ok((authorized, _)) = authorized_headers(&config.headers, config.credential.as_ref()).await
    else {
        return;
    };
    let version = config.protocol_version.lock().await.clone();
    let _ = timeout(
        Duration::from_secs(5),
        client
            .post(config.url.clone())
            .headers(authorized)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-session-id", &config.session)
            .header("mcp-protocol-version", version)
            .body(body)
            .send(),
    )
    .await;
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
        let elicitation = Arc::new(ElicitationBridge::new(config.elicitation_timeout));
        let request_handler = McpClientRequestHandler {
            server_name: config.name.clone(),
            roots: config.roots.clone(),
            elicitation: Arc::clone(&elicitation),
        };
        let rpc: Arc<dyn McpRpc> = match &config.transport {
            ServerTransport::Stdio {
                command,
                args,
                env,
                cwd,
            } => Arc::new(
                StdioRpcClient::spawn_with_secret_env_scrubber(
                    StdioRpcConfig {
                        label: format!("MCP/{}", config.name),
                        command: command.clone(),
                        args: args.clone(),
                        env: env.clone(),
                        cwd: cwd.clone(),
                        framing: RpcFraming::Newline,
                        request_timeout: config.request_timeout,
                        server_request_handler: Some(mcp_server_request_handler(
                            request_handler.clone(),
                        )),
                    },
                    config.secret_env_scrubber.clone(),
                )
                .await?,
            ),
            ServerTransport::Http {
                url,
                headers,
                secrets,
                allow_private_network,
                credential,
                legacy_sse,
            } => {
                if *legacy_sse {
                    Arc::new(
                        LegacySseMcpRpc::connect(
                            url.clone(),
                            headers.as_ref().clone(),
                            secrets.clone(),
                            credential.clone(),
                            *allow_private_network,
                            config.request_timeout,
                            request_handler.clone(),
                        )
                        .await?,
                    )
                } else {
                    Arc::new(HttpMcpRpc::new(HttpMcpConfig {
                        server_name: config.name.clone(),
                        url: url.clone(),
                        headers: headers.as_ref().clone(),
                        secrets: secrets.clone(),
                        credential: credential.clone(),
                        allow_private_network: *allow_private_network,
                        request_timeout: config.request_timeout,
                        roots: config.roots.clone(),
                        elicitation: Arc::clone(&elicitation),
                    }))
                }
            }
            ServerTransport::WebSocket {
                url,
                headers,
                secrets,
                allow_private_network,
                credential,
            } => Arc::new(
                WebSocketMcpRpc::connect(WebSocketMcpConfig {
                    label: format!("MCP/{}", config.name),
                    url: url.clone(),
                    headers: headers.as_ref().clone(),
                    configured_secrets: secrets.clone(),
                    credential: credential.clone(),
                    allow_private_network: *allow_private_network,
                    request_timeout: config.request_timeout,
                    server_request_handler: mcp_server_request_handler(request_handler.clone()),
                })
                .await?,
            ),
        };
        let initialize = match rpc
            .request(
                "initialize",
                Some(json!({
                    "protocolVersion": CURRENT_PROTOCOL_VERSION,
                    "capabilities": {
                        "roots": {"listChanged": false},
                        "elicitation": {}
                    },
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
            elicitation,
            call_lock: Arc::new(Mutex::new(())),
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

    async fn list_tools(
        &self,
        resource_handles: Arc<Mutex<ResourceHandleStore>>,
    ) -> Result<Vec<Arc<dyn Tool>>> {
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
            elicitation: Arc::clone(&self.elicitation),
            call_lock: Arc::clone(&self.call_lock),
            resource_handles,
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

    async fn list_paginated_with_timeout(
        &self,
        method: &str,
        field: &str,
        maximum: usize,
        request_timeout: Duration,
    ) -> Result<Vec<Value>> {
        let deadline = Instant::now() + request_timeout;
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut collected = Vec::new();
        for _ in 0..MAX_LIST_PAGES {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!("MCP server {} {method} 超过 hook timeout", self.name)
            }
            let params = cursor.as_ref().map(|cursor| json!({"cursor": cursor}));
            let result = self
                .rpc
                .request_with_timeout(method, params, remaining)
                .await?;
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
    elicitation: Arc<ElicitationBridge>,
    call_lock: Arc<Mutex<()>>,
    resource_handles: Arc<Mutex<ResourceHandleStore>>,
}

impl McpClientHandle {
    async fn call_tool(
        &self,
        context: &ToolContext,
        name: &str,
        arguments: Value,
    ) -> Result<ToolOutput> {
        let _call = self.call_lock.lock().await;
        let _elicitation = self.elicitation.activate(context.clone())?;
        let result = self
            .rpc
            .request(
                "tools/call",
                Some(json!({"name": name, "arguments": arguments})),
            )
            .await?;
        map_tool_call_result_with_handles(result, &self.name, &self.resource_handles).await
    }
}

#[derive(Default)]
struct ToolResultPreview {
    text: String,
    truncated: bool,
}

impl ToolResultPreview {
    fn push_text(&mut self, value: &str) {
        if value.is_empty() || self.truncated {
            return;
        }
        self.push_separator();
        const MARKER: &str =
            "\n[MCP text preview truncated; complete content was sent to the model]";
        let available = MAX_TOOL_PREVIEW_BYTES.saturating_sub(self.text.len());
        if value.len() <= available {
            self.text.push_str(value);
            return;
        }
        let body_limit = available.saturating_sub(MARKER.len());
        let mut end = body_limit.min(value.len());
        while !value.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        self.text.push_str(&value[..end]);
        if self.text.len().saturating_add(MARKER.len()) <= MAX_TOOL_PREVIEW_BYTES {
            self.text.push_str(MARKER);
        }
        self.truncated = true;
    }

    fn push_summary(&mut self, value: &str) {
        if self.truncated || value.is_empty() {
            return;
        }
        let separator = usize::from(!self.text.is_empty());
        if self
            .text
            .len()
            .saturating_add(separator)
            .saturating_add(value.len())
            > MAX_TOOL_PREVIEW_BYTES
        {
            return;
        }
        self.push_separator();
        self.text.push_str(value);
    }

    fn push_separator(&mut self) {
        if !self.text.is_empty() {
            self.text.push('\n');
        }
    }

    fn finish(self, is_error: bool) -> String {
        if self.text.is_empty() {
            if is_error {
                "MCP tool reported an error without text content".to_owned()
            } else {
                "MCP tool returned no text content".to_owned()
            }
        } else {
            self.text
        }
    }
}

async fn map_tool_call_result_with_handles(
    result: Value,
    server: &str,
    resource_handles: &Arc<Mutex<ResourceHandleStore>>,
) -> Result<ToolOutput> {
    let links = collect_resource_link_uris(&result)?;
    let handles = resource_handles
        .lock()
        .await
        .insert_resource_links(server, links)?;
    map_tool_call_result_inner(result, Some(&handles))
}

fn collect_resource_link_uris(result: &Value) -> Result<Vec<String>> {
    let content = result
        .get("content")
        .and_then(Value::as_array)
        .context("MCP tools/call result 缺少 content array")?;
    if content.len() > MAX_TOOL_CONTENT_BLOCKS {
        bail!("MCP tools/call content 超过 {MAX_TOOL_CONTENT_BLOCKS} 个 block 限制")
    }
    let mut links = Vec::new();
    for (index, block) in content.iter().enumerate() {
        let Some(object) = block.as_object() else {
            continue;
        };
        if object.get("type").and_then(Value::as_str) != Some("resource_link") {
            continue;
        }
        let uri = bounded_required_string(
            object.get("uri"),
            &format!("MCP tools/call content[{index}].resource_link.uri"),
            MAX_RESOURCE_URI_BYTES,
        )?;
        safe_resource_uri_metadata(uri).context("MCP resource_link.uri 不是安全的绝对 URI")?;
        links.push(uri.to_owned());
    }
    Ok(links)
}

#[cfg(test)]
fn map_tool_call_result(result: Value) -> Result<ToolOutput> {
    map_tool_call_result_inner(result, None)
}

fn map_tool_call_result_inner(
    result: Value,
    resource_link_handles: Option<&[String]>,
) -> Result<ToolOutput> {
    let encoded_len = serde_json::to_vec(&result)?.len();
    if encoded_len > MAX_TOOL_RESULT_BYTES {
        bail!("MCP tools/call result 超过 {MAX_TOOL_RESULT_BYTES} 字节限制")
    }
    let result = remove_reserved_metadata(result);
    let object = result
        .as_object()
        .context("MCP tools/call result 必须是 object")?;
    let is_error = match object.get("isError") {
        Some(value) => value
            .as_bool()
            .context("MCP tools/call isError 必须是 boolean")?,
        None => false,
    };
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .context("MCP tools/call result 缺少 content array")?;
    if content.len() > MAX_TOOL_CONTENT_BLOCKS {
        bail!("MCP tools/call content 超过 {MAX_TOOL_CONTENT_BLOCKS} 个 block 限制")
    }

    let mut preview = ToolResultPreview::default();
    let mut model_blocks = Vec::with_capacity(content.len().saturating_add(1));
    let mut media_bytes = 0usize;
    let mut resource_link_index = 0usize;
    for (index, block) in content.iter().enumerate() {
        let resource_handle = if block.get("type").and_then(Value::as_str) == Some("resource_link")
        {
            let handle = resource_link_handles
                .map(|handles| {
                    handles
                        .get(resource_link_index)
                        .context("MCP resource_link handle count 不一致")
                })
                .transpose()?;
            resource_link_index = resource_link_index.saturating_add(1);
            handle.map(String::as_str)
        } else {
            None
        };
        map_tool_content_block(
            block,
            index,
            &mut preview,
            &mut model_blocks,
            &mut media_bytes,
            resource_handle,
        )?;
    }
    if resource_link_handles.is_some_and(|handles| resource_link_index != handles.len()) {
        bail!("MCP resource_link handle count 不一致")
    }

    if let Some(structured) = object.get("structuredContent") {
        if !structured.is_object() {
            bail!("MCP tools/call structuredContent 必须是 object")
        }
        // Keep structured JSON complete. It is intentionally omitted from the bounded
        // terminal preview so that the preview can never contain a half-truncated JSON value.
        let rendered = serde_json::to_string(structured)?;
        model_blocks.push(json!({
            "type":"text",
            "text":format!("MCP structured content:\n{rendered}")
        }));
        preview.push_summary(&format!(
            "[MCP structured content: {} bytes; available to the model in full]",
            rendered.len()
        ));
    }

    if model_blocks.is_empty() {
        model_blocks.push(json!({"type":"text", "text":"MCP tool returned no content"}));
    }
    let content = preview.finish(is_error);
    let mut output = ToolOutput::success_with_model_content(content, Value::Array(model_blocks));
    output.is_error = is_error;
    Ok(output)
}

fn map_tool_content_block(
    block: &Value,
    index: usize,
    preview: &mut ToolResultPreview,
    model_blocks: &mut Vec<Value>,
    media_bytes: &mut usize,
    resource_handle: Option<&str>,
) -> Result<()> {
    let object = block
        .as_object()
        .with_context(|| format!("MCP tools/call content[{index}] 必须是 object"))?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .with_context(|| format!("MCP tools/call content[{index}] 缺少 type"))?;
    match kind {
        "text" => {
            let text = object
                .get("text")
                .and_then(Value::as_str)
                .with_context(|| format!("MCP tools/call content[{index}].text 必须是 string"))?;
            let text = sanitize_text(text, MAX_TOOL_RESULT_BYTES);
            preview.push_text(&text);
            model_blocks.push(json!({"type":"text", "text":text}));
        }
        "image" => {
            let media_type = required_content_string(object, "mimeType", index)?;
            let data = required_content_string(object, "data", index)?;
            let raw_len = validate_tool_media(media_type, data, media_bytes, "image")?;
            preview.push_summary(&format!("[MCP image: {media_type}, {raw_len} bytes]"));
            model_blocks.push(json!({
                "type":"image",
                "source":{"type":"base64", "media_type":media_type, "data":data}
            }));
        }
        "resource" => {
            map_embedded_resource(object, index, preview, model_blocks, media_bytes)?;
        }
        "resource_link" => {
            map_resource_link(object, index, preview, model_blocks, resource_handle)?;
        }
        "audio" => {
            let media_type = required_content_string(object, "mimeType", index)?;
            let data = required_content_string(object, "data", index)?;
            let raw_len = validate_opaque_media(media_type, data, media_bytes, "audio", true)?;
            let summary = media_metadata_text("audio", None, None, media_type, raw_len);
            preview.push_summary(&summary);
            model_blocks.push(json!({"type":"text", "text":summary}));
        }
        other => bail!("MCP tools/call content[{index}] type {other:?} 不受支持"),
    }
    Ok(())
}

fn required_content_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    index: usize,
) -> Result<&'a str> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("MCP tools/call content[{index}].{field} 必须是非空 string"))
}

fn map_embedded_resource(
    block: &serde_json::Map<String, Value>,
    index: usize,
    preview: &mut ToolResultPreview,
    model_blocks: &mut Vec<Value>,
    media_bytes: &mut usize,
) -> Result<()> {
    let resource = block
        .get("resource")
        .and_then(Value::as_object)
        .with_context(|| format!("MCP tools/call content[{index}].resource 必须是 object"))?;
    let uri = resource
        .get("uri")
        .and_then(Value::as_str)
        .filter(|uri| !uri.is_empty() && uri.len() <= MAX_RESOURCE_URI_BYTES)
        .with_context(|| {
            format!(
                "MCP tools/call content[{index}].resource.uri 为空或超过 {MAX_RESOURCE_URI_BYTES} 字节限制"
            )
        })?;
    let safe_uri =
        safe_resource_uri_metadata(uri).context("MCP embedded resource.uri 不是安全的绝对 URI")?;
    let text = match resource.get("text") {
        Some(value) => Some(value.as_str().with_context(|| {
            format!("MCP tools/call content[{index}].resource.text 必须是 string")
        })?),
        None => None,
    };
    let blob = match resource.get("blob") {
        Some(value) => Some(value.as_str().with_context(|| {
            format!("MCP tools/call content[{index}].resource.blob 必须是 string")
        })?),
        None => None,
    };
    match (text, blob) {
        (Some(text), None) => {
            let text = sanitize_text(text, MAX_TOOL_RESULT_BYTES);
            preview.push_text(&text);
            model_blocks.push(json!({"type":"text", "text":text}));
        }
        (None, Some(blob)) => {
            let media_type = resource
                .get("mimeType")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .with_context(|| {
                    format!("MCP tools/call content[{index}].resource blob 缺少 mimeType string")
                })?;
            if is_native_media_type(media_type) {
                let raw_len = validate_tool_media(media_type, blob, media_bytes, "resource")?;
                if media_type == "application/pdf" {
                    preview.push_summary(&format!("[MCP PDF resource: {raw_len} bytes]"));
                    model_blocks.push(json!({
                        "type":"document",
                        "title":"mcp-resource.pdf",
                        "source":{"type":"base64", "media_type":media_type, "data":blob}
                    }));
                } else {
                    preview.push_summary(&format!(
                        "[MCP image resource: {media_type}, {raw_len} bytes]"
                    ));
                    model_blocks.push(json!({
                        "type":"image",
                        "source":{"type":"base64", "media_type":media_type, "data":blob}
                    }));
                }
            } else {
                let raw_len = validate_opaque_media(
                    media_type,
                    blob,
                    media_bytes,
                    "resource",
                    media_type.starts_with("audio/"),
                )?;
                let summary = media_metadata_text(
                    if media_type.starts_with("audio/") {
                        "audio resource"
                    } else {
                        "binary resource"
                    },
                    Some(&safe_uri),
                    None,
                    media_type,
                    raw_len,
                );
                preview.push_summary(&summary);
                model_blocks.push(json!({"type":"text", "text":summary}));
            }
        }
        (Some(_), Some(_)) => {
            bail!("MCP tools/call content[{index}].resource 不得同时包含 text 和 blob")
        }
        (None, None) => bail!("MCP tools/call content[{index}].resource 必须包含 text 或 blob"),
    }
    Ok(())
}

fn map_resource_link(
    block: &serde_json::Map<String, Value>,
    index: usize,
    preview: &mut ToolResultPreview,
    model_blocks: &mut Vec<Value>,
    resource_handle: Option<&str>,
) -> Result<()> {
    let uri = bounded_required_string(
        block.get("uri"),
        &format!("MCP tools/call content[{index}].resource_link.uri"),
        MAX_RESOURCE_URI_BYTES,
    )?;
    let safe_uri =
        safe_resource_uri_metadata(uri).context("MCP resource_link.uri 不是安全的绝对 URI")?;
    let name = bounded_required_string(
        block.get("name"),
        &format!("MCP tools/call content[{index}].resource_link.name"),
        1024,
    )?;
    let description = block
        .get("description")
        .map(|value| {
            value
                .as_str()
                .filter(|value| value.len() <= MAX_DESCRIPTION_BYTES)
                .context("MCP resource_link.description 必须是 bounded string")
        })
        .transpose()?;
    let media_type = block
        .get("mimeType")
        .map(|value| {
            let value = value
                .as_str()
                .context("MCP resource_link.mimeType 必须是 string")?;
            validate_mime_type(value)?;
            Ok::<_, anyhow::Error>(value)
        })
        .transpose()?;
    let size = block
        .get("size")
        .map(|value| {
            value
                .as_u64()
                .context("MCP resource_link.size 必须是非负 integer")
        })
        .transpose()?;
    let mut metadata = json!({
        "kind":"resource_link",
        "name":sanitize_text(name, 1024),
        "resource":safe_uri,
        "description":description.map(|value| sanitize_text(value, MAX_DESCRIPTION_BYTES)),
        "mime_type":media_type,
        "size":size,
    });
    if let Some(handle) = resource_handle {
        metadata["uri"] = Value::String(handle.to_owned());
    }
    let text = format!("[MCP resource link] {}", serde_json::to_string(&metadata)?);
    preview.push_summary(&text);
    model_blocks.push(json!({"type":"text", "text":text}));
    Ok(())
}

fn validate_tool_media(
    media_type: &str,
    encoded: &str,
    total_bytes: &mut usize,
    source: &str,
) -> Result<usize> {
    validate_mime_type(media_type)?;
    let bytes = decode_bounded_media(encoded, total_bytes, source)?;

    let detected = detect_tool_media_type(&bytes);
    match media_type {
        "image/png" | "image/jpeg" | "image/gif" | "image/webp" => {
            if detected != Some(media_type) {
                bail!("MCP {source} 内容签名与声明的 MIME {media_type:?} 不一致")
            }
        }
        "application/pdf" => {
            if detected != Some("application/pdf") {
                bail!("MCP {source} 内容缺少有效 PDF 签名")
            }
        }
        _ => bail!("MCP {source} binary MIME {media_type:?} 不受支持"),
    }
    Ok(bytes.len())
}

fn validate_opaque_media(
    media_type: &str,
    encoded: &str,
    total_bytes: &mut usize,
    source: &str,
    require_audio: bool,
) -> Result<usize> {
    validate_mime_type(media_type)?;
    if require_audio && !media_type.starts_with("audio/") {
        bail!("MCP {source} content 必须声明 audio/* MIME")
    }
    if !require_audio && is_native_media_type(media_type) {
        bail!("MCP {source} native media 必须走 image/PDF validation")
    }
    let bytes = decode_bounded_media(encoded, total_bytes, source)?;
    if media_type.starts_with("audio/") && detect_audio_media_type(&bytes) != Some(media_type) {
        bail!("MCP {source} 内容签名与声明的 audio MIME {media_type:?} 不一致")
    }
    Ok(bytes.len())
}

fn validate_mime_type(media_type: &str) -> Result<()> {
    if media_type.is_empty()
        || media_type.len() > 128
        || media_type.matches('/').count() != 1
        || media_type.bytes().any(|byte| {
            byte.is_ascii_uppercase()
                || !(byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-' | b'/'
                    ))
        })
    {
        bail!("MCP MIME type 为空、过长或语法无效")
    }
    Ok(())
}

fn decode_bounded_media(encoded: &str, total_bytes: &mut usize, source: &str) -> Result<Vec<u8>> {
    if encoded.is_empty() || encoded.len() > MAX_TOOL_MEDIA_BASE64_BYTES {
        bail!("MCP {source} base64 为空或超过 {MAX_TOOL_MEDIA_BASE64_BYTES} 字节限制")
    }
    let bytes = BASE64
        .decode(encoded)
        .with_context(|| format!("MCP {source} 包含无效 base64"))?;
    if BASE64.encode(&bytes) != encoded {
        bail!("MCP {source} base64 不是规范的 RFC 4648 编码")
    }
    if bytes.is_empty() {
        bail!("MCP {source} 解码后为空")
    }
    let updated_total = total_bytes
        .checked_add(bytes.len())
        .context("MCP media 大小累计溢出")?;
    if updated_total > MAX_TOOL_MEDIA_RAW_BYTES {
        bail!("MCP media 解码后累计超过 {MAX_TOOL_MEDIA_RAW_BYTES} 字节限制")
    }
    *total_bytes = updated_total;
    Ok(bytes)
}

fn is_native_media_type(media_type: &str) -> bool {
    matches!(
        media_type,
        "image/png" | "image/jpeg" | "image/gif" | "image/webp" | "application/pdf"
    )
}

fn detect_audio_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WAVE" {
        Some("audio/wav")
    } else if bytes.starts_with(b"ID3")
        || bytes
            .get(..2)
            .is_some_and(|prefix| prefix[0] == 0xff && prefix[1] & 0xe0 == 0xe0)
    {
        Some("audio/mpeg")
    } else if bytes.starts_with(b"OggS") {
        Some("audio/ogg")
    } else if bytes.starts_with(b"fLaC") {
        Some("audio/flac")
    } else if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        Some("audio/mp4")
    } else {
        None
    }
}

fn media_metadata_text(
    kind: &str,
    resource: Option<&Value>,
    name: Option<&str>,
    media_type: &str,
    raw_len: usize,
) -> String {
    format!(
        "[MCP {kind}; binary bytes intentionally omitted] {}",
        json!({
            "kind":kind,
            "resource":resource,
            "name":name,
            "mime_type":media_type,
            "bytes":raw_len,
        })
    )
}

fn detect_tool_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else if bytes.starts_with(b"%PDF-") {
        Some("application/pdf")
    } else {
        None
    }
}

fn safe_resource_uri_metadata(uri: &str) -> Result<Value> {
    if uri.is_empty() || uri.len() > MAX_RESOURCE_URI_BYTES || uri.contains('\0') {
        bail!("MCP resource URI 为空、过长或包含 NUL")
    }
    let url = Url::parse(uri).context("MCP resource URI 不是有效绝对 URI")?;
    if !url.username().is_empty() || url.password().is_some() {
        bail!("MCP resource URI 不得包含 userinfo")
    }
    match url.scheme() {
        "file" => Ok(json!({
            "kind":"local_resource",
            "scheme":"file"
        })),
        "http" | "https" => {
            if url.host_str().is_none() {
                bail!("MCP http(s) resource URI 缺少 host")
            }
            Ok(json!({
                "kind":"network_resource",
                "origin":url.origin().ascii_serialization(),
                "scheme":url.scheme()
            }))
        }
        scheme => Ok(json!({
            "kind":"resource",
            "scheme":scheme
        })),
    }
}

impl ResourceHandleStore {
    fn replace_server_kind(
        &mut self,
        server: &str,
        kind: ResourceHandleKind,
        values: Vec<(String, Vec<String>)>,
    ) -> Result<Vec<String>> {
        let retained_bytes = self
            .entries
            .values()
            .filter(|entry| entry.server != server || entry.kind != kind)
            .try_fold(0usize, |total, entry| total.checked_add(entry.raw.len()))
            .context("MCP resource handle byte count 溢出")?;
        let retained_count = self
            .entries
            .values()
            .filter(|entry| entry.server != server || entry.kind != kind)
            .count();
        let added_bytes = values
            .iter()
            .try_fold(0usize, |total, (raw, _)| total.checked_add(raw.len()))
            .context("MCP resource handle byte count 溢出")?;
        let total_count = retained_count
            .checked_add(values.len())
            .context("MCP resource handle count 溢出")?;
        let total_bytes = retained_bytes
            .checked_add(added_bytes)
            .context("MCP resource handle byte count 溢出")?;
        if total_count > MAX_RESOURCE_HANDLES || total_bytes > MAX_RESOURCE_HANDLE_BYTES {
            bail!(
                "MCP resource handle registry 超过 {} 项或 {} 字节限制",
                MAX_RESOURCE_HANDLES,
                MAX_RESOURCE_HANDLE_BYTES
            )
        }

        self.entries
            .retain(|_, entry| entry.server != server || entry.kind != kind);
        self.raw_bytes = retained_bytes;
        let mut handles = Vec::with_capacity(values.len());
        for (raw, variables) in values {
            let prefix = match kind {
                ResourceHandleKind::Direct | ResourceHandleKind::Linked => "mcp-resource",
                ResourceHandleKind::Template => "mcp-template",
            };
            let handle = loop {
                let candidate = format!("{prefix}:{}", uuid::Uuid::new_v4());
                if !self.entries.contains_key(&candidate) {
                    break candidate;
                }
            };
            self.raw_bytes = self
                .raw_bytes
                .checked_add(raw.len())
                .context("MCP resource handle byte count 溢出")?;
            self.entries.insert(
                handle.clone(),
                ResourceHandleEntry {
                    server: server.to_owned(),
                    raw,
                    kind,
                    variables,
                },
            );
            handles.push(handle);
        }
        Ok(handles)
    }

    fn insert_resource_links(&mut self, server: &str, values: Vec<String>) -> Result<Vec<String>> {
        let mut handles = Vec::with_capacity(values.len());
        for raw in values {
            if let Some((handle, _)) = self.entries.iter().find(|(_, entry)| {
                entry.server == server
                    && entry.raw == raw
                    && matches!(
                        entry.kind,
                        ResourceHandleKind::Direct | ResourceHandleKind::Linked
                    )
            }) {
                handles.push(handle.clone());
                continue;
            }
            while self.entries.len() >= MAX_RESOURCE_HANDLES
                || self.raw_bytes.saturating_add(raw.len()) > MAX_RESOURCE_HANDLE_BYTES
            {
                let stale = self
                    .entries
                    .iter()
                    .find(|(_, entry)| entry.kind == ResourceHandleKind::Linked)
                    .map(|(handle, _)| handle.clone())
                    .context("MCP resource handle registry 已满，且没有可回收的 link handle")?;
                if let Some(removed) = self.entries.remove(&stale) {
                    self.raw_bytes = self.raw_bytes.saturating_sub(removed.raw.len());
                }
            }
            let handle = loop {
                let candidate = format!("mcp-resource:{}", uuid::Uuid::new_v4());
                if !self.entries.contains_key(&candidate) {
                    break candidate;
                }
            };
            self.raw_bytes = self
                .raw_bytes
                .checked_add(raw.len())
                .context("MCP resource handle byte count 溢出")?;
            self.entries.insert(
                handle.clone(),
                ResourceHandleEntry {
                    server: server.to_owned(),
                    raw,
                    kind: ResourceHandleKind::Linked,
                    variables: Vec::new(),
                },
            );
            handles.push(handle);
        }
        Ok(handles)
    }

    fn resolve(
        &self,
        server: &str,
        value: &str,
        arguments: Option<&serde_json::Map<String, Value>>,
    ) -> Result<String> {
        if let Some(entry) = self.entries.get(value) {
            if entry.server != server {
                bail!("MCP resource handle 不属于所选 server")
            }
            return match entry.kind {
                ResourceHandleKind::Direct | ResourceHandleKind::Linked => {
                    if arguments.is_some_and(|arguments| !arguments.is_empty()) {
                        bail!("direct MCP resource handle 不接受 template arguments")
                    }
                    Ok(entry.raw.clone())
                }
                ResourceHandleKind::Template => {
                    let arguments = arguments.context("MCP template handle 缺少 arguments")?;
                    expand_uri_template(&entry.raw, &entry.variables, arguments)
                }
            };
        }
        if value.starts_with("mcp-resource:") || value.starts_with("mcp-template:") {
            bail!("MCP resource handle 已失效或不存在；请重新列出 resources")
        }
        if arguments.is_some_and(|arguments| !arguments.is_empty()) {
            bail!("显式 MCP resource URI 不接受 template arguments")
        }
        safe_resource_uri_metadata(value)?;
        Ok(value.to_owned())
    }
}

#[derive(Clone, Debug)]
struct UriTemplateVariable {
    name: String,
    prefix: Option<usize>,
}

#[derive(Clone, Debug)]
enum UriTemplatePart {
    Literal(String),
    Expression {
        operator: Option<char>,
        variables: Vec<UriTemplateVariable>,
    },
}

fn parse_uri_template(template: &str) -> Result<(Vec<UriTemplatePart>, Vec<String>)> {
    if template.is_empty() || template.len() > MAX_RESOURCE_URI_BYTES || template.contains('\0') {
        bail!("MCP resource template 为空、过长或包含 NUL")
    }
    let mut parts = Vec::new();
    let mut variables = Vec::new();
    let mut cursor = 0usize;
    while cursor < template.len() {
        let remainder = &template[cursor..];
        let opening = remainder.find('{');
        let stray_closing = remainder.find('}');
        if stray_closing.is_some_and(|closing| opening.is_none_or(|opening| closing < opening)) {
            bail!("MCP resource template 包含未配对的 }}")
        }
        let Some(opening) = opening else {
            parts.push(UriTemplatePart::Literal(remainder.to_owned()));
            break;
        };
        let opening = cursor + opening;
        if opening > cursor {
            parts.push(UriTemplatePart::Literal(
                template[cursor..opening].to_owned(),
            ));
        }
        let tail = &template[opening + 1..];
        let closing = tail.find('}').context("MCP resource template 缺少 }")?;
        if tail[..closing].contains('{') {
            bail!("MCP resource template 不得嵌套 expression")
        }
        let expression = &tail[..closing];
        if expression.is_empty() {
            bail!("MCP resource template expression 为空")
        }
        let first = expression
            .chars()
            .next()
            .context("空 template expression")?;
        let (operator, variable_source) =
            if matches!(first, '+' | '#' | '.' | '/' | ';' | '?' | '&') {
                (Some(first), &expression[first.len_utf8()..])
            } else {
                (None, expression)
            };
        if variable_source.is_empty() {
            bail!("MCP resource template expression 缺少变量")
        }
        let mut expression_variables = Vec::new();
        for raw_variable in variable_source.split(',') {
            if raw_variable.is_empty() || raw_variable.ends_with('*') {
                bail!("MCP resource template 仅支持有界 scalar 变量")
            }
            let (name, prefix) = match raw_variable.split_once(':') {
                Some((name, prefix)) => {
                    let prefix = prefix
                        .parse::<usize>()
                        .context("MCP resource template prefix 必须是整数")?;
                    if prefix == 0 || prefix > MAX_RESOURCE_TEMPLATE_ARGUMENT_BYTES {
                        bail!("MCP resource template prefix 超过限制")
                    }
                    (name, Some(prefix))
                }
                None => (raw_variable, None),
            };
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
            {
                bail!("MCP resource template 变量名无效")
            }
            if !variables.iter().any(|existing| existing == name) {
                if variables.len() >= MAX_RESOURCE_TEMPLATE_VARIABLES {
                    bail!(
                        "MCP resource template 变量超过 {} 个限制",
                        MAX_RESOURCE_TEMPLATE_VARIABLES
                    )
                }
                variables.push(name.to_owned());
            }
            expression_variables.push(UriTemplateVariable {
                name: name.to_owned(),
                prefix,
            });
        }
        parts.push(UriTemplatePart::Expression {
            operator,
            variables: expression_variables,
        });
        cursor = opening + 1 + closing + 1;
    }
    if variables.is_empty() {
        bail!("MCP resource template 不包含变量")
    }
    Ok((parts, variables))
}

fn encode_uri_template_value(value: &str, allow_reserved: bool) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        let unreserved = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~');
        let reserved = matches!(
            byte,
            b':' | b'/'
                | b'?'
                | b'#'
                | b'['
                | b']'
                | b'@'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        );
        if unreserved || (allow_reserved && reserved) {
            output.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(output, "%{byte:02X}");
        }
    }
    output
}

fn expand_uri_template(
    template: &str,
    expected_variables: &[String],
    arguments: &serde_json::Map<String, Value>,
) -> Result<String> {
    let (parts, parsed_variables) = parse_uri_template(template)?;
    if parsed_variables != expected_variables {
        bail!("MCP resource template registry 与 template 不一致")
    }
    if arguments.len() != expected_variables.len()
        || arguments
            .keys()
            .any(|key| !expected_variables.iter().any(|expected| expected == key))
    {
        bail!("MCP template arguments 必须与列出的变量完全一致")
    }
    let mut output = String::new();
    for part in parts {
        match part {
            UriTemplatePart::Literal(literal) => output.push_str(&literal),
            UriTemplatePart::Expression {
                operator,
                variables,
            } => {
                let (prefix, separator, named, allow_reserved) = match operator {
                    None => ("", ",", false, false),
                    Some('+') => ("", ",", false, true),
                    Some('#') => ("#", ",", false, true),
                    Some('.') => (".", ".", false, false),
                    Some('/') => ("/", "/", false, false),
                    Some(';') => (";", ";", true, false),
                    Some('?') => ("?", "&", true, false),
                    Some('&') => ("&", "&", true, false),
                    Some(_) => unreachable!("template parser rejects unknown operators"),
                };
                output.push_str(prefix);
                for (index, variable) in variables.iter().enumerate() {
                    if index > 0 {
                        output.push_str(separator);
                    }
                    let raw = arguments
                        .get(&variable.name)
                        .and_then(Value::as_str)
                        .with_context(|| {
                            format!("MCP template argument {} 必须是 string", variable.name)
                        })?;
                    if raw.len() > MAX_RESOURCE_TEMPLATE_ARGUMENT_BYTES {
                        bail!("MCP template argument {} 超过限制", variable.name)
                    }
                    let raw = variable
                        .prefix
                        .map(|limit| raw.chars().take(limit).collect::<String>())
                        .unwrap_or_else(|| raw.to_owned());
                    let encoded = encode_uri_template_value(&raw, allow_reserved);
                    if named {
                        output.push_str(&variable.name);
                        if operator != Some(';') || !encoded.is_empty() {
                            output.push('=');
                            output.push_str(&encoded);
                        }
                    } else {
                        output.push_str(&encoded);
                    }
                }
            }
        }
        if output.len() > MAX_RESOURCE_URI_BYTES {
            bail!("expanded MCP resource URI 超过限制")
        }
    }
    safe_resource_uri_metadata(&output)?;
    Ok(output)
}

fn resource_template_metadata(template: &str) -> Result<(Vec<String>, Value)> {
    let (_, variables) = parse_uri_template(template)?;
    let arguments = variables
        .iter()
        .map(|name| (name.clone(), Value::String("x".to_owned())))
        .collect::<serde_json::Map<_, _>>();
    let expanded = expand_uri_template(template, &variables, &arguments)?;
    Ok((variables, safe_resource_uri_metadata(&expanded)?))
}

fn sanitize_external_resource_uris(mut value: Value) -> Result<Value> {
    match &mut value {
        Value::Object(object) => {
            if let Some(uri) = object.remove("uri") {
                let uri = uri.as_str().context("MCP external uri 必须是 string")?;
                object.insert("uriMetadata".to_owned(), safe_resource_uri_metadata(uri)?);
            }
            if let Some(template) = object.remove("uriTemplate") {
                let template = template
                    .as_str()
                    .context("MCP external uriTemplate 必须是 string")?;
                let (arguments, metadata) = resource_template_metadata(template)?;
                object.insert(
                    "uriTemplateMetadata".to_owned(),
                    json!({"resource":metadata,"arguments":arguments}),
                );
            }
            for child in object.values_mut() {
                *child = sanitize_external_resource_uris(child.take())?;
            }
        }
        Value::Array(values) => {
            for child in values {
                *child = sanitize_external_resource_uris(child.take())?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(value)
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

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        self.client
            .call_tool(context, &self.original_name, input)
            .await
    }
}

impl McpManager {
    fn clients_snapshot(&self) -> Vec<Arc<McpClient>> {
        self.clients
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn insert_connected_client(&self, client: Arc<McpClient>) {
        client.tools_changed.store(true, Ordering::Release);
        let mut clients = self
            .clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if clients
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&client.name))
        {
            return;
        }
        clients.push(client);
        clients.sort_by_key(|client| self.server_states.order_of(&client.name));
    }

    fn replace_connected_client(&self, client: Arc<McpClient>) -> Option<Arc<McpClient>> {
        client.tools_changed.store(true, Ordering::Release);
        client.resources_changed.store(true, Ordering::Release);
        let mut clients = self
            .clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = clients
            .iter()
            .position(|existing| existing.name.eq_ignore_ascii_case(&client.name))
            .map(|index| std::mem::replace(&mut clients[index], Arc::clone(&client)));
        if previous.is_none() {
            clients.push(client);
        }
        clients.sort_by_key(|client| self.server_states.order_of(&client.name));
        previous
    }

    async fn start_background_connections(self: &Arc<Self>, configs: Vec<ServerConfig>) {
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            stream::iter(configs)
                .for_each_concurrent(MAX_CONCURRENT_SERVER_STARTS, |config| {
                    let weak = weak.clone();
                    async move {
                        let name = config.name.clone();
                        let auth_configured = server_config_uses_auth(&config);
                        let attempt = McpClient::connect(config).await;
                        let Some(manager) = weak.upgrade() else {
                            if let Ok(client) = attempt {
                                client.shutdown().await;
                            }
                            return;
                        };
                        match attempt {
                            Ok(client) => {
                                manager.insert_connected_client(client);
                                manager
                                    .server_states
                                    .set(&name, McpServerStateKind::Connected);
                            }
                            Err(error) => {
                                manager.server_states.set(
                                    &name,
                                    classify_connection_failure(&error, auth_configured),
                                );
                                eprintln!("MCP server skipped: {error:#}");
                            }
                        }
                    }
                })
                .await;
        });
        *self.connection_task.lock().await = Some(task);
    }

    async fn discover_initial_tools(&self) -> Result<Vec<Arc<dyn Tool>>> {
        let mut attempts = stream::iter(self.clients_snapshot().into_iter().enumerate())
            .map(|(index, client)| async move {
                let result = client.list_tools(Arc::clone(&self.resource_handles)).await;
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
        let clients = self.clients_snapshot();
        clients
            .iter()
            .find(|client| client.name == name || client.namespace == name)
            .cloned()
            .with_context(|| {
                format!(
                    "MCP server {name:?} 不存在；可用: {}",
                    clients
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
                .clients_snapshot()
                .iter()
                .filter(|client| client.supports_resources)
                .cloned()
                .collect()),
        }
    }

    async fn list_resources(
        &self,
        context: &ToolContext,
        server: Option<&str>,
        templates: bool,
    ) -> Result<Value> {
        let mut output = Vec::new();
        for client in self.resource_clients(server)? {
            let _call = client.call_lock.lock().await;
            let _elicitation = client.elicitation.activate(context.clone())?;
            let (method, field) = if templates {
                ("resources/templates/list", "resourceTemplates")
            } else {
                ("resources/list", "resources")
            };
            let values = client.list_paginated(method, field, MAX_RESOURCES).await?;
            let mut prepared = Vec::with_capacity(values.len());
            let mut registry_values = Vec::with_capacity(values.len());
            for value in values {
                let mut value = remove_reserved_metadata(value);
                let object = value
                    .as_object_mut()
                    .with_context(|| format!("{method} item 必须是 object"))?;
                let exposed_field = if templates { "uriTemplate" } else { "uri" };
                let raw = object
                    .remove(exposed_field)
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .with_context(|| format!("{method} item 缺少 string URI"))?;
                let (variables, metadata) = if templates {
                    resource_template_metadata(&raw)?
                } else {
                    (Vec::new(), safe_resource_uri_metadata(&raw)?)
                };
                registry_values.push((raw, variables.clone()));
                prepared.push((value, variables, metadata));
            }
            let kind = if templates {
                ResourceHandleKind::Template
            } else {
                ResourceHandleKind::Direct
            };
            let handles = self.resource_handles.lock().await.replace_server_kind(
                &client.name,
                kind,
                registry_values,
            )?;
            for ((mut value, variables, metadata), handle) in prepared.into_iter().zip(handles) {
                let object = value
                    .as_object_mut()
                    .context("validated MCP resource item stopped being an object")?;
                let exposed_field = if templates { "uriTemplate" } else { "uri" };
                object.insert(exposed_field.into(), Value::String(handle));
                object.insert("uriMetadata".into(), metadata);
                if templates {
                    object.insert("arguments".into(), json!(variables));
                }
                object.insert("server".into(), Value::String(client.name.clone()));
                output.push(value);
            }
            client.resources_changed.store(false, Ordering::Release);
        }
        Ok(Value::Array(output))
    }

    async fn read_resource(
        &self,
        context: &ToolContext,
        server: &str,
        uri: &str,
        arguments: Option<&serde_json::Map<String, Value>>,
    ) -> Result<Value> {
        if uri.is_empty() || uri.len() > MAX_RESOURCE_URI_BYTES {
            bail!("resource URI 为空或超过 {MAX_RESOURCE_URI_BYTES} 字节限制")
        }
        let client = self.client(server)?;
        if !client.supports_resources {
            bail!("MCP server {} 未声明 resources capability", client.name)
        }
        let uri = self
            .resource_handles
            .lock()
            .await
            .resolve(&client.name, uri, arguments)?;
        let _call = client.call_lock.lock().await;
        let _elicitation = client.elicitation.activate(context.clone())?;
        let value = client
            .rpc
            .request("resources/read", Some(json!({"uri": uri})))
            .await?;
        let mut media_bytes = 0usize;
        let value =
            sanitize_external_binary_payload(remove_reserved_metadata(value), &mut media_bytes)?;
        sanitize_external_resource_uris(value)
    }

    async fn list_prompts(&self, context: &ToolContext, server: Option<&str>) -> Result<Value> {
        let clients = match server {
            Some(name) => {
                let client = self.client(name)?;
                if !client.supports_prompts {
                    bail!("MCP server {} 未声明 prompts capability", client.name)
                }
                vec![client]
            }
            None => self
                .clients_snapshot()
                .iter()
                .filter(|client| client.supports_prompts)
                .cloned()
                .collect(),
        };
        let mut output = Vec::new();
        for client in clients {
            let _call = client.call_lock.lock().await;
            let _elicitation = client.elicitation.activate(context.clone())?;
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
        context: &ToolContext,
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
        let _call = client.call_lock.lock().await;
        let _elicitation = client.elicitation.activate(context.clone())?;
        let mut params = json!({"name": name});
        if let Some(arguments) = arguments {
            if !arguments.is_object() {
                bail!("MCP prompt arguments 必须是 object")
            }
            params["arguments"] = arguments;
        }
        let value = client.rpc.request("prompts/get", Some(params)).await?;
        let mut media_bytes = 0usize;
        let value =
            sanitize_external_binary_payload(remove_reserved_metadata(value), &mut media_bytes)?;
        sanitize_external_resource_uris(value)
    }
}

#[async_trait]
impl McpControl for McpManager {
    fn status(&self) -> Vec<McpServerStatus> {
        self.server_states.status()
    }

    async fn reconnect(&self, server: &str) -> Result<()> {
        if server.is_empty() || server.len() > MAX_SERVER_NAME_BYTES {
            bail!("MCP reconnect server 名称无效")
        }
        let _reconnect = self.reconnect_lock.lock().await;
        let config = self
            .reconnect_configs
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&server.to_ascii_lowercase())
            .cloned()
            .with_context(|| format!("MCP server {server:?} 不存在或已禁用"))?;
        let name = config.name.clone();
        let auth_configured = server_config_uses_auth(&config);
        self.server_states.set(&name, McpServerStateKind::Pending);
        match McpClient::connect(config).await {
            Ok(client) => {
                let previous = self.replace_connected_client(client);
                self.server_states.set(&name, McpServerStateKind::Connected);
                if let Some(previous) = previous {
                    previous.shutdown().await;
                }
                Ok(())
            }
            Err(error) => {
                self.server_states
                    .set(&name, classify_connection_failure(&error, auth_configured));
                Err(error).with_context(|| format!("MCP server {name} reconnect 失败"))
            }
        }
    }

    async fn list_prompts(&self, context: &ToolContext) -> Result<Value> {
        McpManager::list_prompts(self, context, None).await
    }

    async fn get_prompt(
        &self,
        context: &ToolContext,
        server: &str,
        name: &str,
        arguments: Option<Value>,
    ) -> Result<Value> {
        McpManager::get_prompt(self, context, server, name, arguments).await
    }
}

#[async_trait]
impl McpHookInvoker for McpManager {
    async fn invoke(&self, call: McpHookCall) -> Result<McpHookResult> {
        let McpHookCall {
            server,
            tool,
            input,
            timeout: request_timeout,
        } = call;
        let deadline = Instant::now() + request_timeout;
        if !input.is_object() {
            bail!("MCP hook tool input 必须是 object")
        }
        let client = self
            .clients_snapshot()
            .into_iter()
            .find(|client| client.name == server)
            .with_context(|| format!("MCP server {server:?} 未连接"))?;
        if !client.supports_tools {
            bail!("MCP server {} 未声明 tools capability", client.name)
        }

        let definitions = client
            .list_paginated_with_timeout(
                "tools/list",
                "tools",
                MAX_TOOLS_PER_SERVER,
                request_timeout,
            )
            .await?;
        let definition = definitions
            .iter()
            .find(|definition| definition.get("name").and_then(Value::as_str) == Some(&tool))
            .with_context(|| format!("MCP server {} 未提供 tool {tool:?}", client.name))?;
        let schema = definition
            .get("inputSchema")
            .cloned()
            .unwrap_or_else(|| object_schema(json!({}), &[]));
        if !schema.is_object() || serde_json::to_vec(&schema)?.len() > MAX_TOOL_SCHEMA_BYTES {
            bail!("MCP tool {tool} inputSchema 无效或超过限制")
        }
        let validator = jsonschema::validator_for(&schema)
            .with_context(|| format!("MCP tool {tool} inputSchema 无效"))?;
        if let Err(error) = validator.validate(&input) {
            bail!(
                "MCP tool {tool} input 不符合 schema at {}: {error}",
                error.instance_path()
            )
        }

        let _call = client.call_lock.lock().await;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("MCP hook tool call 超过 timeout")
        }
        let result = client
            .rpc
            .request_with_timeout(
                "tools/call",
                Some(json!({"name": tool, "arguments": input})),
                remaining,
            )
            .await?;
        let output = map_tool_call_result_inner(result, None)?;
        Ok(McpHookResult {
            output: output.content,
            is_error: output.is_error,
        })
    }
}

#[async_trait]
impl ToolService for McpManager {
    async fn shutdown(&self) {
        if let Some(task) = self.connection_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        for client in self.clients_snapshot() {
            client.shutdown().await;
        }
    }
}

#[async_trait]
impl ToolDiscovery for McpManager {
    fn pending_names(&self) -> Vec<String> {
        self.server_states.pending_names()
    }

    async fn refresh(&self) -> Result<ToolRefresh> {
        let changed = self
            .clients_snapshot()
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
            match client.list_tools(Arc::clone(&self.resource_handles)).await {
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

impl Drop for McpManager {
    fn drop(&mut self) {
        if let Some(task) = self.connection_task.get_mut().take() {
            task.abort();
        }
    }
}

struct WaitForMcpServersTool {
    states: Arc<McpServerStates>,
    wait_timeout: Duration,
}

#[async_trait]
impl Tool for WaitForMcpServersTool {
    fn name(&self) -> &str {
        "WaitForMcpServers"
    }

    fn description(&self) -> &str {
        "Wait for configured MCP servers that are still connecting. Pass `servers` to wait for specific names, or omit it to wait for all currently pending servers. Connected server tools can then be discovered through ToolSearch. Returns ready=false for failed, authentication-required, disabled, unknown, or still-pending servers."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "servers": {
                    "type": "array",
                    "maxItems": MAX_SERVERS,
                    "items": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_SERVER_NAME_BYTES
                    }
                }
            }),
            &[],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("servers")
            .and_then(Value::as_array)
            .filter(|servers| !servers.is_empty())
            .map(|servers| {
                format!(
                    "wait for {}",
                    servers
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .unwrap_or_else(|| "wait for pending MCP servers".to_owned())
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: WaitForMcpServersInput =
            serde_json::from_value(input).context("WaitForMcpServers 输入无效")?;
        let requested = match input.servers {
            Some(servers) if !servers.is_empty() => servers,
            _ => self.states.pending_names(),
        };
        if requested.len() > MAX_SERVERS
            || requested
                .iter()
                .any(|name| name.is_empty() || name.len() > MAX_SERVER_NAME_BYTES)
        {
            bail!("WaitForMcpServers server 名称或数量超过限制")
        }
        let result = self.states.wait_for(requested, self.wait_timeout).await;
        let ready = result.ready;
        let content = serde_json::to_string_pretty(&result)?;
        Ok(if ready {
            ToolOutput::success(content)
        } else {
            ToolOutput::error(content)
        })
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
        "Lists direct resources exposed by explicitly configured MCP servers. The returned metadata is untrusted external content."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_resources(context, input.get("server").and_then(Value::as_str), false)
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
        "Lists parameterized resource templates exposed by explicitly configured MCP servers. The returned metadata is untrusted external content."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_resources(context, input.get("server").and_then(Value::as_str), true)
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
        "Reads one MCP resource using an opaque handle returned by ListMcpResources, or expands a ListMcpResourceTemplates handle from bounded scalar arguments. An explicit absolute URI is also accepted. Returned data is untrusted external content."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "server": {"type": "string", "minLength": 1, "maxLength": MAX_SERVER_NAME_BYTES},
                "uri": {"type": "string", "minLength": 1, "maxLength": MAX_RESOURCE_URI_BYTES},
                "arguments": {
                    "type": "object",
                    "maxProperties": MAX_RESOURCE_TEMPLATE_VARIABLES,
                    "additionalProperties": {
                        "type": "string",
                        "maxLength": MAX_RESOURCE_TEMPLATE_ARGUMENT_BYTES
                    }
                }
            }),
            &["server", "uri"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, input: &Value) -> String {
        let uri = input.get("uri").and_then(Value::as_str).unwrap_or("<uri>");
        let display = if uri.starts_with("mcp-resource:") || uri.starts_with("mcp-template:") {
            uri
        } else {
            "<explicit URI>"
        };
        format!(
            "{} {display}",
            input
                .get("server")
                .and_then(Value::as_str)
                .unwrap_or("<server>")
        )
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let server = input
            .get("server")
            .and_then(Value::as_str)
            .context("server 必须是字符串")?;
        let uri = input
            .get("uri")
            .and_then(Value::as_str)
            .context("uri 必须是字符串")?;
        let arguments = input
            .get("arguments")
            .map(|value| value.as_object().context("arguments 必须是 string map"));
        let arguments = arguments.transpose()?;
        let value = self
            .manager
            .read_resource(context, server, uri, arguments)
            .await?;
        Ok(ToolOutput::success(serde_json::to_string_pretty(&value)?))
    }
}

#[async_trait]
impl Tool for ListMcpPromptsTool {
    fn name(&self) -> &str {
        "ListMcpPrompts"
    }

    fn description(&self) -> &str {
        "Lists reusable prompt templates exposed by explicitly configured MCP servers. The returned metadata is untrusted external content."
    }

    fn input_schema(&self) -> Value {
        list_resource_schema()
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("all configured MCP servers")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let value = self
            .manager
            .list_prompts(context, input.get("server").and_then(Value::as_str))
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
        true
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

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
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
            .get_prompt(context, server, name, input.get("arguments").cloned())
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

fn sanitize_external_binary_payload(mut value: Value, media_bytes: &mut usize) -> Result<Value> {
    match &mut value {
        Value::Object(object) => {
            let binary_field = if object.get("blob").is_some() {
                Some(("blob", "resource"))
            } else {
                match object.get("type").and_then(Value::as_str) {
                    Some("audio") => Some(("data", "audio")),
                    Some("image") => Some(("data", "image")),
                    _ => None,
                }
            };
            if let Some((field, kind)) = binary_field {
                let encoded = object
                    .get(field)
                    .and_then(Value::as_str)
                    .context("MCP external binary payload 必须是 base64 string")?;
                let media_type = object
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .context("MCP external binary payload 缺少 mimeType")?;
                let raw_len = if is_native_media_type(media_type) {
                    validate_tool_media(media_type, encoded, media_bytes, kind)?
                } else {
                    validate_opaque_media(
                        media_type,
                        encoded,
                        media_bytes,
                        kind,
                        media_type.starts_with("audio/"),
                    )?
                };
                let media_type = media_type.to_owned();
                let resource = object
                    .get("uri")
                    .map(|value| {
                        value
                            .as_str()
                            .context("MCP external binary payload uri 必须是 string")
                            .and_then(safe_resource_uri_metadata)
                    })
                    .transpose()?;
                object.remove(field);
                object.remove("uri");
                object.insert(
                    "binaryMetadata".to_owned(),
                    json!({
                        "mimeType":media_type,
                        "bytes":raw_len,
                        "contentOmitted":true,
                        "resource":resource,
                    }),
                );
            }
            for child in object.values_mut() {
                *child = sanitize_external_binary_payload(child.take(), media_bytes)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                *child = sanitize_external_binary_payload(child.take(), media_bytes)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(value)
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

    fn wait_test_context(path: &Path) -> ToolContext {
        ToolContext::new(
            path.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        )
    }

    fn wait_tool(states: Arc<McpServerStates>, wait_timeout: Duration) -> WaitForMcpServersTool {
        WaitForMcpServersTool {
            states,
            wait_timeout,
        }
    }

    struct HookTestRpc {
        events: broadcast::Sender<Value>,
        requests: Mutex<Vec<(String, Option<Value>)>>,
    }

    impl HookTestRpc {
        fn new() -> Self {
            let (events, _) = broadcast::channel(4);
            Self {
                events,
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl McpRpc for HookTestRpc {
        async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
            self.requests
                .lock()
                .await
                .push((method.to_owned(), params.clone()));
            match method {
                "tools/list" => Ok(json!({"tools":[{
                    "name":"inspect",
                    "description":"Inspect input",
                    "inputSchema":{
                        "type":"object",
                        "properties":{"path":{"type":"string"}},
                        "required":["path"],
                        "additionalProperties":false
                    }
                }]})),
                "tools/call" => Ok(json!({
                    "content":[{"type":"text","text":"hook-called"}],
                    "isError":false
                })),
                other => bail!("unexpected test MCP method: {other}"),
            }
        }

        async fn notify(&self, _: &str, _: Option<Value>) -> Result<()> {
            Ok(())
        }

        fn subscribe(&self) -> broadcast::Receiver<Value> {
            self.events.subscribe()
        }

        async fn set_protocol_version(&self, _: &str) {}

        async fn start_notifications(&self) {}

        async fn diagnostic_excerpt(&self) -> String {
            String::new()
        }

        async fn shutdown(&self) {}
    }

    fn hook_test_manager(rpc: Arc<HookTestRpc>) -> McpManager {
        let client = Arc::new(McpClient {
            name: "configured".to_owned(),
            namespace: "configured_ns".to_owned(),
            rpc,
            supports_tools: true,
            supports_resources: false,
            supports_prompts: false,
            tools_changed: Arc::new(AtomicBool::new(false)),
            resources_changed: Arc::new(AtomicBool::new(false)),
            event_task: Mutex::new(None),
            elicitation: Arc::new(ElicitationBridge::new(Duration::from_secs(1))),
            call_lock: Arc::new(Mutex::new(())),
        });
        McpManager {
            clients: StdRwLock::new(vec![client]),
            reconnect_configs: StdRwLock::new(HashMap::new()),
            reconnect_lock: Mutex::new(()),
            known_tools: Mutex::new(HashMap::new()),
            resource_handles: Arc::new(Mutex::new(ResourceHandleStore::default())),
            server_states: Arc::new(McpServerStates::new(vec![McpServerState {
                name: "configured".to_owned(),
                kind: McpServerStateKind::Connected,
            }])),
            connection_task: Mutex::new(None),
            strict: true,
            debug: false,
        }
    }

    #[test]
    fn mcp_control_status_is_bounded_and_serializable() {
        let mut manager = hook_test_manager(Arc::new(HookTestRpc::new()));
        manager.server_states = Arc::new(McpServerStates::new(vec![
            McpServerState {
                name: "connected".to_owned(),
                kind: McpServerStateKind::Connected,
            },
            McpServerState {
                name: "pending".to_owned(),
                kind: McpServerStateKind::Pending,
            },
            McpServerState {
                name: "failed".to_owned(),
                kind: McpServerStateKind::Failed,
            },
            McpServerState {
                name: "needs-auth".to_owned(),
                kind: McpServerStateKind::NeedsAuth,
            },
            McpServerState {
                name: "disabled".to_owned(),
                kind: McpServerStateKind::Disabled,
            },
        ]));
        let control: Arc<dyn McpControl> = Arc::new(manager);
        let status = control.status();
        assert!(status.len() <= MAX_SERVERS);
        assert!(status.iter().all(|server| {
            !server.name.is_empty() && server.name.len() <= MAX_SERVER_NAME_BYTES
        }));
        assert_eq!(
            serde_json::to_value(status).unwrap(),
            json!([
                {"name":"connected", "status":"connected"},
                {"name":"pending", "status":"pending"},
                {"name":"failed", "status":"failed"},
                {"name":"needs-auth", "status":"needs_auth"},
                {"name":"disabled", "status":"disabled"}
            ])
        );
        let oversized = McpServerStates::new(
            (0..MAX_SERVERS + 3)
                .map(|index| McpServerState {
                    name: format!("server-{index}"),
                    kind: McpServerStateKind::Pending,
                })
                .collect(),
        );
        assert_eq!(oversized.status().len(), MAX_SERVERS);
    }

    #[tokio::test]
    async fn hook_invoker_rejects_unknown_server_tool_and_invalid_input() {
        let manager = hook_test_manager(Arc::new(HookTestRpc::new()));
        for call in [
            McpHookCall {
                server: "missing".to_owned(),
                tool: "inspect".to_owned(),
                input: json!({"path":"safe"}),
                timeout: Duration::from_secs(1),
            },
            McpHookCall {
                server: "configured_ns".to_owned(),
                tool: "inspect".to_owned(),
                input: json!({"path":"safe"}),
                timeout: Duration::from_secs(1),
            },
            McpHookCall {
                server: "configured".to_owned(),
                tool: "missing".to_owned(),
                input: json!({"path":"safe"}),
                timeout: Duration::from_secs(1),
            },
            McpHookCall {
                server: "configured".to_owned(),
                tool: "inspect".to_owned(),
                input: json!({"wrong":true}),
                timeout: Duration::from_secs(1),
            },
        ] {
            assert!(manager.invoke(call).await.is_err());
        }
    }

    #[tokio::test]
    async fn hook_invoker_calls_only_validated_connected_tool() {
        let rpc = Arc::new(HookTestRpc::new());
        let manager = hook_test_manager(rpc.clone());
        let result = manager
            .invoke(McpHookCall {
                server: "configured".to_owned(),
                tool: "inspect".to_owned(),
                input: json!({"path":"src/lib.rs"}),
                timeout: Duration::from_secs(1),
            })
            .await
            .unwrap();
        assert_eq!(
            result,
            McpHookResult {
                output: "hook-called".to_owned(),
                is_error: false
            }
        );
        let requests = rpc.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0, "tools/list");
        assert_eq!(requests[1].0, "tools/call");
        assert_eq!(requests[1].1.as_ref().unwrap()["name"], "inspect");
        assert_eq!(
            requests[1].1.as_ref().unwrap()["arguments"]["path"],
            "src/lib.rs"
        );
    }

    #[test]
    fn wait_for_mcp_servers_schema_is_strict_and_bounded() {
        let tool = wait_tool(Arc::new(McpServerStates::new(Vec::new())), Duration::ZERO);
        assert!(tool.validate_input(&json!({})).is_ok());
        assert!(
            tool.validate_input(&json!({"servers":["one"]})).is_ok(),
            "valid named wait must pass"
        );
        assert!(
            tool.validate_input(&json!({"servers":[], "extra":true}))
                .is_err(),
            "unknown fields must fail closed"
        );
        assert!(tool.validate_input(&json!({"servers":[""]})).is_err());
        assert!(
            tool.validate_input(&json!({
                "servers": (0..=MAX_SERVERS).map(|index| format!("s{index}")).collect::<Vec<_>>()
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn wait_for_mcp_servers_wakes_on_connection_without_polling() {
        let temp = tempfile::tempdir().unwrap();
        let states = Arc::new(McpServerStates::new(vec![McpServerState {
            name: "Example".to_owned(),
            kind: McpServerStateKind::Pending,
        }]));
        let updater = Arc::clone(&states);
        let update = tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            updater.set("Example", McpServerStateKind::Connected);
        });
        let output = wait_tool(states, Duration::from_secs(1))
            .execute(
                &wait_test_context(temp.path()),
                json!({"servers":["example"]}),
            )
            .await
            .unwrap();
        update.await.unwrap();
        assert!(!output.is_error, "{}", output.content);
        let result: WaitForMcpServersResult = serde_json::from_str(&output.content).unwrap();
        assert!(result.ready);
        assert_eq!(result.connected, ["Example"]);
        assert!(result.still_pending.is_empty());
    }

    #[tokio::test]
    async fn wait_for_mcp_servers_reports_all_terminal_failures_and_unknowns() {
        let temp = tempfile::tempdir().unwrap();
        let states = Arc::new(McpServerStates::new(vec![
            McpServerState {
                name: "connected".to_owned(),
                kind: McpServerStateKind::Connected,
            },
            McpServerState {
                name: "failed".to_owned(),
                kind: McpServerStateKind::Failed,
            },
            McpServerState {
                name: "auth".to_owned(),
                kind: McpServerStateKind::NeedsAuth,
            },
            McpServerState {
                name: "disabled".to_owned(),
                kind: McpServerStateKind::Disabled,
            },
        ]));
        let output = wait_tool(states, Duration::from_secs(1))
            .execute(
                &wait_test_context(temp.path()),
                json!({"servers":["CONNECTED", "failed", "auth", "disabled", "missing"]}),
            )
            .await
            .unwrap();
        assert!(
            output.is_error,
            "terminal non-ready states must be an error result"
        );
        let result: WaitForMcpServersResult = serde_json::from_str(&output.content).unwrap();
        assert!(!result.ready);
        assert_eq!(result.connected, ["connected"]);
        assert_eq!(result.failed, ["failed"]);
        assert_eq!(result.needs_auth, ["auth"]);
        assert_eq!(result.disabled, ["disabled"]);
        assert_eq!(result.unknown, ["missing"]);
    }

    #[tokio::test]
    async fn wait_for_mcp_servers_timeout_keeps_server_pending() {
        let temp = tempfile::tempdir().unwrap();
        let states = Arc::new(McpServerStates::new(vec![McpServerState {
            name: "slow".to_owned(),
            kind: McpServerStateKind::Pending,
        }]));
        let output = timeout(
            Duration::from_millis(500),
            wait_tool(states, Duration::from_millis(25))
                .execute(&wait_test_context(temp.path()), json!({})),
        )
        .await
        .expect("bounded wait must return")
        .unwrap();
        assert!(output.is_error);
        let result: WaitForMcpServersResult = serde_json::from_str(&output.content).unwrap();
        assert!(!result.ready);
        assert_eq!(result.still_pending, ["slow"]);
    }

    #[tokio::test]
    async fn wait_for_mcp_servers_is_drop_cancel_safe() {
        let temp = tempfile::tempdir().unwrap();
        let states = Arc::new(McpServerStates::new(vec![McpServerState {
            name: "slow".to_owned(),
            kind: McpServerStateKind::Pending,
        }]));
        let tool = wait_tool(states, Duration::from_secs(5));
        let context = wait_test_context(temp.path());
        let task = tokio::spawn(async move { tool.execute(&context, json!({})).await });
        tokio::task::yield_now().await;
        task.abort();
        let cancellation = timeout(Duration::from_millis(200), task)
            .await
            .expect("cancelled wait must not leave a background polling task")
            .unwrap_err();
        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn connection_failure_classification_distinguishes_auth_from_transport() {
        assert_eq!(
            classify_connection_failure(&anyhow::anyhow!("MCP HTTP 401: denied"), false),
            McpServerStateKind::NeedsAuth
        );
        assert_eq!(
            classify_connection_failure(
                &anyhow::anyhow!("MCP bearer token environment variable 未设置"),
                true,
            ),
            McpServerStateKind::NeedsAuth
        );
        assert_eq!(
            classify_connection_failure(&anyhow::anyhow!("MCP HTTP POST 失败"), false),
            McpServerStateKind::Failed
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn non_strict_pending_server_waits_then_refreshes_through_tool_search() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("delayed-mcp.sh");
        std::fs::write(
            &script,
            r#"sleep 0.05
while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"delayed","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object","additionalProperties":false}}]}}' ;;
esac
done
"#,
        )
        .unwrap();
        let settings = Settings {
            raw: json!({
                "mcpServers": {
                    "Delayed": {"command":"/bin/sh", "args":[script]}
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
        let definitions = registry.definitions();
        assert!(
            definitions
                .iter()
                .any(|tool| tool["name"] == "WaitForMcpServers")
        );
        assert!(definitions.iter().any(|tool| tool["name"] == "ToolSearch"));
        let context = wait_test_context(temp.path());
        let waited = registry
            .execute(
                &context,
                "WaitForMcpServers",
                json!({"servers":["delayed"]}),
            )
            .await;
        assert!(!waited.is_error, "{}", waited.content);
        let selected = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"select:mcp__delayed__echo"}),
            )
            .await;
        assert!(!selected.is_error, "{}", selected.content);
        assert!(selected.content.contains("mcp__delayed__echo"));
        assert!(
            !selected
                .content
                .contains("\"missing\": [\n    \"mcp__delayed__echo\"")
        );
        registry.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_stdio_server_can_be_reconnected_and_rediscovered() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("recoverable-mcp.sh");
        std::fs::write(&script, "exit 1\n").unwrap();
        let settings = Settings {
            raw: json!({
                "mcpServers": {
                    "Recoverable": {"command":"/bin/sh", "args":[script]}
                }
            }),
        };
        let integration = connect_mcp(&settings, temp.path(), false)
            .await
            .unwrap()
            .unwrap();
        let context = wait_test_context(temp.path());
        let wait = integration
            .active_tools
            .iter()
            .find(|tool| tool.name() == "WaitForMcpServers")
            .expect("wait tool missing");
        let settled = wait
            .execute(&context, json!({"servers":["recoverable"]}))
            .await
            .unwrap();
        assert!(settled.content.contains("failed"), "{}", settled.content);

        std::fs::write(
            &script,
            r#"while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"recovered","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object","additionalProperties":false}}]}}' ;;
esac
done
"#,
        )
        .unwrap();
        integration.control.reconnect("RECOVERABLE").await.unwrap();
        let refresh = integration.discovery.refresh().await.unwrap();
        assert_eq!(refresh.remove, Vec::<String>::new());
        assert!(
            refresh
                .upsert
                .iter()
                .any(|tool| tool.name() == "mcp__recoverable__echo")
        );
        let unknown = integration.control.reconnect("missing").await.unwrap_err();
        assert!(unknown.to_string().contains("不存在或已禁用"));
        integration.service.shutdown().await;
    }

    #[test]
    fn trusted_server_settings_are_bounded_and_normalized() {
        let temp = tempfile::tempdir().unwrap();
        let settings = Settings {
            raw: json!({
                "mcpServers": {
                    "Local Server": {
                        "command": "server",
                        "args": ["--stdio"],
                        "roots": ["."]
                    }
                }
            }),
        };
        let configs = parse_server_configs(&settings, temp.path()).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].namespace, "local_server");
        assert_eq!(configs[0].roots.len(), 1);
        assert!(configs[0].roots[0].uri.starts_with("file://"));
        let roots = mcp_client_request_result("roots/list", &configs[0].roots).unwrap();
        assert_eq!(
            roots["roots"][0]["name"],
            temp.path().file_name().unwrap().to_str().unwrap()
        );
    }

    #[test]
    fn http_query_credentials_are_rejected_without_echoing_values() {
        let temp = tempfile::tempdir().unwrap();
        let configured_secret = "unit-test-mcp-query-credential";
        let settings = Settings {
            raw: json!({"mcpServers": {"remote": {
                "url": format!("https://mcp.example.invalid/rpc?access_token={configured_secret}")
            }}}),
        };
        let error = parse_server_configs(&settings, temp.path()).unwrap_err();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("请改用 headers"));
        assert!(!rendered.contains(configured_secret));
    }

    #[test]
    fn trusted_config_accepts_explicit_legacy_sse_bearer_provider() {
        let temp = tempfile::tempdir().unwrap();
        let settings = Settings {
            raw: json!({"mcpServers":{"legacy":{
                "type":"sse",
                "url":"https://mcp.example.invalid/events",
                "auth":{"type":"bearer-env", "env":"UNIT_TEST_MCP_TOKEN"},
                "elicitationTimeoutMs":2500
            }}}),
        };
        let configs = parse_server_configs(&settings, temp.path()).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].elicitation_timeout, Duration::from_millis(2500));
        match &configs[0].transport {
            ServerTransport::Http {
                credential: Some(TokenCredentialProvider::Env { name }),
                legacy_sse,
                ..
            } => {
                assert_eq!(name, "UNIT_TEST_MCP_TOKEN");
                assert!(*legacy_sse);
            }
            _ => panic!("expected legacy SSE bearer-env transport"),
        }
    }

    #[test]
    fn trusted_config_accepts_websocket_and_provider_neutral_oauth() {
        let temp = tempfile::tempdir().unwrap();
        let websocket = Settings {
            raw: json!({"mcpServers":{"socket":{
                "type":"ws",
                "url":"wss://mcp.example.invalid/socket",
                "headers":{"X-Workspace":"trusted"}
            }}}),
        };
        let configs = parse_server_configs(&websocket, temp.path()).unwrap();
        assert!(matches!(
            &configs[0].transport,
            ServerTransport::WebSocket {
                credential: None,
                allow_private_network: false,
                ..
            }
        ));

        let oauth = Settings {
            raw: json!({"mcpServers":{"remote":{
                "type":"http",
                "url":"https://mcp.example.invalid/rpc",
                "auth":{
                    "type":"oauth",
                    "clientId":"public-client",
                    "scopes":["read"],
                    "tokenPath":temp.path().join("token.json"),
                    "authorizationUrlPath":temp.path().join("authorize.txt"),
                    "callbackPath":temp.path().join("callback.txt"),
                    "redirectUri":"http://127.0.0.1/callback"
                }
            }}}),
        };
        let configs = parse_server_configs(&oauth, temp.path()).unwrap();
        assert!(matches!(
            &configs[0].transport,
            ServerTransport::Http {
                credential: Some(TokenCredentialProvider::OAuth(_)),
                ..
            }
        ));

        let websocket_oauth = Settings {
            raw: json!({"mcpServers":{"socket":{
                "type":"ws",
                "url":"wss://mcp.example.invalid/socket",
                "auth":{
                    "type":"oauth",
                    "clientId":"public-client",
                    "tokenPath":temp.path().join("token.json"),
                    "authorizationUrlPath":temp.path().join("authorize.txt"),
                    "callbackPath":temp.path().join("callback.txt"),
                    "redirectUri":"http://127.0.0.1/callback"
                }
            }}}),
        };
        assert!(
            parse_server_configs(&websocket_oauth, temp.path())
                .unwrap_err()
                .to_string()
                .contains("仅适用于 HTTP/SSE")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn trusted_bearer_file_and_command_credentials_are_bounded() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let token_file = temp.path().join("token");
        std::fs::write(&token_file, "unit-test-token\n").unwrap();
        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        let file = TokenCredentialProvider::File {
            path: token_file.clone(),
        };
        let (header, secret) = file.bearer_header().await.unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer unit-test-token");
        assert_eq!(secret, "unit-test-token");

        std::fs::set_permissions(&token_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(file.bearer_header().await.is_err());

        let oversized = temp.path().join("oversized-token");
        std::fs::write(&oversized, vec![b'x'; MAX_AUTH_TOKEN_BYTES + 1]).unwrap();
        std::fs::set_permissions(&oversized, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(
            TokenCredentialProvider::File { path: oversized }
                .bearer_header()
                .await
                .unwrap_err()
                .to_string()
                .contains("字节限制")
        );

        let symlink = temp.path().join("token-link");
        std::os::unix::fs::symlink(&token_file, &symlink).unwrap();
        assert!(
            TokenCredentialProvider::File { path: symlink }
                .bearer_header()
                .await
                .is_err()
        );

        let command = TokenCredentialProvider::Command {
            command: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_owned(), "printf command-token".to_owned()],
            cwd: temp.path().to_owned(),
            timeout: Duration::from_secs(2),
            secret_env_scrubber: SecretEnvScrubber::default(),
        };
        let (header, secret) = command.bearer_header().await.unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer command-token");
        assert_eq!(secret, "command-token");
        assert!(normalize_bearer_token("contains space".to_owned()).is_err());
    }

    #[test]
    fn elicitation_bridge_validates_schema_response_and_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_user_interaction_handler(Some(Arc::new(|request| {
            assert_eq!(request.tool, "McpElicitation");
            assert_eq!(request.input["subtype"], "elicitation");
            Ok(json!({"action":"accept", "content":{"choice":"yes"}}))
        })));
        let bridge = Arc::new(ElicitationBridge::new(Duration::from_secs(1)));
        let _scope = bridge.activate(context).unwrap();
        let handler = McpClientRequestHandler {
            server_name: "mock".to_owned(),
            roots: Vec::new(),
            elicitation: Arc::clone(&bridge),
        };
        let response = handler
            .result(
                "elicitation/create",
                Some(&json!({
                    "message":"Choose",
                    "requestedSchema":{
                        "type":"object",
                        "properties":{"choice":{"type":"string", "enum":["yes","no"]}},
                        "required":["choice"],
                        "additionalProperties":false
                    }
                })),
            )
            .unwrap();
        assert_eq!(
            response,
            json!({"action":"accept", "content":{"choice":"yes"}})
        );

        let invalid = validate_elicitation_response(
            &json!({"action":"accept", "content":{"choice":"maybe"}}),
            Some(&json!({
                "type":"object",
                "properties":{"choice":{"type":"string", "enum":["yes","no"]}},
                "required":["choice"]
            })),
        );
        assert!(invalid.is_err());
        assert!(
            validate_elicitation_response(
                &json!({"action":"accept"}),
                Some(&json!({"type":"object", "properties":{}})),
            )
            .is_err()
        );

        let inactive = ElicitationBridge::new(Duration::from_millis(10));
        assert_eq!(
            inactive.respond(
                "mock",
                Some(&json!({
                    "message":"No handler",
                    "requestedSchema":{"type":"object", "properties":{}}
                }))
            ),
            json!({"action":"cancel"})
        );
    }

    #[test]
    fn tty_elicitation_response_helpers_are_bounded_and_schema_validated() {
        assert_eq!(
            parse_tty_elicitation_response("url", "accept").unwrap(),
            json!({"action":"accept"})
        );
        assert_eq!(
            parse_tty_elicitation_response("url", "d").unwrap(),
            json!({"action":"decline"})
        );
        assert_eq!(
            parse_tty_elicitation_response("url", "").unwrap(),
            json!({"action":"cancel"})
        );
        assert!(parse_tty_elicitation_response("url", "https://example.invalid").is_err());

        let schema = json!({
            "type":"object",
            "properties":{"choice":{"type":"string", "enum":["yes", "no"]}},
            "required":["choice"],
            "additionalProperties":false
        });
        let response = parse_tty_elicitation_response("form", r#"{"choice":"yes"}"#).unwrap();
        assert_eq!(
            validate_elicitation_response(&response, Some(&schema)).unwrap(),
            json!({"action":"accept", "content":{"choice":"yes"}})
        );
        let invalid = parse_tty_elicitation_response("form", r#"{"choice":"maybe"}"#).unwrap();
        assert!(validate_elicitation_response(&invalid, Some(&schema)).is_err());
        assert!(parse_tty_elicitation_response("form", "[]").is_err());

        let mut exact = std::io::Cursor::new(b"1234\r\n".to_vec());
        assert_eq!(read_bounded_line(&mut exact, 4).unwrap(), "1234");
        let mut oversized = std::io::Cursor::new(b"123456789\nnext\n".to_vec());
        assert!(read_bounded_line(&mut oversized, 4).is_err());
        assert_eq!(read_bounded_line(&mut oversized, 4).unwrap(), "next");
        assert_eq!(
            sanitize_terminal_text("safe\n\u{1b}[31m\rtext", 128),
            "safe  [31m text"
        );
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
    fn tool_call_maps_text_image_and_structured_content_without_base64_preview() {
        let image = BASE64.encode(b"\x89PNG\r\n\x1a\n");
        let output = map_tool_call_result(json!({
            "content": [
                {"type":"text", "text":"plain result"},
                {"type":"image", "mimeType":"image/png", "data":image}
            ],
            "structuredContent": {"count": 2, "nested": {"ok": true}},
            "isError": false,
            "_meta": {"private": "not-forwarded"}
        }))
        .unwrap();
        assert!(!output.is_error);
        assert!(output.content.contains("plain result"));
        assert!(output.content.contains("MCP image: image/png, 8 bytes"));
        assert!(output.content.contains("MCP structured content"));
        assert!(!output.content.contains(&image));
        assert!(!output.content.contains("\"count\""));
        assert!(!output.content.contains("not-forwarded"));

        let blocks = output.model_content.unwrap();
        let blocks = blocks.as_array().unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[0], json!({"type":"text", "text":"plain result"}));
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["media_type"], "image/png");
        assert_eq!(blocks[1]["source"]["data"], image);
        assert_eq!(
            blocks[2],
            json!({
                "type":"text",
                "text":"MCP structured content:\n{\"count\":2,\"nested\":{\"ok\":true}}"
            })
        );
    }

    #[test]
    fn tool_call_maps_embedded_text_image_and_pdf_resources() {
        let image = BASE64.encode(b"GIF89a");
        let pdf = BASE64.encode(b"%PDF-1.7\n");
        let output = map_tool_call_result(json!({
            "content": [
                {"type":"resource", "resource": {
                    "uri":"mcp://host/notes.txt", "mimeType":"text/plain", "text":"notes"
                }},
                {"type":"resource", "resource": {
                    "uri":"mcp://host/image.gif", "mimeType":"image/gif", "blob":image
                }},
                {"type":"resource", "resource": {
                    "uri":"mcp://host/docs/report.pdf", "mimeType":"application/pdf", "blob":pdf
                }}
            ]
        }))
        .unwrap();
        assert!(!output.is_error);
        assert!(output.content.contains("notes"));
        assert!(output.content.contains("MCP image resource: image/gif"));
        assert!(output.content.contains("MCP PDF resource"));
        assert!(!output.content.contains(&image));
        assert!(!output.content.contains(&pdf));

        let blocks = output.model_content.unwrap();
        let blocks = blocks.as_array().unwrap();
        assert_eq!(blocks[0], json!({"type":"text", "text":"notes"}));
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["media_type"], "image/gif");
        assert_eq!(blocks[2]["type"], "document");
        assert_eq!(blocks[2]["title"], "mcp-resource.pdf");
        assert_eq!(blocks[2]["source"]["media_type"], "application/pdf");
    }

    #[test]
    fn tool_call_preserves_server_error_semantics_and_model_text() {
        let output = map_tool_call_result(json!({
            "content": [{"type":"text", "text":"operation failed"}],
            "isError": true
        }))
        .unwrap();
        assert!(output.is_error);
        assert_eq!(output.content, "operation failed");
        assert_eq!(
            output.model_content.unwrap(),
            json!([{"type":"text", "text":"operation failed"}])
        );
    }

    #[test]
    fn tool_call_maps_audio_binary_and_resource_links_without_base64_leakage() {
        let audio_data = BASE64.encode(b"RIFF\x04\0\0\0WAVE");
        let binary_data = BASE64.encode(b"\0binary");
        let output = map_tool_call_result(json!({
            "content": [
                {"type":"audio", "mimeType":"audio/wav", "data":audio_data},
                {"type":"resource", "resource": {
                    "uri":"mcp://host/data.bin", "mimeType":"application/octet-stream", "blob":binary_data
                }},
                {"type":"resource_link", "uri":"mcp://host/report", "name":"Report",
                 "description":"Generated report", "mimeType":"text/plain", "size":42}
            ]
        }))
        .unwrap();
        assert!(output.content.contains("MCP audio"));
        assert!(output.content.contains("binary resource"));
        assert!(output.content.contains("resource_link"));
        assert!(!output.content.contains(&audio_data));
        assert!(!output.content.contains(&binary_data));
        let blocks = output.model_content.unwrap();
        assert!(
            blocks
                .as_array()
                .unwrap()
                .iter()
                .all(|block| block["type"] == "text")
        );
        assert!(!blocks.to_string().contains(&audio_data));
        assert!(!blocks.to_string().contains(&binary_data));
    }

    #[tokio::test]
    async fn tool_resource_links_receive_readable_opaque_handles_without_uri_leakage() {
        let store = Arc::new(Mutex::new(ResourceHandleStore::default()));
        let raw = "file:///Users/alice/private/report.txt?token=secret#hidden";
        let output = map_tool_call_result_with_handles(
            json!({
                "content":[{
                    "type":"resource_link",
                    "uri":raw,
                    "name":"Report"
                }]
            }),
            "mock",
            &store,
        )
        .await
        .unwrap();
        let handle = store.lock().await.entries.keys().next().unwrap().clone();
        assert!(handle.starts_with("mcp-resource:"));
        assert!(output.content.contains(&handle));
        assert!(!output.content.contains("Users"));
        assert!(!output.content.contains("token"));
        let model_content = output.model_content.unwrap().to_string();
        assert!(model_content.contains(&handle));
        assert!(!model_content.contains("Users"));
        assert!(!model_content.contains("token"));
        assert_eq!(
            store.lock().await.resolve("mock", &handle, None).unwrap(),
            raw
        );
        assert!(store.lock().await.resolve("other", &handle, None).is_err());
    }

    #[test]
    fn resource_uri_metadata_never_exposes_paths_credentials_or_secrets() {
        let local = safe_resource_uri_metadata(
            "file:///Users/alice/private/report.pdf?token=secret#fragment-secret",
        )
        .unwrap();
        assert_eq!(local, json!({"kind":"local_resource", "scheme":"file"}));
        let local_text = local.to_string();
        assert!(!local_text.contains("Users"));
        assert!(!local_text.contains("token"));

        let network = safe_resource_uri_metadata(
            "https://example.invalid/private/report?token=secret#fragment-secret",
        )
        .unwrap();
        assert_eq!(
            network,
            json!({
                "kind":"network_resource",
                "origin":"https://example.invalid",
                "scheme":"https"
            })
        );
        let network_text = network.to_string();
        assert!(!network_text.contains("private"));
        assert!(!network_text.contains("secret"));

        assert_eq!(
            safe_resource_uri_metadata("mcp://host/private?secret=yes#hidden").unwrap(),
            json!({"kind":"resource", "scheme":"mcp"})
        );
        assert!(safe_resource_uri_metadata("https://user:password@example.invalid/x").is_err());
        assert!(safe_resource_uri_metadata("relative/path").is_err());
    }

    #[test]
    fn opaque_resource_templates_expand_bounded_scalars_without_exposing_raw_uris() {
        let template = "https://example.invalid/private/{name}{?query}";
        let (variables, metadata) = resource_template_metadata(template).unwrap();
        assert_eq!(variables, vec!["name", "query"]);
        assert_eq!(
            metadata,
            json!({
                "kind":"network_resource",
                "origin":"https://example.invalid",
                "scheme":"https"
            })
        );
        let arguments = json!({"name":"folder/item", "query":"a b"});
        let expanded =
            expand_uri_template(template, &variables, arguments.as_object().unwrap()).unwrap();
        assert_eq!(
            expanded,
            "https://example.invalid/private/folder%2Fitem?query=a%20b"
        );
        assert!(
            expand_uri_template(
                template,
                &variables,
                json!({"name":"item"}).as_object().unwrap(),
            )
            .is_err()
        );

        let sanitized = sanitize_external_resource_uris(json!({
            "contents":[{
                "uri":"file:///Users/alice/private.txt?token=secret#hidden",
                "text":"body"
            }]
        }))
        .unwrap();
        let rendered = sanitized.to_string();
        assert!(rendered.contains("uriMetadata"));
        assert!(rendered.contains("local_resource"));
        assert!(!rendered.contains("Users"));
        assert!(!rendered.contains("token"));
    }

    #[test]
    fn tool_resource_metadata_omits_file_paths_and_uri_secrets() {
        let binary = BASE64.encode(b"\0binary");
        let pdf = BASE64.encode(b"%PDF-1.7\n");
        let output = map_tool_call_result(json!({
            "content":[
                {"type":"resource", "resource":{
                    "uri":"file:///Users/alice/private/data.bin?token=secret#hidden",
                    "mimeType":"application/octet-stream",
                    "blob":binary
                }},
                {"type":"resource", "resource":{
                    "uri":"file:///Users/alice/private/report.pdf",
                    "mimeType":"application/pdf",
                    "blob":pdf
                }},
                {"type":"resource_link",
                    "uri":"https://example.invalid/private/report?token=secret#hidden",
                    "name":"Report"
                }
            ]
        }))
        .unwrap();
        let rendered = format!("{} {}", output.content, output.model_content.unwrap());
        assert!(!rendered.contains("/Users/"));
        assert!(!rendered.contains("private/report"));
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("fragment"));
        assert!(rendered.contains("local_resource"));
        assert!(rendered.contains("https://example.invalid"));
        assert!(rendered.contains("mcp-resource.pdf"));

        let rejected = map_tool_call_result(json!({
            "content":[{"type":"resource_link",
                "uri":"https://user:password@example.invalid/report",
                "name":"Report"
            }]
        }))
        .unwrap_err();
        assert!(format!("{rejected:#}").contains("userinfo"));
    }

    #[test]
    fn tool_call_rejects_malformed_binary_content() {
        let bad_audio = map_tool_call_result(json!({
            "content": [{"type":"audio", "mimeType":"audio/wav", "data":"AA=="}]
        }))
        .unwrap_err();
        assert!(bad_audio.to_string().contains("内容签名"));

        let bad_base64 = map_tool_call_result(json!({
            "content": [{"type":"image", "mimeType":"image/png", "data":"not-base64"}]
        }))
        .unwrap_err();
        assert!(bad_base64.to_string().contains("无效 base64"));

        let wrong_signature = map_tool_call_result(json!({
            "content": [{
                "type":"image", "mimeType":"image/png", "data":BASE64.encode(b"GIF89a")
            }]
        }))
        .unwrap_err();
        assert!(wrong_signature.to_string().contains("内容签名"));
    }

    #[test]
    fn resource_and_prompt_payload_sanitizer_omits_validated_binary_bytes() {
        let encoded = BASE64.encode(b"\0binary");
        let mut total = 0;
        let sanitized = sanitize_external_binary_payload(
            json!({"contents":[{"uri":"mcp://host/blob", "mimeType":"application/octet-stream", "blob":encoded}]}),
            &mut total,
        )
        .unwrap();
        assert_eq!(total, 7);
        assert_eq!(sanitized["contents"][0]["binaryMetadata"]["bytes"], 7);
        assert!(sanitized["contents"][0].get("blob").is_none());
        assert!(!sanitized.to_string().contains(&encoded));

        let encoded = BASE64.encode(b"\0binary");
        let mut total = 0;
        let sanitized = sanitize_external_binary_payload(
            json!({"contents":[{
                "uri":"file:///Users/alice/private.bin?token=secret#hidden",
                "mimeType":"application/octet-stream",
                "blob":encoded
            }]}),
            &mut total,
        )
        .unwrap();
        let rendered = sanitized.to_string();
        assert!(!rendered.contains("/Users/"));
        assert!(!rendered.contains("token"));
        assert_eq!(
            sanitized["contents"][0]["binaryMetadata"]["resource"],
            json!({"kind":"local_resource", "scheme":"file"})
        );
    }

    #[test]
    fn tool_call_rejects_invalid_shapes_and_excessive_results() {
        let invalid_structured = map_tool_call_result(json!({
            "content": [], "structuredContent": [1, 2, 3]
        }))
        .unwrap_err();
        assert!(invalid_structured.to_string().contains("必须是 object"));

        let too_many = map_tool_call_result(json!({
            "content": (0..=MAX_TOOL_CONTENT_BLOCKS)
                .map(|_| json!({"type":"text", "text":"x"}))
                .collect::<Vec<_>>()
        }))
        .unwrap_err();
        assert!(too_many.to_string().contains("block 限制"));

        let oversized = map_tool_call_result(json!({
            "content": [{"type":"text", "text":"x".repeat(MAX_TOOL_RESULT_BYTES)}]
        }))
        .unwrap_err();
        assert!(oversized.to_string().contains("result 超过"));
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

    #[tokio::test]
    async fn legacy_sse_transport_discovers_same_origin_endpoint_and_round_trips() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut get_stream, _) = listener.accept().unwrap();
            let (request_line, _) = read_http_request(&mut get_stream);
            assert!(request_line.starts_with("GET /events "));
            let (sender, receiver) = std::sync::mpsc::channel::<Value>();
            let sse = thread::spawn(move || {
                write!(
                    get_stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\nevent: endpoint\ndata: /messages?sessionId=unit-session\r\n\r\n"
                )
                .unwrap();
                get_stream.flush().unwrap();
                for value in receiver {
                    write!(get_stream, "event: message\ndata: {value}\n\n").unwrap();
                    get_stream.flush().unwrap();
                }
            });

            let (mut post_stream, _) = listener.accept().unwrap();
            let (request_line, body) = read_http_request(&mut post_stream);
            assert!(request_line.starts_with("POST /messages?sessionId=unit-session "));
            assert_eq!(body["method"], "ping");
            write!(
                post_stream,
                "HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            sender
                .send(json!({"jsonrpc":"2.0", "id":body["id"], "result":{"ok":true}}))
                .unwrap();
            drop(sender);
            sse.join().unwrap();
        });

        let bridge = Arc::new(ElicitationBridge::new(Duration::from_secs(1)));
        let rpc = LegacySseMcpRpc::connect(
            Url::parse(&format!("http://{address}/events")).unwrap(),
            HeaderMap::new(),
            Vec::new(),
            None,
            true,
            Duration::from_secs(2),
            McpClientRequestHandler {
                server_name: "legacy".to_owned(),
                roots: Vec::new(),
                elicitation: bridge,
            },
        )
        .await
        .unwrap();
        assert_eq!(rpc.request("ping", None).await.unwrap(), json!({"ok":true}));
        rpc.shutdown().await;
        server.join().unwrap();

        let origin = Url::parse("https://mcp.example.invalid/events").unwrap();
        assert!(
            validate_legacy_post_url(&origin, "https://other.example.invalid/messages").is_err()
        );
    }

    #[tokio::test]
    async fn streamable_http_processes_elicitation_before_final_sse_result() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut call_stream, _) = listener.accept().unwrap();
            let (request_line, call) = read_http_request(&mut call_stream);
            assert!(request_line.starts_with("POST /mcp "));
            assert_eq!(call["method"], "tools/call");
            let call_id = call["id"].clone();
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let response_stream = thread::spawn(move || {
                write!(
                    call_stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\ndata: {}\n\n",
                    json!({
                        "jsonrpc":"2.0", "id":"ask-1", "method":"elicitation/create",
                        "params":{
                            "message":"Choose",
                            "requestedSchema":{
                                "type":"object",
                                "properties":{"choice":{"type":"string", "enum":["yes","no"]}},
                                "required":["choice"],
                                "additionalProperties":false
                            }
                        }
                    })
                )
                .unwrap();
                call_stream.flush().unwrap();
                receiver.recv().unwrap();
                write!(
                    call_stream,
                    "data: {}\n\n",
                    json!({"jsonrpc":"2.0", "id":call_id, "result":{"content":[{"type":"text","text":"done"}]}})
                )
                .unwrap();
                call_stream.flush().unwrap();
            });

            let (mut elicitation_stream, _) = listener.accept().unwrap();
            let (request_line, response) = read_http_request(&mut elicitation_stream);
            assert!(request_line.starts_with("POST /mcp "));
            assert_eq!(response["id"], "ask-1");
            assert_eq!(response["result"]["action"], "accept");
            assert_eq!(response["result"]["content"]["choice"], "yes");
            write!(
                elicitation_stream,
                "HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
            sender.send(()).unwrap();
            response_stream.join().unwrap();
        });

        let temp = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_user_interaction_handler(Some(Arc::new(|_| {
            Ok(json!({"action":"accept", "content":{"choice":"yes"}}))
        })));
        let bridge = Arc::new(ElicitationBridge::new(Duration::from_secs(1)));
        let _scope = bridge.activate(context).unwrap();
        let rpc = HttpMcpRpc::new(HttpMcpConfig {
            server_name: "http".to_owned(),
            url: Url::parse(&format!("http://{address}/mcp")).unwrap(),
            headers: HeaderMap::new(),
            secrets: Vec::new(),
            credential: None,
            allow_private_network: true,
            request_timeout: Duration::from_secs(2),
            roots: Vec::new(),
            elicitation: Arc::clone(&bridge),
        });
        let result = rpc
            .request("tools/call", Some(json!({"name":"echo", "arguments":{}})))
            .await
            .unwrap();
        assert_eq!(result["content"][0]["text"], "done");
        rpc.shutdown().await;
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_tools_and_resources_join_the_registry() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("mock-mcp.sh");
        std::fs::write(
            &script,
            r#"tool_lists=0
roots_seen=0
while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*)
    printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{},"resources":{},"prompts":{}},"serverInfo":{"name":"mock","version":"1"}}}'
    printf '%s\n' '{"jsonrpc":"2.0","id":"roots-1","method":"roots/list"}' ;;
  *'"id":"roots-1"'*'"result"'*'file:'*) roots_seen=1 ;;
  *'"method":"tools/list"'*)
    tool_lists=$((tool_lists + 1))
    if [ "$tool_lists" -eq 1 ]; then
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo input","inputSchema":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}}]}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}'
    else
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"dynamic","description":"Dynamic input","inputSchema":{"type":"object","additionalProperties":false}}]}}'
    fi ;;
  *'"method":"tools/call"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"elicit-1","method":"elicitation/create","params":{"message":"Choose","requestedSchema":{"type":"object","properties":{"choice":{"type":"string","enum":["yes","no"]}},"required":["choice"],"additionalProperties":false}}}' ;;
  *'"id":"elicit-1"'*'"action":"accept"'*'"choice":"yes"'*) printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"content":[{"type":"text","text":"called-dynamic"}],"isError":false}}' ;;
  *'"method":"resources/list"'*) printf '{"jsonrpc":"2.0","id":5,"result":{"resources":[{"uri":"file:///private/project/secret.txt?token=hidden#fragment","name":"root-%s"}]}}\n' "$roots_seen" ;;
  *'"method":"resources/read"'*'file:///private/project/secret.txt?token=hidden#fragment'*) printf '%s\n' '{"jsonrpc":"2.0","id":6,"result":{"contents":[{"uri":"file:///private/project/secret.txt?token=hidden#fragment","text":"direct-resource-body"}]}}' ;;
  *'"method":"resources/templates/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":7,"result":{"resourceTemplates":[{"uriTemplate":"mock://host/private/{name}?token=hidden#fragment","name":"by-name"}]}}' ;;
  *'"method":"resources/read"'*'mock://host/private/item?token=hidden#fragment'*) printf '%s\n' '{"jsonrpc":"2.0","id":8,"result":{"contents":[{"uri":"mock://host/private/item?token=hidden#fragment","text":"template-resource-body"}]}}' ;;
  *'"method":"prompts/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":9,"result":{"prompts":[{"name":"review","description":"Review input"}]}}' ;;
  *'"method":"prompts/get"'*) printf '%s\n' '{"jsonrpc":"2.0","id":10,"result":{"description":"Rendered","messages":[{"role":"user","content":{"type":"text","text":"review this"}},{"role":"user","content":{"type":"resource","resource":{"uri":"file:///private/prompt.txt?token=hidden#fragment","text":"prompt resource"}}}]}}' ;;
esac
done
"#,
        )
        .unwrap();
        let settings = Settings {
            raw: json!({
                "strictMcpConfig": true,
                "mcpServers": {
                    "mock": {"command": "/bin/sh", "args": [script], "roots": ["."]}
                }
            }),
        };
        let integration = connect_mcp(&settings, temp.path(), false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(integration.server_count, 1);
        for name in [
            "ListMcpResources",
            "ListMcpResourceTemplates",
            "ReadMcpResource",
            "ListMcpPrompts",
            "GetMcpPrompt",
        ] {
            let tool = integration
                .active_tools
                .iter()
                .find(|tool| tool.name() == name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(tool.read_only(&json!({})), "{name} must be read-only");
            assert!(
                tool.concurrency_safe(&json!({})),
                "{name} must be concurrency-safe"
            );
        }
        let external = integration
            .deferred_tools
            .iter()
            .find(|tool| tool.name() == "mcp__mock__echo")
            .expect("external MCP tool missing");
        assert!(!external.read_only(&json!({"text":"x"})));
        assert!(external.destructive(&json!({"text":"x"})));
        assert!(!external.concurrency_safe(&json!({"text":"x"})));
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
        context.set_user_interaction_handler(Some(Arc::new(|request| {
            assert_eq!(request.tool, "McpElicitation");
            assert_eq!(request.input["mcp_server_name"], "mock");
            Ok(json!({"action":"accept", "content":{"choice":"yes"}}))
        })));
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
        assert!(resources.content.contains("root-1"));
        assert!(resources.content.contains("local_resource"));
        assert!(!resources.content.contains("/private/project"));
        assert!(!resources.content.contains("token=hidden"));
        let resources_json: Value = serde_json::from_str(&resources.content).unwrap();
        let resource_handle = resources_json[0]["uri"].as_str().unwrap();
        assert!(resource_handle.starts_with("mcp-resource:"));
        let resource = registry
            .execute(
                &context,
                "ReadMcpResource",
                json!({"server":"mock","uri":resource_handle}),
            )
            .await;
        assert!(!resource.is_error, "{}", resource.content);
        assert!(resource.content.contains("direct-resource-body"));
        assert!(resource.content.contains("uriMetadata"));
        assert!(!resource.content.contains("/private/project"));
        assert!(!resource.content.contains("token=hidden"));
        let templates = registry
            .execute(
                &context,
                "ListMcpResourceTemplates",
                json!({"server":"mock"}),
            )
            .await;
        assert!(!templates.is_error, "{}", templates.content);
        assert!(templates.content.contains("uriTemplate"));
        assert!(templates.content.contains("\"name\""));
        assert!(!templates.content.contains("/private/"));
        assert!(!templates.content.contains("token=hidden"));
        let templates_json: Value = serde_json::from_str(&templates.content).unwrap();
        let template_handle = templates_json[0]["uriTemplate"].as_str().unwrap();
        assert!(template_handle.starts_with("mcp-template:"));
        let templated_resource = registry
            .execute(
                &context,
                "ReadMcpResource",
                json!({"server":"mock","uri":template_handle,"arguments":{"name":"item"}}),
            )
            .await;
        assert!(
            !templated_resource.is_error,
            "{}",
            templated_resource.content
        );
        assert!(
            templated_resource
                .content
                .contains("template-resource-body")
        );
        assert!(!templated_resource.content.contains("/private/"));
        assert!(!templated_resource.content.contains("token=hidden"));
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
        assert!(prompt.content.contains("uriMetadata"));
        assert!(!prompt.content.contains("/private/prompt"));
        assert!(!prompt.content.contains("token=hidden"));
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
