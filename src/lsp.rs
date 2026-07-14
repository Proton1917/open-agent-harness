use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
    time::timeout,
};
use url::Url;

use crate::{
    config::Settings,
    process::SecretEnvScrubber,
    rpc::{RpcFraming, RpcServerRequestHandler, StdioRpcClient, StdioRpcConfig},
    session::sanitize_transport_text,
    tools::{Tool, ToolContext, ToolOutput, ToolService, object_schema},
};

const MAX_SERVERS: usize = 32;
const MAX_FILE_BYTES: u64 = 10 * 1024 * 1024;
const MAX_SERVER_NAME_BYTES: usize = 128;
const MAX_COMMAND_BYTES: usize = 4096;
const MAX_ARGS: usize = 128;
const MAX_ARG_BYTES: usize = 32 * 1024;
const MAX_ENV_VARS: usize = 256;
const MAX_ENV_VALUE_BYTES: usize = 256 * 1024;
const MAX_EXTENSIONS: usize = 128;
const MAX_DIAGNOSTIC_FILES: usize = 128;
const MAX_DIAGNOSTICS_PER_FILE: usize = 10;
const MAX_DIAGNOSTICS_PER_RESULT: usize = 30;
const MAX_OPEN_DOCUMENTS_PER_SERVER: usize = 512;
const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 4096;
const MAX_DIAGNOSTIC_METADATA_BYTES: usize = 256;
const MAX_URI_BYTES: usize = 16 * 1024;
const OUTSIDE_WORKSPACE_URI: &str = "[outside-workspace-uri]";
const OUTSIDE_WORKSPACE_PATH: &str = "[outside-workspace-path]";
const DIAGNOSTIC_WAIT: Duration = Duration::from_millis(250);
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 120_000;
const MIN_REQUEST_TIMEOUT_MS: u64 = 1_000;
const MAX_REQUEST_TIMEOUT_MS: u64 = 600_000;
const MAX_RESTARTS: u8 = 3;

pub struct LspIntegration {
    pub deferred_tools: Vec<Arc<dyn Tool>>,
    pub service: Arc<dyn ToolService>,
    pub server_count: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServerConfig {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    cwd: Option<String>,
    #[serde(rename = "extensionToLanguage")]
    extension_to_language: BTreeMap<String, String>,
    #[serde(rename = "initializationOptions", default)]
    initialization_options: Value,
    #[serde(default)]
    settings: Value,
    #[serde(rename = "timeoutMs")]
    timeout_ms: Option<u64>,
    #[serde(rename = "maxRestarts", default)]
    max_restarts: u8,
    #[serde(default = "default_true")]
    diagnostics: bool,
    #[serde(default)]
    disabled: bool,
}

#[derive(Debug, Clone)]
struct ServerConfig {
    name: String,
    command: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: PathBuf,
    extension_to_language: BTreeMap<String, String>,
    initialization_options: Value,
    settings: Value,
    request_timeout: Duration,
    max_restarts: u8,
    diagnostics: bool,
    secret_env_scrubber: SecretEnvScrubber,
}

#[derive(Clone, Copy)]
struct DocumentState {
    version: i64,
    content_hash: u128,
}

struct LspClient {
    config: ServerConfig,
    rpc: Arc<StdioRpcClient>,
    documents: Arc<Mutex<HashMap<String, DocumentState>>>,
    diagnostics: Arc<Mutex<DiagnosticStore>>,
    diagnostic_notify: Arc<Notify>,
    event_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct DiagnosticStore {
    by_uri: BTreeMap<String, Vec<Value>>,
}

struct LspManager {
    workspace: PathBuf,
    configs: HashMap<String, ServerConfig>,
    extensions: HashMap<String, String>,
    clients: Mutex<HashMap<String, Arc<LspClient>>>,
    diagnostics: Arc<Mutex<DiagnosticStore>>,
    diagnostic_notify: Arc<Notify>,
    debug: bool,
}

pub fn configure_lsp(
    settings: &Settings,
    workspace: &Path,
    debug: bool,
) -> Result<Option<LspIntegration>> {
    let (configs, extensions) = parse_server_configs(settings, workspace)?;
    if configs.is_empty() {
        return Ok(None);
    }
    let server_count = configs.len();
    let manager = Arc::new(LspManager {
        workspace: std::fs::canonicalize(workspace)
            .with_context(|| format!("无法解析 LSP workspace: {}", workspace.display()))?,
        configs,
        extensions,
        clients: Mutex::new(HashMap::new()),
        diagnostics: Arc::new(Mutex::new(DiagnosticStore::default())),
        diagnostic_notify: Arc::new(Notify::new()),
        debug,
    });
    let tool: Arc<dyn Tool> = Arc::new(LspTool {
        manager: Arc::clone(&manager),
    });
    let service: Arc<dyn ToolService> = manager;
    Ok(Some(LspIntegration {
        deferred_tools: vec![tool],
        service,
        server_count,
    }))
}

fn parse_server_configs(
    settings: &Settings,
    workspace: &Path,
) -> Result<(HashMap<String, ServerConfig>, HashMap<String, String>)> {
    let Some(raw_servers) = settings.raw.get("lspServers") else {
        return Ok((HashMap::new(), HashMap::new()));
    };
    let raw_servers = raw_servers
        .as_object()
        .context("lspServers 必须是 JSON object")?;
    let secret_env_scrubber = SecretEnvScrubber::from_settings(settings)?;
    if raw_servers.len() > MAX_SERVERS {
        bail!("lspServers 超过 {MAX_SERVERS} 个限制")
    }
    let mut configs = HashMap::new();
    let mut extensions = HashMap::new();
    for (name, value) in raw_servers {
        if name.is_empty() || name.len() > MAX_SERVER_NAME_BYTES {
            bail!("LSP server 名称长度无效: {name:?}")
        }
        let raw: RawServerConfig = serde_json::from_value(value.clone())
            .with_context(|| format!("LSP server {name} 配置无效"))?;
        if raw.disabled {
            continue;
        }
        validate_server_config(name, &raw)?;
        let cwd = resolve_directory(raw.cwd.as_deref(), workspace)
            .with_context(|| format!("LSP server {name} cwd 无效"))?;
        let mut language_map = BTreeMap::new();
        for (extension, language) in raw.extension_to_language {
            let extension = normalize_extension(&extension)?;
            if let Some(previous) = extensions.insert(extension.clone(), name.clone()) {
                bail!("LSP extension {extension} 同时由 {previous} 与 {name} 配置")
            }
            language_map.insert(extension, language);
        }
        let timeout_ms = raw
            .timeout_ms
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS)
            .clamp(MIN_REQUEST_TIMEOUT_MS, MAX_REQUEST_TIMEOUT_MS);
        configs.insert(
            name.clone(),
            ServerConfig {
                name: name.clone(),
                command: raw.command,
                args: raw.args,
                env: raw.env,
                cwd,
                extension_to_language: language_map,
                initialization_options: raw.initialization_options,
                settings: raw.settings,
                request_timeout: Duration::from_millis(timeout_ms),
                max_restarts: raw.max_restarts.min(MAX_RESTARTS),
                diagnostics: raw.diagnostics,
                secret_env_scrubber: secret_env_scrubber.clone(),
            },
        );
    }
    Ok((configs, extensions))
}

fn validate_server_config(name: &str, config: &RawServerConfig) -> Result<()> {
    if config.command.trim().is_empty()
        || config.command.len() > MAX_COMMAND_BYTES
        || config.command.contains('\0')
    {
        bail!("LSP server {name} command 为空、过长或包含 NUL")
    }
    if config.args.len() > MAX_ARGS {
        bail!("LSP server {name} args 超过 {MAX_ARGS} 项限制")
    }
    for argument in &config.args {
        if argument.len() > MAX_ARG_BYTES || argument.contains('\0') {
            bail!("LSP server {name} argument 过长或包含 NUL")
        }
    }
    if config.env.len() > MAX_ENV_VARS {
        bail!("LSP server {name} env 超过 {MAX_ENV_VARS} 项限制")
    }
    for (key, value) in &config.env {
        if !valid_environment_key(key) || value.len() > MAX_ENV_VALUE_BYTES || value.contains('\0')
        {
            bail!("LSP server {name} env entry 无效: {key:?}")
        }
    }
    if config.extension_to_language.is_empty()
        || config.extension_to_language.len() > MAX_EXTENSIONS
    {
        bail!("LSP server {name} extensionToLanguage 为空或超过限制")
    }
    for language in config.extension_to_language.values() {
        if language.is_empty() || language.len() > 128 {
            bail!("LSP server {name} language id 无效")
        }
    }
    Ok(())
}

fn valid_environment_key(key: &str) -> bool {
    !key.is_empty()
        && key.bytes().enumerate().all(|(index, byte)| {
            matches!(
                (index, byte),
                (0, b'A'..=b'Z' | b'a'..=b'z' | b'_')
                    | (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
            )
        })
}

fn resolve_directory(configured: Option<&str>, workspace: &Path) -> Result<PathBuf> {
    let path = configured.map_or_else(|| workspace.to_owned(), PathBuf::from);
    let path = if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    };
    let path = std::fs::canonicalize(&path)?;
    if !path.is_dir() {
        bail!("{} 不是目录", path.display())
    }
    Ok(path)
}

fn normalize_extension(extension: &str) -> Result<String> {
    let extension = extension.trim().to_ascii_lowercase();
    if extension.is_empty() || extension.len() > 32 || extension.contains(['/', '\\', '\0']) {
        bail!("无效 LSP extension: {extension:?}")
    }
    Ok(if extension.starts_with('.') {
        extension
    } else {
        format!(".{extension}")
    })
}

impl LspClient {
    async fn connect(
        config: ServerConfig,
        workspace: &Path,
        diagnostics: Arc<Mutex<DiagnosticStore>>,
        diagnostic_notify: Arc<Notify>,
    ) -> Result<Arc<Self>> {
        let root_uri = file_uri(workspace)?;
        let workspace_folders = json!([{"uri": root_uri, "name": workspace.file_name().and_then(|name| name.to_str()).unwrap_or("workspace")}]);
        let settings = config.settings.clone();
        let folders_for_handler = workspace_folders.clone();
        let handler: RpcServerRequestHandler = Arc::new(move |method, params| match method {
            "workspace/configuration" => {
                let count = params
                    .and_then(|value| value.get("items"))
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                Some(Value::Array(vec![settings.clone(); count]))
            }
            "workspace/workspaceFolders" => Some(folders_for_handler.clone()),
            "client/registerCapability"
            | "client/unregisterCapability"
            | "window/workDoneProgress/create" => Some(json!({})),
            "workspace/applyEdit" => Some(json!({
                "applied": false,
                "failureReason": "Workspace edits initiated by an LSP server are not authorized"
            })),
            _ => None,
        });
        let rpc = Arc::new(
            StdioRpcClient::spawn_with_secret_env_scrubber(
                StdioRpcConfig {
                    label: format!("LSP/{}", config.name),
                    command: config.command.clone(),
                    args: config.args.clone(),
                    env: config.env.clone(),
                    cwd: config.cwd.clone(),
                    framing: RpcFraming::ContentLength,
                    request_timeout: config.request_timeout,
                    server_request_handler: Some(handler),
                },
                config.secret_env_scrubber.clone(),
            )
            .await?,
        );
        rpc.request(
            "initialize",
            Some(json!({
                "processId": std::process::id(),
                "clientInfo": {"name": "open-agent-harness", "version": env!("CARGO_PKG_VERSION")},
                "rootUri": root_uri,
                "workspaceFolders": workspace_folders,
                "initializationOptions": config.initialization_options,
                "capabilities": {
                    "workspace": {"symbol": {}, "workspaceFolders": true, "configuration": true},
                    "textDocument": {
                        "definition": {}, "references": {}, "hover": {}, "documentSymbol": {},
                        "implementation": {}, "callHierarchy": {}, "rename": {},
                        "diagnostic": {},
                        "synchronization": {"didSave": true, "dynamicRegistration": false}
                    }
                }
            })),
        )
        .await
        .with_context(|| format!("LSP server {} initialize 失败", config.name))?;
        rpc.notify("initialized", Some(json!({}))).await?;
        if !config.settings.is_null() {
            rpc.notify(
                "workspace/didChangeConfiguration",
                Some(json!({"settings": config.settings})),
            )
            .await?;
        }

        let documents = Arc::new(Mutex::new(HashMap::new()));
        let client = Arc::new(Self {
            config,
            rpc,
            documents: Arc::clone(&documents),
            diagnostics: Arc::clone(&diagnostics),
            diagnostic_notify,
            event_task: Mutex::new(None),
        });
        let mut events = client.rpc.subscribe();
        let weak = Arc::downgrade(&client);
        let task = tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                if event.get("method").and_then(Value::as_str)
                    != Some("textDocument/publishDiagnostics")
                {
                    continue;
                }
                let Some(client) = weak.upgrade() else {
                    return;
                };
                if client.config.diagnostics {
                    client.record_diagnostics(event.get("params")).await;
                }
            }
        });
        *client.event_task.lock().await = Some(task);
        Ok(client)
    }

    async fn sync_document(&self, path: &Path, text: &str) -> Result<()> {
        let uri = file_uri(path)?;
        let extension = extension_of(path)?;
        let language_id = self
            .config
            .extension_to_language
            .get(&extension)
            .map(String::as_str)
            .unwrap_or("plaintext");
        let content_hash = hash_bytes(text.as_bytes());
        let mut documents = self.documents.lock().await;
        if !documents.contains_key(&uri) && documents.len() >= MAX_OPEN_DOCUMENTS_PER_SERVER {
            bail!("LSP open document 达到 {MAX_OPEN_DOCUMENTS_PER_SERVER} 个限制")
        }
        match documents.get_mut(&uri) {
            None => {
                self.rpc
                    .notify(
                        "textDocument/didOpen",
                        Some(json!({
                            "textDocument": {"uri": uri, "languageId": language_id, "version": 1, "text": text}
                        })),
                    )
                    .await?;
                documents.insert(
                    uri,
                    DocumentState {
                        version: 1,
                        content_hash,
                    },
                );
            }
            Some(state) if state.content_hash != content_hash => {
                state.version = state.version.saturating_add(1);
                state.content_hash = content_hash;
                self.rpc
                    .notify(
                        "textDocument/didChange",
                        Some(json!({
                            "textDocument": {"uri": uri, "version": state.version},
                            "contentChanges": [{"text": text}]
                        })),
                    )
                    .await?;
                self.rpc
                    .notify(
                        "textDocument/didSave",
                        Some(json!({"textDocument": {"uri": uri}})),
                    )
                    .await?;
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn record_diagnostics(&self, params: Option<&Value>) {
        let Some(params) = params else {
            return;
        };
        let Some(uri) = params.get("uri").and_then(Value::as_str) else {
            return;
        };
        if uri.len() > MAX_URI_BYTES {
            return;
        }
        let documents = self.documents.lock().await;
        let Some(document) = documents.get(uri) else {
            // A language server is not allowed to inject diagnostics for files the
            // harness did not explicitly open for this client.
            return;
        };
        if params
            .get("version")
            .and_then(Value::as_i64)
            .is_some_and(|version| version < document.version)
        {
            return;
        }
        drop(documents);
        let diagnostics = params
            .get("diagnostics")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(validate_diagnostic)
            .take(MAX_DIAGNOSTICS_PER_FILE)
            .collect::<Vec<_>>();
        let mut store = self.diagnostics.lock().await;
        if store.by_uri.len() >= MAX_DIAGNOSTIC_FILES && !store.by_uri.contains_key(uri) {
            store.by_uri.pop_first();
        }
        if diagnostics.is_empty() {
            store.by_uri.remove(uri);
        } else {
            store.by_uri.insert(uri.to_owned(), diagnostics);
        }
        drop(store);
        self.diagnostic_notify.notify_waiters();
    }

    async fn shutdown(&self) {
        let _ = timeout(Duration::from_secs(2), self.rpc.request("shutdown", None)).await;
        let _ = self.rpc.notify("exit", None).await;
        if let Some(task) = self.event_task.lock().await.take() {
            task.abort();
        }
        self.rpc.shutdown().await;
    }
}

impl LspManager {
    fn server_name_for_path(&self, path: &Path) -> Result<Option<String>> {
        let Ok(extension) = extension_of(path) else {
            return Ok(None);
        };
        Ok(self.extensions.get(&extension).cloned())
    }

    async fn sync_changed_files(&self, paths: &[PathBuf]) -> Result<Vec<String>> {
        let mut feedback = Vec::new();
        for path in paths {
            let canonical = match std::fs::canonicalize(path) {
                Ok(path) => path,
                // File mutation tools currently create or replace files. A
                // future delete tool may legitimately leave no document to
                // synchronize, so disappearance is not an LSP failure.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if !canonical.starts_with(&self.workspace) {
                bail!("LSP file-change hint escaped the configured workspace")
            }
            let metadata = std::fs::symlink_metadata(&canonical)?;
            if !metadata.is_file() {
                continue;
            }
            if metadata.len() > MAX_FILE_BYTES {
                continue;
            }
            let Some((_name, client)) = self.client_for_path(&canonical).await? else {
                continue;
            };
            let bytes = std::fs::read(&canonical)?;
            if bytes.len() as u64 > MAX_FILE_BYTES {
                continue;
            }
            let Ok(text) = String::from_utf8(bytes) else {
                // LSP text synchronization is undefined for binary data.
                continue;
            };
            client.sync_document(&canonical, &text).await?;
            let diagnostics = self.take_diagnostics_for_uri(&file_uri(&canonical)?).await;
            if diagnostics
                .as_array()
                .is_some_and(|items| !items.is_empty())
            {
                let relative = canonical
                    .strip_prefix(&self.workspace)
                    .unwrap_or(&canonical)
                    .to_string_lossy()
                    .replace('\\', "/");
                feedback.push(serde_json::to_string(&json!({
                    "source": "lsp_diagnostics",
                    "file": relative,
                    "diagnostics": diagnostics,
                }))?);
            }
        }
        Ok(feedback)
    }

    async fn client_for_path(&self, path: &Path) -> Result<Option<(String, Arc<LspClient>)>> {
        let Some(name) = self.server_name_for_path(path)? else {
            return Ok(None);
        };
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&name) {
            return Ok(Some((name, Arc::clone(client))));
        }
        let config = self
            .configs
            .get(&name)
            .cloned()
            .with_context(|| format!("LSP server config 消失: {name}"))?;
        let client = LspClient::connect(
            config,
            &self.workspace,
            Arc::clone(&self.diagnostics),
            Arc::clone(&self.diagnostic_notify),
        )
        .await?;
        clients.insert(name.clone(), Arc::clone(&client));
        Ok(Some((name, client)))
    }

    async fn restart(&self, name: &str) -> bool {
        let client = { self.clients.lock().await.remove(name) };
        if let Some(client) = client {
            let uris = client
                .documents
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            let mut diagnostics = self.diagnostics.lock().await;
            for uri in uris {
                diagnostics.by_uri.remove(&uri);
            }
            drop(diagnostics);
            client.shutdown().await;
            true
        } else {
            false
        }
    }

    async fn take_diagnostics_for_uri(&self, uri: &str) -> Value {
        for attempt in 0..2 {
            if let Some(diagnostics) = self.diagnostics.lock().await.by_uri.remove(uri) {
                return Value::Array(diagnostics);
            }
            if attempt == 0 {
                let _ = timeout(DIAGNOSTIC_WAIT, self.diagnostic_notify.notified()).await;
            }
        }
        Value::Array(Vec::new())
    }

    async fn take_diagnostics(&self) -> Value {
        let mut store = self.diagnostics.lock().await;
        let mut files = Vec::new();
        let mut remaining = MAX_DIAGNOSTICS_PER_RESULT;
        for (uri, diagnostics) in std::mem::take(&mut store.by_uri) {
            if remaining == 0 {
                break;
            }
            let diagnostics = diagnostics.into_iter().take(remaining).collect::<Vec<_>>();
            remaining = remaining.saturating_sub(diagnostics.len());
            files.push(json!({"uri": uri, "diagnostics": diagnostics}));
        }
        Value::Array(files)
    }

    async fn shutdown_all(&self) {
        let clients = self
            .clients
            .lock()
            .await
            .drain()
            .map(|(_, client)| client)
            .collect::<Vec<_>>();
        for client in clients {
            client.shutdown().await;
        }
    }
}

#[async_trait]
impl ToolService for LspManager {
    async fn files_changed(&self, paths: &[PathBuf]) -> Result<Vec<String>> {
        self.sync_changed_files(paths).await
    }

    async fn shutdown(&self) {
        self.shutdown_all().await;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LspInput {
    operation: String,
    file_path: String,
    line: Option<u64>,
    character: Option<u64>,
    query: Option<String>,
    new_name: Option<String>,
}

struct LspTool {
    manager: Arc<LspManager>,
}

#[async_trait]
impl Tool for LspTool {
    fn name(&self) -> &str {
        "LSP"
    }

    fn description(&self) -> &str {
        "Queries user-configured local language servers for navigation, symbols, diagnostics, rename previews, and controlled restart. Rename returns a WorkspaceEdit preview and never applies it."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "operation": {"type": "string", "enum": [
                    "goToDefinition", "findReferences", "hover", "documentSymbol",
                    "workspaceSymbol", "goToImplementation", "prepareCallHierarchy",
                    "incomingCalls", "outgoingCalls", "diagnostics", "rename", "restart"
                ]},
                "filePath": {"type": "string", "minLength": 1, "maxLength": 16384},
                "line": {"type": "integer", "minimum": 1},
                "character": {"type": "integer", "minimum": 1},
                "query": {"type": "string", "maxLength": 4096},
                "newName": {"type": "string", "minLength": 1, "maxLength": 1024}
            }),
            &["operation", "filePath"],
        )
    }

    fn read_only(&self, input: &Value) -> bool {
        input.get("operation").and_then(Value::as_str) != Some("restart")
    }

    fn path_fields(&self) -> &'static [&'static str] {
        &["filePath"]
    }

    fn summary(&self, input: &Value) -> String {
        format!(
            "{} {}",
            input
                .get("operation")
                .and_then(Value::as_str)
                .unwrap_or("<operation>"),
            input
                .get("filePath")
                .and_then(Value::as_str)
                .unwrap_or("<file>")
        )
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: LspInput = serde_json::from_value(input)?;
        let path = std::fs::canonicalize(context.resolve_path(&input.file_path)?)
            .with_context(|| format!("无法解析 LSP file: {}", input.file_path))?;
        let metadata = std::fs::metadata(&path)?;
        if !metadata.is_file() {
            bail!("LSP path 不是文件: {}", path.display())
        }
        if metadata.len() > MAX_FILE_BYTES {
            bail!("LSP file 超过 {MAX_FILE_BYTES} 字节限制")
        }
        let text = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("LSP file 不是有效 UTF-8: {}", path.display()))?;
        let config = self
            .manager
            .server_name_for_path(&path)?
            .and_then(|name| self.manager.configs.get(&name))
            .cloned();
        let Some(config) = config else {
            return Ok(ToolOutput::error(format!(
                "没有为 {} 配置 LSP server",
                extension_of(&path)?
            )));
        };
        if input.operation == "restart" {
            let restarted = self.manager.restart(&config.name).await;
            let output = sanitize_lsp_value(
                json!({
                "operation": "restart",
                "filePath": input.file_path,
                "server": config.name,
                "restarted": restarted
                }),
                &self.manager.workspace,
            );
            return Ok(ToolOutput::success(serde_json::to_string_pretty(&output)?));
        }
        let mut last_error = None;
        for attempt in 0..=config.max_restarts {
            let Some((name, client)) = self.manager.client_for_path(&path).await? else {
                bail!("LSP server mapping 在执行时消失")
            };
            let result = async {
                client.sync_document(&path, &text).await?;
                if input.operation == "diagnostics" {
                    return Ok(self
                        .manager
                        .take_diagnostics_for_uri(&file_uri(&path)?)
                        .await);
                }
                execute_operation(&client, &path, &input).await
            }
            .await;
            match result {
                Ok(result) => {
                    let result = if input.operation == "rename" {
                        json!({"workspaceEdit": result, "applied": false})
                    } else {
                        result
                    };
                    let diagnostics = self.manager.take_diagnostics().await;
                    let output = sanitize_lsp_value(
                        json!({
                            "operation": input.operation,
                            "filePath": input.file_path,
                            "result": result,
                            "diagnostics": diagnostics,
                        }),
                        &self.manager.workspace,
                    );
                    return Ok(ToolOutput::success(serde_json::to_string_pretty(&output)?));
                }
                Err(error) if attempt < config.max_restarts => {
                    if self.manager.debug {
                        eprintln!(
                            "LSP request failed for {name}; restarting ({}/{}): {error:#}",
                            attempt + 1,
                            config.max_restarts
                        );
                    }
                    last_error = Some(error);
                    self.manager.restart(&name).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("LSP request 失败")))
    }
}

async fn execute_operation(client: &LspClient, path: &Path, input: &LspInput) -> Result<Value> {
    let uri = file_uri(path)?;
    let text_document = json!({"uri": uri});
    let (method, params) = match input.operation.as_str() {
        "goToDefinition" => (
            "textDocument/definition",
            json!({"textDocument": text_document, "position": position(input)?}),
        ),
        "findReferences" => (
            "textDocument/references",
            json!({"textDocument": text_document, "position": position(input)?, "context": {"includeDeclaration": true}}),
        ),
        "hover" => (
            "textDocument/hover",
            json!({"textDocument": text_document, "position": position(input)?}),
        ),
        "documentSymbol" => (
            "textDocument/documentSymbol",
            json!({"textDocument": text_document}),
        ),
        "workspaceSymbol" => (
            "workspace/symbol",
            json!({"query": input.query.as_deref().unwrap_or("")}),
        ),
        "rename" => {
            let new_name = input
                .new_name
                .as_deref()
                .context("rename operation 需要 newName")?;
            if new_name.len() > 1024 || new_name.chars().any(|character| character.is_control()) {
                bail!("rename newName 过长或包含控制字符")
            }
            (
                "textDocument/rename",
                json!({
                    "textDocument": text_document,
                    "position": position(input)?,
                    "newName": new_name
                }),
            )
        }
        "goToImplementation" => (
            "textDocument/implementation",
            json!({"textDocument": text_document, "position": position(input)?}),
        ),
        "prepareCallHierarchy" | "incomingCalls" | "outgoingCalls" => (
            "textDocument/prepareCallHierarchy",
            json!({"textDocument": text_document, "position": position(input)?}),
        ),
        _ => bail!("不支持的 LSP operation: {}", input.operation),
    };
    let result = client.rpc.request(method, Some(params)).await?;
    if !matches!(input.operation.as_str(), "incomingCalls" | "outgoingCalls") {
        return Ok(result);
    }
    let Some(item) = result.as_array().and_then(|items| items.first()).cloned() else {
        return Ok(Value::Array(Vec::new()));
    };
    let method = if input.operation == "incomingCalls" {
        "callHierarchy/incomingCalls"
    } else {
        "callHierarchy/outgoingCalls"
    };
    client
        .rpc
        .request(method, Some(json!({"item": item})))
        .await
}

fn position(input: &LspInput) -> Result<Value> {
    let line = input.line.context("此 LSP operation 需要 line")?;
    let character = input.character.context("此 LSP operation 需要 character")?;
    Ok(json!({
        "line": line.saturating_sub(1),
        "character": character.saturating_sub(1)
    }))
}

fn extension_of(path: &Path) -> Result<String> {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| format!(".{}", extension.to_ascii_lowercase()))
        .context("文件没有可识别的 UTF-8 extension")
}

fn file_uri(path: &Path) -> Result<String> {
    Url::from_file_path(path)
        .map(String::from)
        .map_err(|_| anyhow::anyhow!("无法将路径转换为 file URI: {}", path.display()))
}

fn hash_bytes(bytes: &[u8]) -> u128 {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u128::from(*byte)).wrapping_mul(PRIME)
    })
}

fn validate_diagnostic(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let message = object.get("message")?.as_str()?;
    if message.is_empty() || message.len() > MAX_DIAGNOSTIC_MESSAGE_BYTES {
        return None;
    }
    let range = sanitize_range(object.get("range")?)?;
    let mut cleaned = json!({"message": message, "range": range});
    if let Some(severity) = object
        .get("severity")
        .and_then(Value::as_u64)
        .filter(|severity| (1..=4).contains(severity))
    {
        cleaned["severity"] = json!(severity);
    }
    if let Some(code) = object.get("code").filter(|code| {
        code.is_number()
            || code
                .as_str()
                .is_some_and(|code| code.len() <= MAX_DIAGNOSTIC_METADATA_BYTES)
    }) {
        cleaned["code"] = code.clone();
    }
    if let Some(source) = object
        .get("source")
        .and_then(Value::as_str)
        .filter(|source| source.len() <= MAX_DIAGNOSTIC_METADATA_BYTES)
    {
        cleaned["source"] = Value::String(source.to_owned());
    }
    if let Some(tags) = object.get("tags").and_then(Value::as_array) {
        cleaned["tags"] = Value::Array(
            tags.iter()
                .filter_map(Value::as_u64)
                .filter(|tag| matches!(tag, 1 | 2))
                .take(8)
                .map(Value::from)
                .collect(),
        );
    }
    Some(cleaned)
}

fn sanitize_range(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let position = |name: &str| {
        let value = object.get(name)?.as_object()?;
        let line = value.get("line")?.as_u64()?;
        let character = value.get("character")?.as_u64()?;
        (line <= u64::from(u32::MAX) && character <= u64::from(u32::MAX))
            .then(|| json!({"line": line, "character": character}))
    };
    Some(json!({"start": position("start")?, "end": position("end")?}))
}

fn sanitize_lsp_value(value: Value, workspace: &Path) -> Value {
    match value {
        Value::Object(object) => {
            let mut sanitized = serde_json::Map::new();
            for (key, child) in object {
                if key == "data" {
                    continue;
                }
                let base_key = sanitize_lsp_string(&key, workspace);
                let mut output_key = base_key.clone();
                let mut collision = 2_usize;
                while sanitized.contains_key(&output_key) {
                    output_key = format!("{base_key}#{collision}");
                    collision += 1;
                }
                let child = if matches!(key.as_str(), "line" | "character") {
                    child
                        .as_u64()
                        .map(|value| Value::from(value.saturating_add(1)))
                        .unwrap_or(child)
                } else {
                    child
                };
                sanitized.insert(output_key, sanitize_lsp_value(child, workspace));
            }
            Value::Object(sanitized)
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(|child| sanitize_lsp_value(child, workspace))
                .collect(),
        ),
        Value::String(text) => Value::String(sanitize_lsp_string(&text, workspace)),
        Value::Null | Value::Bool(_) | Value::Number(_) => value,
    }
}

fn sanitize_lsp_string(value: &str, workspace: &Path) -> String {
    if value.starts_with("file://") {
        return sanitize_file_uri(value, workspace);
    }
    let path = Path::new(value);
    if path.is_absolute() {
        return sanitize_absolute_lsp_path(path, workspace);
    }

    let mut sanitized = replace_embedded_file_uris(value, workspace);
    if let Some(workspace) = workspace.to_str() {
        sanitized = sanitized.replace(workspace, ".");
    }
    sanitize_transport_text(&sanitized, workspace)
}

fn sanitize_file_uri(value: &str, workspace: &Path) -> String {
    let Ok(uri) = Url::parse(value) else {
        return OUTSIDE_WORKSPACE_URI.to_owned();
    };
    if uri.scheme() != "file" {
        return sanitize_transport_text(value, workspace);
    }
    let Ok(path) = uri.to_file_path() else {
        return OUTSIDE_WORKSPACE_URI.to_owned();
    };
    workspace_relative_path(&path, workspace).unwrap_or_else(|| OUTSIDE_WORKSPACE_URI.to_owned())
}

fn sanitize_absolute_lsp_path(path: &Path, workspace: &Path) -> String {
    workspace_relative_path(path, workspace).unwrap_or_else(|| OUTSIDE_WORKSPACE_PATH.to_owned())
}

fn workspace_relative_path(path: &Path, workspace: &Path) -> Option<String> {
    let relative = platform_relative_path(path, workspace).or_else(|| {
        let path = std::fs::canonicalize(path).ok()?;
        let workspace = std::fs::canonicalize(workspace).ok()?;
        platform_relative_path(&path, &workspace)
    })?;
    if relative.components().any(|component| {
        matches!(
            component,
            std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_)
        )
    }) {
        return None;
    }
    if relative.as_os_str().is_empty() {
        Some(".".to_owned())
    } else {
        Some(relative.to_string_lossy().replace('\\', "/"))
    }
}

#[cfg(not(windows))]
fn platform_relative_path(path: &Path, workspace: &Path) -> Option<PathBuf> {
    path.strip_prefix(workspace).ok().map(Path::to_path_buf)
}

#[cfg(windows)]
fn platform_relative_path(path: &Path, workspace: &Path) -> Option<PathBuf> {
    let path = normalize_windows_file_path(path)?;
    let workspace = normalize_windows_file_path(workspace)?;
    path.strip_prefix(workspace).ok().map(Path::to_path_buf)
}

#[cfg(windows)]
fn normalize_windows_file_path(path: &Path) -> Option<PathBuf> {
    let normalized = path.to_str()?.replace('\\', "/");
    let normalized = if let Some(local) = normalized.strip_prefix("//?/") {
        if let Some(unc) = local.strip_prefix("UNC/") {
            format!("//{unc}")
        } else if local.as_bytes().get(1) == Some(&b':') {
            local.to_owned()
        } else {
            return None;
        }
    } else {
        normalized
    };
    Some(PathBuf::from(normalized))
}

fn replace_embedded_file_uris(value: &str, workspace: &Path) -> String {
    let mut remaining = value;
    let mut output = String::with_capacity(value.len());
    while let Some(start) = remaining.find("file://") {
        output.push_str(&remaining[..start]);
        let candidate = &remaining[start..];
        let end = candidate
            .find(|character: char| {
                character.is_whitespace()
                    || matches!(
                        character,
                        '"' | '\'' | '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}' | ','
                    )
            })
            .unwrap_or(candidate.len());
        output.push_str(&sanitize_file_uri(&candidate[..end], workspace));
        remaining = &candidate[end..];
    }
    output.push_str(remaining);
    output
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::{ToolContext, ToolRegistry},
    };

    #[test]
    fn duplicate_extension_owners_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let settings = Settings {
            raw: json!({"lspServers": {
                "one": {"command": "one", "extensionToLanguage": {"rs": "rust"}},
                "two": {"command": "two", "extensionToLanguage": {".rs": "rust"}}
            }}),
        };
        let error = parse_server_configs(&settings, temp.path()).unwrap_err();
        assert!(error.to_string().contains("同时"));
    }

    #[test]
    fn diagnostic_volume_and_metadata_are_bounded() {
        let valid = json!({
            "message": "problem",
            "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}},
            "data": {"hidden": true},
            "source": "x".repeat(MAX_DIAGNOSTIC_METADATA_BYTES + 1),
            "relatedInformation": [{"message": "unbounded metadata is dropped"}]
        });
        let cleaned = validate_diagnostic(&valid).unwrap();
        assert!(cleaned.get("data").is_none());
        assert!(cleaned.get("source").is_none());
        assert!(cleaned.get("relatedInformation").is_none());
        assert_eq!(cleaned["message"], "problem");
    }

    #[test]
    fn lsp_paths_are_workspace_relative_or_explicitly_redacted() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(workspace.path().join("src")).unwrap();
        let inside = workspace.path().join("src/main.rs");
        std::fs::write(&inside, "fn main() {}\n").unwrap();
        std::fs::write(workspace.path().join("src/lib.rs"), "").unwrap();
        let canonical_workspace = std::fs::canonicalize(workspace.path()).unwrap();
        let inside_uri = file_uri(&inside).unwrap();
        let outside = std::env::temp_dir().join("outside-private.rs");
        let outside_uri = file_uri(&outside).unwrap();
        let value = json!({
            "uri": inside_uri,
            "absolute": inside,
            "outsideUri": outside_uri,
            "outsidePath": outside,
            "changes": {
                file_uri(&workspace.path().join("src/lib.rs")).unwrap(): [],
                "file:///definitely/outside.rs": []
            },
            "hover": format!("defined at {} and file:///definitely/outside.rs", canonical_workspace.display()),
            "data": {"private": true}
        });
        let sanitized = sanitize_lsp_value(value, &canonical_workspace);
        let encoded = serde_json::to_string(&sanitized).unwrap();
        assert_eq!(sanitized["uri"], "src/main.rs");
        assert_eq!(sanitized["absolute"], "src/main.rs");
        assert_eq!(sanitized["outsideUri"], OUTSIDE_WORKSPACE_URI);
        assert_eq!(sanitized["outsidePath"], OUTSIDE_WORKSPACE_PATH);
        assert!(sanitized["changes"].get("src/lib.rs").is_some());
        assert!(sanitized["changes"].get(OUTSIDE_WORKSPACE_URI).is_some());
        assert!(sanitized.get("data").is_none());
        assert!(!encoded.contains("file://"));
        assert!(!encoded.contains(canonical_workspace.to_string_lossy().as_ref()));
        let position = sanitize_lsp_value(
            json!({"range":{"start":{"line":0,"character":0},"end":{"line":4,"character":8}}}),
            &canonical_workspace,
        );
        assert_eq!(position["range"]["start"]["line"], 1);
        assert_eq!(position["range"]["start"]["character"], 1);
        assert_eq!(position["range"]["end"]["line"], 5);
        assert_eq!(position["range"]["end"]["character"], 9);
    }

    #[tokio::test]
    async fn configured_server_handles_definition_request() {
        let workspace = tempfile::tempdir().unwrap();
        let server = tempfile::tempdir().unwrap();
        let source = server.path().join("mock_lsp.rs");
        let binary = server
            .path()
            .join(format!("mock_lsp{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(
            &source,
            r#"use std::io::{self, BufRead, Read, Write};
fn send(value: &str) {
    let mut stdout = io::stdout().lock();
    write!(stdout, "Content-Length: {}\r\n\r\n{}", value.len(), value).unwrap();
    stdout.flush().unwrap();
}
fn request_id(body: &str) -> &str {
    let rest = body.split("\"id\":").nth(1).unwrap();
    let end = rest.find([',', '}']).unwrap();
    &rest[..end]
}
fn string_field<'a>(body: &'a str, name: &str) -> &'a str {
    let marker = format!("\"{}\":\"", name);
    let rest = body.split(&marker).nth(1).unwrap();
    &rest[..rest.find('"').unwrap()]
}
fn reply(body: &str, result: &str) {
    send(&format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{}}}",
        request_id(body), result
    ));
}
fn main() {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    loop {
        let mut length = None;
        loop {
            let mut line = String::new();
            if input.read_line(&mut line).unwrap() == 0 { return; }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() { break; }
            if let Some(value) = line.strip_prefix("Content-Length:") {
                length = Some(value.trim().parse::<usize>().unwrap());
            }
        }
        let mut body = vec![0; length.unwrap()];
        input.read_exact(&mut body).unwrap();
        let body = String::from_utf8(body).unwrap();
        if body.contains("\"method\":\"initialize\"") {
            reply(&body, "{\"capabilities\":{},\"serverInfo\":{\"name\":\"mock\",\"version\":\"1\"}}");
        } else if body.contains("\"method\":\"textDocument/didOpen\"") {
            let uri = string_field(&body, "uri");
            send("{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/publishDiagnostics\",\"params\":{\"uri\":\"file:///not-opened-private.txt\",\"diagnostics\":[{\"message\":\"injected diagnostic\",\"range\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}}}]}}");
            send(&format!("{{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/publishDiagnostics\",\"params\":{{\"uri\":\"{}\",\"version\":1,\"diagnostics\":[{{\"message\":\"mock warning\",\"severity\":2,\"range\":{{\"start\":{{\"line\":0,\"character\":0}},\"end\":{{\"line\":0,\"character\":1}}}}}}]}}}}", uri));
        } else if body.contains("\"method\":\"textDocument/didChange\"") {
            let uri = string_field(&body, "uri");
            send(&format!("{{\"jsonrpc\":\"2.0\",\"method\":\"textDocument/publishDiagnostics\",\"params\":{{\"uri\":\"{}\",\"version\":2,\"diagnostics\":[{{\"message\":\"passive edit warning\",\"severity\":2,\"range\":{{\"start\":{{\"line\":0,\"character\":0}},\"end\":{{\"line\":0,\"character\":1}}}}}}]}}}}", uri));
        } else if body.contains("\"method\":\"textDocument/definition\"") {
            let uri = string_field(&body, "uri");
            if body.contains("\"line\":0") && body.contains("\"character\":0") {
                reply(&body, &format!("[{{\"uri\":\"{}\",\"range\":{{\"start\":{{\"line\":0,\"character\":0}},\"end\":{{\"line\":0,\"character\":1}}}}}}]", uri));
            } else {
                reply(&body, "null");
            }
        } else if body.contains("\"method\":\"textDocument/documentSymbol\"") {
            reply(&body, "[{\"name\":\"Demo\",\"kind\":12,\"range\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}},\"selectionRange\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}}}]");
        } else if body.contains("\"method\":\"workspace/symbol\"") {
            reply(&body, "[{\"name\":\"WorkspaceDemo\",\"kind\":12,\"location\":{\"uri\":\"file:///mock.txt\",\"range\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}}}}]");
        } else if body.contains("\"method\":\"textDocument/rename\"") {
            let uri = string_field(&body, "uri");
            reply(&body, &format!("{{\"changes\":{{\"{}\":[{{\"range\":{{\"start\":{{\"line\":0,\"character\":0}},\"end\":{{\"line\":0,\"character\":1}}}},\"newText\":\"Renamed\"}}]}}}}", uri));
        } else if body.contains("\"method\":\"shutdown\"") {
            reply(&body, "null");
        } else if body.contains("\"method\":\"exit\"") {
            return;
        }
    }
}
"#,
        )
        .unwrap();
        let status = std::process::Command::new("rustc")
            .args(["--edition=2021", "-O"])
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .status()
            .unwrap();
        assert!(status.success());
        let file = workspace.path().join("sample.txt");
        std::fs::write(&file, "hello").unwrap();
        let settings = Settings {
            raw: json!({"lspServers": {"mock": {
                "command": binary,
                "extensionToLanguage": {".txt": "plaintext"},
                "maxRestarts": 1
            }}}),
        };
        let integration = configure_lsp(&settings, workspace.path(), false)
            .unwrap()
            .unwrap();
        let registry = ToolRegistry::with_services(
            Vec::new(),
            integration.deferred_tools,
            vec![integration.service],
        )
        .unwrap();
        let context = ToolContext::new(
            workspace.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        let selected = registry
            .execute(&context, "ToolSearch", json!({"query": "select:LSP"}))
            .await;
        assert!(!selected.is_error, "{}", selected.content);
        let diagnostics = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "diagnostics",
                    "filePath": file
                }),
            )
            .await;
        assert!(!diagnostics.is_error, "{}", diagnostics.content);
        assert!(diagnostics.content.contains("mock warning"));
        assert!(!diagnostics.content.contains("injected diagnostic"));
        let output = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "goToDefinition",
                    "filePath": file,
                    "line": 1,
                    "character": 1
                }),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("sample.txt"));
        assert!(!output.content.contains("file://"));
        assert!(
            !output
                .content
                .contains(workspace.path().to_string_lossy().as_ref())
        );
        let output_json: Value = serde_json::from_str(&output.content).unwrap();
        assert_eq!(output_json["result"][0]["range"]["start"]["line"], 1);
        assert_eq!(output_json["result"][0]["range"]["start"]["character"], 1);
        let symbols = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "documentSymbol",
                    "filePath": file
                }),
            )
            .await;
        assert!(!symbols.is_error, "{}", symbols.content);
        assert!(symbols.content.contains("Demo"));
        let workspace_symbols = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "workspaceSymbol",
                    "filePath": file,
                    "query": "Workspace"
                }),
            )
            .await;
        assert!(!workspace_symbols.is_error, "{}", workspace_symbols.content);
        assert!(workspace_symbols.content.contains("WorkspaceDemo"));
        assert!(workspace_symbols.content.contains(OUTSIDE_WORKSPACE_URI));
        assert!(!workspace_symbols.content.contains("file://"));
        let rename = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "rename",
                    "filePath": file,
                    "line": 1,
                    "character": 1,
                    "newName": "Renamed"
                }),
            )
            .await;
        assert!(!rename.is_error, "{}", rename.content);
        assert!(rename.content.contains("newText"));
        assert!(rename.content.contains("sample.txt"));
        assert!(!rename.content.contains("file://"));
        assert!(rename.content.contains("\"applied\": false"));
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello");
        let restarted = registry
            .execute(
                &context,
                "LSP",
                json!({"operation": "restart", "filePath": file}),
            )
            .await;
        assert!(!restarted.is_error, "{}", restarted.content);
        assert!(restarted.content.contains("\"restarted\": true"));
        let after_restart = registry
            .execute(
                &context,
                "LSP",
                json!({
                    "operation": "goToDefinition",
                    "filePath": file,
                    "line": 1,
                    "character": 1
                }),
            )
            .await;
        assert!(!after_restart.is_error, "{}", after_restart.content);
        assert!(after_restart.content.contains("sample.txt"));
        assert!(!after_restart.content.contains("file://"));
        let read = registry
            .execute(&context, "Read", json!({"file_path": file}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let edit = registry
            .execute(
                &context,
                "Edit",
                json!({
                    "file_path": file,
                    "old_string": "hello",
                    "new_string": "updated"
                }),
            )
            .await;
        assert!(!edit.is_error, "{}", edit.content);
        assert!(edit.content.contains("passive edit warning"));
        assert!(edit.content.contains("lsp_diagnostics"));
        assert!(edit.content.contains("sample.txt"));
        assert!(!edit.content.contains("file://"));
        registry.shutdown().await;
    }
}
