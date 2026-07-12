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
use tokio::{sync::Mutex, task::JoinHandle, time::timeout};
use url::Url;

use crate::{
    config::Settings,
    rpc::{RpcFraming, RpcServerRequestHandler, StdioRpcClient, StdioRpcConfig},
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
const MAX_URI_BYTES: usize = 16 * 1024;
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
            StdioRpcClient::spawn(StdioRpcConfig {
                label: format!("LSP/{}", config.name),
                command: config.command.clone(),
                args: config.args.clone(),
                env: config.env.clone(),
                cwd: config.cwd.clone(),
                framing: RpcFraming::ContentLength,
                request_timeout: config.request_timeout,
                server_request_handler: Some(handler),
            })
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
                        "implementation": {}, "callHierarchy": {},
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
        if let Some(version) = params.get("version").and_then(Value::as_i64) {
            let stale = self
                .documents
                .lock()
                .await
                .get(uri)
                .is_some_and(|current| version < current.version);
            if stale {
                return;
            }
        }
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
        let extension = extension_of(path)?;
        Ok(self.extensions.get(&extension).cloned())
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
        let client =
            LspClient::connect(config, &self.workspace, Arc::clone(&self.diagnostics)).await?;
        clients.insert(name.clone(), Arc::clone(&client));
        Ok(Some((name, client)))
    }

    async fn restart(&self, name: &str) {
        if let Some(client) = self.clients.lock().await.remove(name) {
            client.shutdown().await;
        }
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
        "Queries user-configured local language servers for definitions, references, hover, symbols, implementations, and call hierarchy."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "operation": {"type": "string", "enum": [
                    "goToDefinition", "findReferences", "hover", "documentSymbol",
                    "workspaceSymbol", "goToImplementation", "prepareCallHierarchy",
                    "incomingCalls", "outgoingCalls"
                ]},
                "filePath": {"type": "string", "minLength": 1, "maxLength": 16384},
                "line": {"type": "integer", "minimum": 1},
                "character": {"type": "integer", "minimum": 1},
                "query": {"type": "string", "maxLength": 4096}
            }),
            &["operation", "filePath"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        true
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
        let mut last_error = None;
        for attempt in 0..=config.max_restarts {
            let Some((name, client)) = self.manager.client_for_path(&path).await? else {
                bail!("LSP server mapping 在执行时消失")
            };
            let result = async {
                client.sync_document(&path, &text).await?;
                execute_operation(&client, &path, &input).await
            }
            .await;
            match result {
                Ok(result) => {
                    let diagnostics = self.manager.take_diagnostics().await;
                    return Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
                        "operation": input.operation,
                        "filePath": input.file_path,
                        "result": result,
                        "diagnostics": diagnostics,
                    }))?));
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
        return Ok(sanitize_lsp_value(result));
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
        .map(sanitize_lsp_value)
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
    let range = object.get("range")?;
    if !range.is_object() {
        return None;
    }
    let mut cleaned = json!({"message": message, "range": range});
    for field in ["severity", "code", "source", "tags", "relatedInformation"] {
        if let Some(value) = object.get(field) {
            cleaned[field] = value.clone();
        }
    }
    Some(cleaned)
}

fn sanitize_lsp_value(mut value: Value) -> Value {
    match &mut value {
        Value::Object(object) => {
            object.remove("data");
            for child in object.values_mut() {
                *child = sanitize_lsp_value(child.take());
            }
        }
        Value::Array(values) => {
            for child in values {
                *child = sanitize_lsp_value(child.take());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    value
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
            "data": {"hidden": true}
        });
        let cleaned = validate_diagnostic(&valid).unwrap();
        assert!(cleaned.get("data").is_none());
        assert_eq!(cleaned["message"], "problem");
    }

    #[tokio::test]
    async fn configured_server_handles_definition_request() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("mock_lsp.rs");
        let binary = temp
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
            send("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"capabilities\":{},\"serverInfo\":{\"name\":\"mock\",\"version\":\"1\"}}}");
        } else if body.contains("\"method\":\"textDocument/definition\"") {
            send("{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":[{\"uri\":\"file:///mock.txt\",\"range\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}}}]}");
        } else if body.contains("\"method\":\"textDocument/documentSymbol\"") {
            send("{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":[{\"name\":\"Demo\",\"kind\":12,\"range\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}},\"selectionRange\":{\"start\":{\"line\":0,\"character\":0},\"end\":{\"line\":0,\"character\":1}}}]}");
        } else if body.contains("\"method\":\"shutdown\"") {
            send("{\"jsonrpc\":\"2.0\",\"id\":4,\"result\":null}");
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
        let file = temp.path().join("sample.txt");
        std::fs::write(&file, "hello").unwrap();
        let settings = Settings {
            raw: json!({"lspServers": {"mock": {
                "command": binary,
                "extensionToLanguage": {".txt": "plaintext"},
                "maxRestarts": 1
            }}}),
        };
        let integration = configure_lsp(&settings, temp.path(), false)
            .unwrap()
            .unwrap();
        let registry = ToolRegistry::with_services(
            Vec::new(),
            integration.deferred_tools,
            vec![integration.service],
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
        let selected = registry
            .execute(&context, "ToolSearch", json!({"query": "select:LSP"}))
            .await;
        assert!(!selected.is_error, "{}", selected.content);
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
        assert!(output.content.contains("file:///mock.txt"));
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
        registry.shutdown().await;
    }
}
