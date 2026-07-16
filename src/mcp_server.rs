//! Bounded provider-neutral MCP server entrypoint.
//!
//! The interactive harness is an MCP client. This module implements the
//! inverse embedding surface exposed by the source snapshot's `mcp serve`
//! command: local harness tools are listed and invoked over newline-delimited
//! JSON-RPC without constructing a model client or conversation transcript.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use tokio::io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use crate::{
    auto_memory::AutoMemory,
    cli::McpServeArgs,
    config::Settings,
    hooks::HookRunner,
    lsp::configure_lsp,
    permissions::{PermissionManager, PermissionMode},
    plugins::PluginCatalog,
    tools::{MemoryTool, ToolContext, ToolRegistry, ToolService},
    ui_settings::{UiSettings, UiSettingsStore},
    web_tools::configure_web,
};

const CURRENT_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 8 * 1024 * 1024;
const MAX_METHOD_BYTES: usize = 256;
const MAX_REQUEST_ID_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ServerPhase {
    Uninitialized,
    AwaitingInitialized { protocol_version: String },
    Ready { protocol_version: String },
}

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32600,
            message: message.into(),
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn not_initialized() -> Self {
        Self {
            code: -32002,
            message: "Server is not initialized".to_owned(),
        }
    }
}

enum BoundedLine {
    Eof,
    Line,
    TooLarge,
}

/// A single stdio MCP connection. Requests are intentionally processed in
/// order so permission-sensitive filesystem mutations cannot race each other.
pub struct McpToolServer {
    registry: ToolRegistry,
    context: ToolContext,
    phase: ServerPhase,
    debug: bool,
}

impl McpToolServer {
    pub fn new(registry: ToolRegistry, context: ToolContext, debug: bool) -> Self {
        Self {
            registry,
            context,
            phase: ServerPhase::Uninitialized,
            debug,
        }
    }

    pub async fn serve<R, W>(&mut self, mut reader: R, mut writer: W) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        let mut line = Vec::new();
        loop {
            line.clear();
            match read_bounded_line(&mut reader, &mut line).await? {
                BoundedLine::Eof => break,
                BoundedLine::TooLarge => {
                    write_response(
                        &mut writer,
                        &error_response(
                            Value::Null,
                            -32700,
                            format!("Request exceeds {MAX_REQUEST_BYTES} bytes"),
                        ),
                    )
                    .await?;
                    continue;
                }
                BoundedLine::Line => {}
            }
            while matches!(line.last(), Some(b'\n' | b'\r')) {
                line.pop();
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value: Value = match serde_json::from_slice(&line) {
                Ok(value) => value,
                Err(_) => {
                    write_response(
                        &mut writer,
                        &error_response(Value::Null, -32700, "Parse error"),
                    )
                    .await?;
                    continue;
                }
            };
            if let Some(response) = self.handle_message(value).await {
                write_response(&mut writer, &response).await?;
            }
        }
        writer.flush().await.context("cannot flush MCP stdout")
    }

    async fn handle_message(&mut self, value: Value) -> Option<Value> {
        let object = match value.as_object() {
            Some(object) => object,
            None => {
                return Some(error_response(
                    Value::Null,
                    -32600,
                    "JSON-RPC request must be an object",
                ));
            }
        };
        let id = match parse_id(object) {
            Ok(id) => id,
            Err(error) => return Some(error_response(Value::Null, error.code, error.message)),
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return id.map(|id| error_response(id, -32600, "jsonrpc must equal \"2.0\""));
        }
        let method = match object.get("method").and_then(Value::as_str) {
            Some(method)
                if !method.is_empty()
                    && method.len() <= MAX_METHOD_BYTES
                    && !method.chars().any(char::is_control) =>
            {
                method
            }
            _ => {
                return id.map(|id| error_response(id, -32600, "method must be a bounded string"));
            }
        };
        if self.debug {
            eprintln!("[mcp-server] {method}");
        }
        let params = object.get("params");

        if id.is_none() {
            self.handle_notification(method, params);
            return None;
        }
        let id = id.expect("request id was checked");
        let result = self.handle_request(method, params).await;
        Some(match result {
            Ok(result) => success_response(id, result),
            Err(error) => error_response(id, error.code, error.message),
        })
    }

    fn handle_notification(&mut self, method: &str, _params: Option<&Value>) {
        match method {
            "notifications/initialized" => {
                if let ServerPhase::AwaitingInitialized { protocol_version } = &self.phase {
                    self.phase = ServerPhase::Ready {
                        protocol_version: protocol_version.clone(),
                    };
                }
            }
            "notifications/cancelled" | "$/cancelRequest" => {}
            _ => {}
        }
    }

    async fn handle_request(
        &mut self,
        method: &str,
        params: Option<&Value>,
    ) -> std::result::Result<Value, RpcError> {
        match method {
            "initialize" => self.initialize(params),
            "ping" => Ok(json!({})),
            "tools/list" => {
                self.require_ready()?;
                self.list_tools(params)
            }
            "tools/call" => {
                self.require_ready()?;
                self.call_tool(params).await
            }
            _ => Err(RpcError::method_not_found(method)),
        }
    }

    fn initialize(&mut self, params: Option<&Value>) -> std::result::Result<Value, RpcError> {
        if !matches!(self.phase, ServerPhase::Uninitialized) {
            return Err(RpcError::invalid_request(
                "initialize may only be called once per connection",
            ));
        }
        let params = params
            .and_then(Value::as_object)
            .ok_or_else(|| RpcError::invalid_params("initialize params must be an object"))?;
        let requested = params
            .get("protocolVersion")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && value.len() <= 64)
            .ok_or_else(|| RpcError::invalid_params("protocolVersion must be a bounded string"))?;
        let protocol_version = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested) {
            requested
        } else {
            CURRENT_PROTOCOL_VERSION
        }
        .to_owned();
        self.phase = ServerPhase::AwaitingInitialized {
            protocol_version: protocol_version.clone(),
        };
        Ok(json!({
            "protocolVersion": protocol_version,
            "capabilities": {
                "tools": {"listChanged": false}
            },
            "serverInfo": {
                "name": "open-agent-harness",
                "version": env!("CARGO_PKG_VERSION")
            }
        }))
    }

    fn require_ready(&self) -> std::result::Result<(), RpcError> {
        match &self.phase {
            ServerPhase::Ready { protocol_version } => {
                debug_assert!(SUPPORTED_PROTOCOL_VERSIONS.contains(&protocol_version.as_str()));
                Ok(())
            }
            ServerPhase::Uninitialized | ServerPhase::AwaitingInitialized { .. } => {
                Err(RpcError::not_initialized())
            }
        }
    }

    fn list_tools(&self, params: Option<&Value>) -> std::result::Result<Value, RpcError> {
        if let Some(params) = params {
            let params = params
                .as_object()
                .ok_or_else(|| RpcError::invalid_params("tools/list params must be an object"))?;
            if params.get("cursor").is_some_and(|cursor| !cursor.is_null()) {
                return Err(RpcError::invalid_params(
                    "tools/list cursor is not valid for this non-paginated server",
                ));
            }
        }
        let tools = self
            .registry
            .definitions()
            .into_iter()
            .map(|definition| {
                let object = definition.as_object().ok_or_else(|| {
                    RpcError::invalid_request("tool definition must be an object")
                })?;
                Ok(json!({
                    "name": object.get("name").cloned().unwrap_or(Value::Null),
                    "description": object.get("description").cloned().unwrap_or(Value::Null),
                    "inputSchema": object.get("input_schema").cloned().unwrap_or_else(|| json!({"type":"object"}))
                }))
            })
            .collect::<std::result::Result<Vec<Value>, RpcError>>()?;
        Ok(json!({"tools": tools}))
    }

    async fn call_tool(&self, params: Option<&Value>) -> std::result::Result<Value, RpcError> {
        let params = params
            .and_then(Value::as_object)
            .ok_or_else(|| RpcError::invalid_params("tools/call params must be an object"))?;
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty() && name.len() <= 1024)
            .ok_or_else(|| RpcError::invalid_params("tool name must be a bounded string"))?;
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return Err(RpcError::invalid_params("tool arguments must be an object"));
        }
        let output = self.registry.execute(&self.context, name, arguments).await;
        let (text, is_error) = if output.content.len() > MAX_TOOL_RESULT_BYTES {
            (
                format!("Tool result exceeds {MAX_TOOL_RESULT_BYTES} bytes"),
                true,
            )
        } else {
            (output.content, output.is_error || output.interrupted)
        };
        Ok(json!({
            "content": [{"type":"text", "text": text}],
            "isError": is_error
        }))
    }
}

/// Build the local tool surface and own its full shutdown lifecycle. This
/// deliberately does not connect configured upstream MCP servers: the source
/// entrypoint also left re-exporting unspecified, and it can create recursive graphs.
pub async fn run_stdio_server(
    cwd: PathBuf,
    mut settings: Settings,
    args: McpServeArgs,
) -> Result<()> {
    let bare = args.bare || args.safe_mode;
    let mode = if args.dangerously_skip_permissions {
        PermissionMode::BypassPermissions
    } else {
        args.permission_mode
            .or_else(|| settings.permission_mode())
            .unwrap_or(PermissionMode::Default)
    };
    let mut allow_rules = settings.allow_rules();
    allow_rules.extend(args.allowed_tools);
    let mut deny_rules = settings.deny_rules();
    deny_rules.extend(args.disallowed_tools);
    let permissions = PermissionManager::new(mode, false, allow_rules, deny_rules);
    if !bare {
        let ui_settings = match UiSettingsStore::default_user() {
            Ok(store) => store.load().unwrap_or_else(|_| UiSettings::default()),
            Err(_) => UiSettings::default(),
        };
        permissions.set_user_rules(ui_settings.permission_rules)?;
    }

    let mut context = ToolContext::new(cwd.clone(), permissions);
    context.set_bare(bare);
    let additional_roots = context.add_trusted_roots(&args.add_dirs)?;
    context.set_sandbox_runtime(
        settings
            .sandbox_runtime()?
            .with_session_workspaces(&additional_roots)?,
    );

    let plugins = PluginCatalog::discover(&settings, &cwd, bare)?;
    plugins.apply_runtime_contributions(&mut settings)?;
    context.configure_secret_env_scrubber(&settings)?;
    let (plugin_skills, _plugin_commands, plugin_hooks, plugin_monitors) = plugins.into_parts();
    context.set_extension_skills(plugin_skills);
    context.configure_plugin_monitors(plugin_monitors);
    let hooks = Arc::new(HookRunner::from_settings_and_plugins(
        &settings,
        &plugin_hooks,
    )?);
    context.set_hooks(Arc::clone(&hooks));
    context.reload_workspace_context().await?;

    let mut active_tools = Vec::new();
    let mut deferred_tools = Vec::new();
    let mut services: Vec<Arc<dyn ToolService>> = Vec::new();
    if !bare {
        let memory = AutoMemory::open(&cwd, &settings)?;
        if memory.enabled() {
            active_tools.push(MemoryTool::new(memory).into_tool());
        }
        if let Some(lsp) = configure_lsp(&settings, &cwd, args.debug)? {
            deferred_tools.extend(lsp.deferred_tools);
            services.push(lsp.service);
        }
        deferred_tools.extend(configure_web(&settings)?.deferred_tools);
    }
    let registry = ToolRegistry::with_services(active_tools, deferred_tools, services)?;
    if let Some(tools) = &args.tools {
        registry.restrict_to(tools)?;
    }

    let result = {
        let stdin = BufReader::new(io::stdin());
        let stdout = io::stdout();
        McpToolServer::new(registry.clone(), context.clone(), args.debug)
            .serve(stdin, stdout)
            .await
    };

    context.stop_cron_scheduler();
    context.shutdown_background_tasks().await;
    context.shutdown_monitors().await;
    hooks.finalize_async().await;
    registry.shutdown().await;
    result
}

async fn read_bounded_line<R>(reader: &mut R, output: &mut Vec<u8>) -> io::Result<BoundedLine>
where
    R: AsyncBufRead + Unpin,
{
    let mut too_large = false;
    let mut observed = false;
    loop {
        let buffer = reader.fill_buf().await?;
        if buffer.is_empty() {
            return Ok(if !observed {
                BoundedLine::Eof
            } else if too_large {
                BoundedLine::TooLarge
            } else {
                BoundedLine::Line
            });
        }
        observed = true;
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        if !too_large {
            if output.len().saturating_add(consumed) > MAX_REQUEST_BYTES {
                too_large = true;
                output.clear();
            } else {
                output.extend_from_slice(&buffer[..consumed]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(if too_large {
                BoundedLine::TooLarge
            } else {
                BoundedLine::Line
            });
        }
    }
}

fn parse_id(object: &Map<String, Value>) -> std::result::Result<Option<Value>, RpcError> {
    let Some(id) = object.get("id") else {
        return Ok(None);
    };
    match id {
        Value::Null => Ok(Some(Value::Null)),
        Value::String(value)
            if value.len() <= MAX_REQUEST_ID_BYTES && !value.chars().any(char::is_control) =>
        {
            Ok(Some(id.clone()))
        }
        Value::Number(value) if value.is_i64() || value.is_u64() => Ok(Some(id.clone())),
        _ => Err(RpcError::invalid_request(
            "id must be null, a bounded string, or an integer",
        )),
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "result":result})
}

fn error_response(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc":"2.0",
        "id":id,
        "error":{"code":code, "message":message.into()}
    })
}

async fn write_response<W>(writer: &mut W, response: &Value) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(response).context("cannot serialize MCP response")?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        bytes = serde_json::to_vec(&error_response(
            response.get("id").cloned().unwrap_or(Value::Null),
            -32603,
            format!("Response exceeds {MAX_RESPONSE_BYTES} bytes"),
        ))?;
    }
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .context("cannot write MCP response")?;
    writer.flush().await.context("cannot flush MCP response")
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use anyhow::Result;
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

    use super::*;
    use crate::{
        permissions::PermissionManager,
        tools::{Tool, ToolOutput},
    };

    struct EchoTool;

    #[async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "Echo"
        }

        fn description(&self) -> &str {
            "Echo bounded text"
        }

        fn input_schema(&self) -> Value {
            json!({
                "type":"object",
                "properties":{"text":{"type":"string", "maxLength":128}},
                "required":["text"],
                "additionalProperties":false
            })
        }

        fn read_only(&self, _input: &Value) -> bool {
            true
        }

        fn summary(&self, _input: &Value) -> String {
            "echo".to_owned()
        }

        async fn execute(&self, _context: &ToolContext, input: Value) -> Result<ToolOutput> {
            Ok(ToolOutput::success(
                input["text"].as_str().unwrap_or_default(),
            ))
        }
    }

    fn test_server(root: &Path, bypass: bool) -> McpToolServer {
        let mode = if bypass {
            PermissionMode::BypassPermissions
        } else {
            PermissionMode::Default
        };
        let context = ToolContext::new(
            root.to_owned(),
            PermissionManager::new(mode, false, Vec::new(), Vec::new()),
        );
        let registry = ToolRegistry::with_extensions(vec![Arc::new(EchoTool)], Vec::new()).unwrap();
        McpToolServer::new(registry, context, false)
    }

    async fn exchange(server: McpToolServer, input: Vec<u8>) -> Vec<Value> {
        let (client, server_io) = tokio::io::duplex(MAX_REQUEST_BYTES + 4096);
        let (server_read, server_write) = tokio::io::split(server_io);
        let worker = tokio::spawn(async move {
            let mut server = server;
            server
                .serve(BufReader::new(server_read), server_write)
                .await
                .unwrap();
        });
        let (mut client_read, mut client_write) = tokio::io::split(client);
        client_write.write_all(&input).await.unwrap();
        client_write.shutdown().await.unwrap();
        let mut output = Vec::new();
        client_read.read_to_end(&mut output).await.unwrap();
        worker.await.unwrap();
        String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn line(value: Value) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        bytes
    }

    fn initialized_exchange(mut messages: Vec<Value>) -> Vec<u8> {
        let mut all = vec![
            json!({
                "jsonrpc":"2.0",
                "id":1,
                "method":"initialize",
                "params":{"protocolVersion":"2025-11-25", "capabilities":{}, "clientInfo":{"name":"test", "version":"1"}}
            }),
            json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
        ];
        all.append(&mut messages);
        all.into_iter().flat_map(line).collect()
    }

    #[tokio::test]
    async fn handshake_lists_and_calls_tools() {
        let root = tempfile::tempdir().unwrap();
        let responses = exchange(
            test_server(root.path(), false),
            initialized_exchange(vec![
                json!({"jsonrpc":"2.0", "id":"list", "method":"tools/list", "params":{}}),
                json!({"jsonrpc":"2.0", "id":"call", "method":"tools/call", "params":{"name":"Echo", "arguments":{"text":"hello"}}}),
            ]),
        )
        .await;
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0]["result"]["protocolVersion"], "2025-11-25");
        let tools = responses[1]["result"]["tools"].as_array().unwrap();
        let echo = tools.iter().find(|tool| tool["name"] == "Echo").unwrap();
        assert_eq!(echo["inputSchema"]["additionalProperties"], false);
        assert_eq!(responses[2]["result"]["content"][0]["text"], "hello");
        assert_eq!(responses[2]["result"]["isError"], false);
    }

    #[tokio::test]
    async fn protocol_state_and_parse_failures_are_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let mut input = b"not-json\n".to_vec();
        input.extend(line(json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/list", "params":{}
        })));
        input.extend(line(json!({
            "jsonrpc":"2.0", "id":2, "method":"initialize",
            "params":{"protocolVersion":"unknown"}
        })));
        input.extend(line(json!({
            "jsonrpc":"2.0", "id":3, "method":"initialize",
            "params":{"protocolVersion":"2025-11-25"}
        })));
        let responses = exchange(test_server(root.path(), false), input).await;
        assert_eq!(responses[0]["error"]["code"], -32700);
        assert_eq!(responses[1]["error"]["code"], -32002);
        assert_eq!(
            responses[2]["result"]["protocolVersion"],
            CURRENT_PROTOCOL_VERSION
        );
        assert_eq!(responses[3]["error"]["code"], -32600);
    }

    #[tokio::test]
    async fn oversized_request_is_drained_and_next_request_survives() {
        let root = tempfile::tempdir().unwrap();
        let mut input = vec![b'x'; MAX_REQUEST_BYTES + 1];
        input.push(b'\n');
        input.extend(line(json!({"jsonrpc":"2.0", "id":9, "method":"ping"})));
        let responses = exchange(test_server(root.path(), false), input).await;
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["error"]["code"], -32700);
        assert_eq!(responses[1]["id"], 9);
        assert_eq!(responses[1]["result"], json!({}));
    }

    #[tokio::test]
    async fn noninteractive_default_denies_mutating_tools() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("blocked.txt");
        let responses = exchange(
            test_server(root.path(), false),
            initialized_exchange(vec![json!({
                "jsonrpc":"2.0", "id":2, "method":"tools/call",
                "params":{"name":"Write", "arguments":{"file_path":target, "content":"no"}}
            })]),
        )
        .await;
        assert_eq!(responses[1]["result"]["isError"], true);
        assert!(!target.exists());
    }
}
