use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use globset::Glob;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
    task::JoinHandle,
    time::timeout,
};

use crate::{
    config::Settings,
    mcp::{McpHookCall, McpHookInvoker},
    process::{SecretEnvScrubber, resolve_trusted_executable, spawn_managed},
    tools::ToolOutput,
};

const MAX_RULES: usize = 128;
const MAX_COMMANDS_PER_RULE: usize = 16;
const MAX_COMMAND_BYTES: usize = 64 * 1024;
const MAX_ARGS: usize = 128;
const MAX_ARG_BYTES: usize = 32 * 1024;
const MAX_HOOK_INPUT_BYTES: usize = 1024 * 1024;
const MAX_HOOK_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_HOOK_COMBINED_OUTPUT_BYTES: usize = 512 * 1024;
const MAX_MATCHED_COMMANDS_PER_EVENT: usize = 64;
const MAX_ASYNC_HOOKS: usize = 32;
const MAX_HOOK_CONDITION_BYTES: usize = 1024;
const MAX_HOOK_STATUS_BYTES: usize = 8 * 1024;
const MAX_HOOK_WATCH_PATHS: usize = 128;
const MAX_HOOK_WATCH_PATH_BYTES: usize = 16 * 1024;
const MAX_MCP_SERVER_BYTES: usize = 128;
const MAX_MCP_TOOL_BYTES: usize = 1024;
const MAX_MCP_INPUT_DEPTH: usize = 32;
const MAX_MCP_INPUT_NODES: usize = 4096;
const MAX_MCP_PLACEHOLDERS: usize = 256;
const MAX_MCP_PLACEHOLDER_PATH_BYTES: usize = 512;
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const STREAM_DRAIN_GRACE: Duration = Duration::from_secs(1);
const ASYNC_FINALIZE_TIMEOUT: Duration = Duration::from_secs(60);

const SUPPORTED_EVENTS: &[&str] = &[
    "PreToolUse",
    "PostToolUse",
    "PostToolBatch",
    "PostToolUseFailure",
    "PermissionRequest",
    "PermissionDenied",
    "Notification",
    "UserPromptSubmit",
    "UserPromptExpansion",
    "SessionStart",
    "SessionEnd",
    "Stop",
    "StopFailure",
    "SubagentStart",
    "SubagentStop",
    "PreCompact",
    "PostCompact",
    "TaskCreated",
    "TaskCompleted",
    "TeammateIdle",
    "InstructionsLoaded",
    "WorktreeCreate",
    "WorktreeRemove",
    "CwdChanged",
    "FileChanged",
    "ConfigChange",
    "MessageDisplay",
];

#[derive(Debug, thiserror::Error)]
#[error("{event} hook blocked operation{detail}")]
pub struct HookBlocked {
    event: String,
    detail: String,
}

impl HookBlocked {
    fn new(event: &str, detail: impl Into<String>) -> Self {
        let detail = detail.into();
        Self {
            event: event.to_owned(),
            detail: if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            },
        }
    }

    pub fn feedback(&self) -> &str {
        self.detail.trim_start_matches(": ")
    }
}

pub fn blocking_feedback(error: &anyhow::Error) -> Option<String> {
    error
        .downcast_ref::<HookBlocked>()
        .map(|blocked| blocked.feedback().to_owned())
}

#[derive(Clone)]
pub struct HookRunner {
    events: Arc<HashMap<String, Vec<HookRule>>>,
    file_watch_patterns: Arc<Vec<String>>,
    additional: Vec<Arc<HookRunner>>,
    async_slots: Arc<Semaphore>,
    mcp_invoker: Option<Arc<dyn McpHookInvoker>>,
    secret_env_scrubber: SecretEnvScrubber,
    observer: Option<HookObserver>,
    next_observer_id: Arc<AtomicU64>,
    async_tasks: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    async_finalizing: Arc<AtomicBool>,
}

pub type HookObserver = Arc<dyn Fn(&HookExecutionEvent) + Send + Sync>;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HookExecutionEvent {
    HookStarted {
        id: u64,
        event: String,
        asynchronous: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        status_message: Option<String>,
    },
    HookResponse {
        id: u64,
        event: String,
        asynchronous: bool,
        outcome: String,
        exit_code: Option<i32>,
        elapsed_ms: u128,
        truncated: bool,
    },
}

impl Default for HookRunner {
    fn default() -> Self {
        Self {
            events: Arc::new(HashMap::new()),
            file_watch_patterns: Arc::new(Vec::new()),
            additional: Vec::new(),
            async_slots: Arc::new(Semaphore::new(MAX_ASYNC_HOOKS)),
            mcp_invoker: None,
            secret_env_scrubber: SecretEnvScrubber::default(),
            observer: None,
            next_observer_id: Arc::new(AtomicU64::new(1)),
            async_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            async_finalizing: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct HookRule {
    matcher: HookMatcher,
    actions: Vec<Arc<HookAction>>,
}

enum HookMatcher {
    All,
    Patterns(Vec<globset::GlobMatcher>),
}

struct HookCommand {
    command: String,
    args: Option<Vec<String>>,
    shell: HookShell,
    timeout: Duration,
    asynchronous: bool,
    once: bool,
    workspace_relative: bool,
    condition: Option<HookCondition>,
    status_message: Option<String>,
    fired: AtomicBool,
}

struct McpToolHook {
    server: String,
    tool: String,
    input: Value,
    timeout: Duration,
    asynchronous: bool,
    once: bool,
    condition: Option<HookCondition>,
    status_message: Option<String>,
    fired: AtomicBool,
}

enum HookAction {
    Command(Box<HookCommand>),
    McpTool(Box<McpToolHook>),
}

struct HookCondition {
    tool: globset::GlobMatcher,
    content: Option<globset::GlobMatcher>,
}

#[derive(Clone, Copy)]
enum HookShell {
    Default,
    PowerShell,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHookRule {
    #[serde(default)]
    matcher: String,
    hooks: Vec<RawHookAction>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum RawHookAction {
    #[serde(rename = "command")]
    Command {
        command: String,
        args: Option<Vec<String>>,
        shell: Option<String>,
        timeout: Option<f64>,
        #[serde(rename = "timeoutMs")]
        timeout_ms: Option<u64>,
        #[serde(rename = "async", default)]
        asynchronous: bool,
        #[serde(default)]
        once: bool,
        #[serde(rename = "workspaceRelative", default)]
        workspace_relative: bool,
        #[serde(rename = "if")]
        condition: Option<String>,
        #[serde(rename = "statusMessage")]
        status_message: Option<String>,
    },
    #[serde(rename = "mcp_tool")]
    McpTool {
        server: String,
        tool: String,
        input: Option<serde_json::Map<String, Value>>,
        #[serde(rename = "if")]
        condition: Option<String>,
        timeout: Option<f64>,
        #[serde(rename = "statusMessage")]
        status_message: Option<String>,
        #[serde(default)]
        once: bool,
        #[serde(rename = "async", default)]
        asynchronous: bool,
    },
}

#[derive(Debug, Default)]
pub struct HookOutcome {
    pub updated_input: Option<Value>,
    pub updated_output: Option<String>,
    pub additional_context: Vec<String>,
    pub watch_paths: Vec<String>,
}

struct CommandResult {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
    truncated: bool,
}

struct ActionResult {
    succeeded: bool,
    blocked: bool,
    body: String,
    detail: String,
    exit_code: Option<i32>,
    truncated: bool,
}

impl HookRunner {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let secret_env_scrubber = SecretEnvScrubber::from_settings(settings)?;
        let Some(events) = settings.raw.get("hooks") else {
            return Ok(Self {
                secret_env_scrubber,
                ..Self::default()
            });
        };
        let events = events.as_object().context("hooks 必须是 JSON object")?;
        let total_rules = events
            .values()
            .filter_map(Value::as_array)
            .map(Vec::len)
            .sum::<usize>();
        if total_rules > MAX_RULES {
            bail!("hooks 总规则数超过 {MAX_RULES} 项限制")
        }
        let mut parsed = HashMap::new();
        let mut file_watch_patterns = Vec::new();
        for (event, rules) in events {
            if !SUPPORTED_EVENTS.contains(&event.as_str()) {
                bail!("不支持的 hook event: {event}")
            }
            let rules = rules
                .as_array()
                .with_context(|| format!("hooks.{event} 必须是 array"))?;
            let mut parsed_rules = Vec::new();
            for rule in rules {
                let raw: RawHookRule = serde_json::from_value(rule.clone())
                    .with_context(|| format!("hooks.{event} rule 无效"))?;
                if event == "FileChanged" {
                    for pattern in raw
                        .matcher
                        .split('|')
                        .map(str::trim)
                        .filter(|pattern| !pattern.is_empty())
                    {
                        if pattern.len() > MAX_HOOK_WATCH_PATH_BYTES || pattern.contains('\0') {
                            bail!("FileChanged matcher 路径过长或包含 NUL")
                        }
                        if !file_watch_patterns.iter().any(|value| value == pattern) {
                            if file_watch_patterns.len() >= MAX_HOOK_WATCH_PATHS {
                                bail!(
                                    "FileChanged watch matcher 超过 {MAX_HOOK_WATCH_PATHS} 项限制"
                                )
                            }
                            file_watch_patterns.push(pattern.to_owned());
                        }
                    }
                }
                if raw.hooks.is_empty() || raw.hooks.len() > MAX_COMMANDS_PER_RULE {
                    bail!("hooks.{event} 每条规则必须有 1..={MAX_COMMANDS_PER_RULE} 个命令")
                }
                let matcher = parse_matcher(&raw.matcher)?;
                let actions = raw
                    .hooks
                    .into_iter()
                    .map(parse_action)
                    .collect::<Result<Vec<_>>>()?;
                parsed_rules.push(HookRule { matcher, actions });
            }
            parsed.insert(event.clone(), parsed_rules);
        }
        Ok(Self {
            events: Arc::new(parsed),
            file_watch_patterns: Arc::new(file_watch_patterns),
            additional: Vec::new(),
            async_slots: Arc::new(Semaphore::new(MAX_ASYNC_HOOKS)),
            mcp_invoker: None,
            secret_env_scrubber,
            observer: None,
            next_observer_id: Arc::new(AtomicU64::new(1)),
            async_tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            async_finalizing: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn with_observer(mut self, observer: Option<HookObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Installs the already-connected MCP service used by `type=mcp_tool`
    /// hooks. Keeping this explicit prevents hook configuration from creating
    /// transports or reaching servers outside the trusted `mcpServers` set.
    pub fn with_mcp_invoker(mut self, invoker: Option<Arc<dyn McpHookInvoker>>) -> Self {
        self.mcp_invoker = invoker.clone();
        self.additional = self
            .additional
            .iter()
            .map(|runner| Arc::new((**runner).clone().with_mcp_invoker(invoker.clone())))
            .collect();
        self
    }

    fn with_secret_env_scrubber(mut self, scrubber: SecretEnvScrubber) -> Self {
        self.secret_env_scrubber = scrubber.clone();
        self.additional = self
            .additional
            .iter()
            .map(|runner| {
                Arc::new(
                    (**runner)
                        .clone()
                        .with_secret_env_scrubber(scrubber.clone()),
                )
            })
            .collect();
        self
    }

    /// Returns a scoped chain containing the current trusted hooks followed by
    /// one declarative skill hook set. The original runner is unchanged, so
    /// dropping the caller's cloned `ToolContext` restores the prior scope.
    pub fn with_scoped_hooks(&self, hooks: &Value) -> Result<Self> {
        let hooks = hooks.as_object().context("skill hooks 必须是 object")?;
        let additional_rules = hooks
            .values()
            .map(|rules| rules.as_array().map_or(usize::MAX, Vec::len))
            .try_fold(0usize, |total, count| total.checked_add(count))
            .context("skill hook rule 数量溢出")?;
        let existing_rules = self
            .events
            .values()
            .map(Vec::len)
            .chain(
                self.additional
                    .iter()
                    .flat_map(|runner| runner.events.values().map(Vec::len)),
            )
            .try_fold(0usize, |total, count| total.checked_add(count))
            .context("existing hook rule 数量溢出")?;
        if existing_rules.saturating_add(additional_rules) > MAX_RULES {
            bail!("scoped skill hooks 使总规则数超过 {MAX_RULES} 项限制")
        }
        let mut scoped = HookRunner::from_settings(&Settings {
            raw: json!({"hooks": hooks}),
        })?
        .with_observer(self.observer.clone())
        .with_mcp_invoker(self.mcp_invoker.clone())
        .with_secret_env_scrubber(self.secret_env_scrubber.clone());
        // A scope adds rules, not a new runtime. All scoped runners must use
        // the root lifecycle so they cannot multiply the async concurrency
        // allowance or detach tasks when the temporary scope is dropped.
        scoped.async_slots = Arc::clone(&self.async_slots);
        scoped.next_observer_id = Arc::clone(&self.next_observer_id);
        scoped.async_tasks = Arc::clone(&self.async_tasks);
        scoped.async_finalizing = Arc::clone(&self.async_finalizing);
        let mut chained = self.clone();
        chained.additional.push(Arc::new(scoped));
        Ok(chained)
    }

    /// Combines trusted settings hooks with already discovered local plugin
    /// hooks, then applies the same strict parser and global limits once.
    pub fn from_settings_and_plugins(settings: &Settings, plugin_hooks: &Value) -> Result<Self> {
        if plugin_hooks.is_null()
            || plugin_hooks
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
        {
            return Self::from_settings(settings);
        }
        let plugin_hooks = plugin_hooks
            .as_object()
            .context("plugin hooks 必须是 object")?;
        let mut raw = settings.raw.clone();
        if !raw.is_object() {
            bail!("trusted settings 顶层必须是 object")
        }
        let root = raw.as_object_mut().expect("settings object was checked");
        let hooks = root
            .entry("hooks")
            .or_insert_with(|| Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .context("settings hooks 必须是 object")?;
        for (event, rules) in plugin_hooks {
            let rules = rules
                .as_array()
                .with_context(|| format!("plugin hooks.{event} 必须是 array"))?;
            hooks
                .entry(event.clone())
                .or_insert_with(|| Value::Array(Vec::new()))
                .as_array_mut()
                .with_context(|| format!("settings hooks.{event} 必须是 array"))?
                .extend(rules.iter().cloned());
        }
        Self::from_settings(&Settings { raw })
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.additional.iter().all(|runner| runner.is_empty())
    }

    pub fn has_event(&self, event: &str) -> bool {
        self.events
            .get(event)
            .is_some_and(|rules| !rules.is_empty())
            || self.additional.iter().any(|runner| runner.has_event(event))
    }

    pub(crate) fn file_watch_patterns(&self) -> Result<Vec<String>> {
        let mut patterns = self.file_watch_patterns.as_ref().clone();
        for runner in &self.additional {
            patterns.extend(runner.file_watch_patterns()?);
            if patterns.len() > MAX_HOOK_WATCH_PATHS {
                bail!("scoped FileChanged watch matcher 超过 {MAX_HOOK_WATCH_PATHS} 项限制")
            }
        }
        patterns.sort();
        patterns.dedup();
        Ok(patterns)
    }

    pub async fn run(
        &self,
        event: &str,
        matcher_value: Option<&str>,
        payload: Value,
        cwd: &std::path::Path,
    ) -> Result<HookOutcome> {
        let matcher_values = matcher_value.into_iter().collect::<Vec<_>>();
        self.run_with_matchers(event, &matcher_values, payload, cwd)
            .await
    }

    /// File-change hooks historically matched the mutating tool name. The
    /// observable file event uses its normalized path as the primary matcher,
    /// while retaining tool-name matching for existing trusted configs. A rule
    /// is evaluated once even when several selectors match it.
    pub async fn run_file_changed(
        &self,
        tool_name: &str,
        file_path: &str,
        payload: Value,
        cwd: &std::path::Path,
    ) -> Result<HookOutcome> {
        let file_name = Path::new(file_path)
            .file_name()
            .and_then(|name| name.to_str());
        let mut matchers = vec![file_path, tool_name];
        if let Some(file_name) = file_name {
            if !matchers.contains(&file_name) {
                matchers.push(file_name);
            }
        }
        self.run_with_matchers("FileChanged", &matchers, payload, cwd)
            .await
    }

    async fn run_with_matchers(
        &self,
        event: &str,
        matcher_values: &[&str],
        payload: Value,
        cwd: &std::path::Path,
    ) -> Result<HookOutcome> {
        let mut outcome = self
            .run_local(event, matcher_values, payload.clone(), cwd)
            .await?;
        for runner in &self.additional {
            let incoming = runner
                .run_local(event, matcher_values, payload.clone(), cwd)
                .await?;
            if incoming.updated_input.is_some() {
                outcome.updated_input = incoming.updated_input;
            }
            if incoming.updated_output.is_some() {
                outcome.updated_output = incoming.updated_output;
            }
            outcome
                .additional_context
                .extend(incoming.additional_context);
            outcome.watch_paths.extend(incoming.watch_paths);
            outcome.watch_paths.sort();
            outcome.watch_paths.dedup();
            if outcome.watch_paths.len() > MAX_HOOK_WATCH_PATHS {
                bail!("hook watchPaths 合并后超过 {MAX_HOOK_WATCH_PATHS} 项限制")
            }
            validate_outcome_size(&outcome)?;
        }
        Ok(outcome)
    }

    async fn run_local(
        &self,
        event: &str,
        matcher_values: &[&str],
        payload: Value,
        cwd: &std::path::Path,
    ) -> Result<HookOutcome> {
        let Some(rules) = self.events.get(event) else {
            return Ok(HookOutcome::default());
        };
        let mut payload = payload;
        if !payload.is_object() {
            payload = json!({"payload": payload});
        }
        payload["hook_event_name"] = Value::String(event.to_owned());
        payload["cwd"] = Value::String(cwd.display().to_string());
        let encoded = serde_json::to_vec(&payload)?;
        if encoded.len() > MAX_HOOK_INPUT_BYTES {
            bail!("hook input 超过 {MAX_HOOK_INPUT_BYTES} 字节限制")
        }
        let mut outcome = HookOutcome::default();
        let mut matched_actions = 0usize;
        for rule in rules {
            if !rule.matcher.matches_any(matcher_values) {
                continue;
            }
            for action in &rule.actions {
                if !action.matches_condition(event, &payload) {
                    continue;
                }
                if action.mark_once_fired() {
                    continue;
                }
                matched_actions = matched_actions.saturating_add(1);
                if matched_actions > MAX_MATCHED_COMMANDS_PER_EVENT {
                    bail!("{event} hook 匹配动作超过 {MAX_MATCHED_COMMANDS_PER_EVENT} 个限制")
                }
                let observer_id = self.next_observer_id.fetch_add(1, Ordering::Relaxed);
                emit_observer(
                    self.observer.as_ref(),
                    HookExecutionEvent::HookStarted {
                        id: observer_id,
                        event: event.to_owned(),
                        asynchronous: action.asynchronous(),
                        status_message: action.status_message().map(ToOwned::to_owned),
                    },
                );
                if action.asynchronous() {
                    let permit = match Arc::clone(&self.async_slots).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            action.reset_once();
                            emit_observer(
                                self.observer.as_ref(),
                                HookExecutionEvent::HookResponse {
                                    id: observer_id,
                                    event: event.to_owned(),
                                    asynchronous: true,
                                    outcome: "dropped".into(),
                                    exit_code: None,
                                    elapsed_ms: 0,
                                    truncated: false,
                                },
                            );
                            continue;
                        }
                    };
                    let action = Arc::clone(action);
                    let encoded = encoded.clone();
                    let payload = payload.clone();
                    let cwd = cwd.to_owned();
                    let mcp_invoker = self.mcp_invoker.clone();
                    let secret_env_scrubber = self.secret_env_scrubber.clone();
                    let observer = self.observer.clone();
                    let event = event.to_owned();
                    let mut tasks = self
                        .async_tasks
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if self.async_finalizing.load(Ordering::Acquire) {
                        action.reset_once();
                        emit_observer(
                            observer.as_ref(),
                            HookExecutionEvent::HookResponse {
                                id: observer_id,
                                event,
                                asynchronous: true,
                                outcome: "dropped".into(),
                                exit_code: None,
                                elapsed_ms: 0,
                                truncated: false,
                            },
                        );
                        continue;
                    }
                    let task = tokio::spawn(async move {
                        let _permit = permit;
                        let started = Instant::now();
                        let result = execute_action(
                            &action,
                            &encoded,
                            &payload,
                            &cwd,
                            mcp_invoker.as_ref(),
                            &secret_env_scrubber,
                        )
                        .await;
                        let (outcome, exit_code, truncated) = match result {
                            Ok(result) => (
                                action_outcome(&result).to_owned(),
                                result.exit_code,
                                result.truncated,
                            ),
                            Err(_) => ("error".to_owned(), None, false),
                        };
                        emit_observer(
                            observer.as_ref(),
                            HookExecutionEvent::HookResponse {
                                id: observer_id,
                                event,
                                asynchronous: true,
                                outcome,
                                exit_code,
                                elapsed_ms: started.elapsed().as_millis(),
                                truncated,
                            },
                        );
                    });
                    tasks.retain(|task| !task.is_finished());
                    tasks.push(task);
                    continue;
                }
                let started = Instant::now();
                let result = match execute_action(
                    action,
                    &encoded,
                    &payload,
                    cwd,
                    self.mcp_invoker.as_ref(),
                    &self.secret_env_scrubber,
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => {
                        emit_observer(
                            self.observer.as_ref(),
                            HookExecutionEvent::HookResponse {
                                id: observer_id,
                                event: event.to_owned(),
                                asynchronous: false,
                                outcome: "error".into(),
                                exit_code: None,
                                elapsed_ms: started.elapsed().as_millis(),
                                truncated: false,
                            },
                        );
                        return Err(error);
                    }
                };
                emit_observer(
                    self.observer.as_ref(),
                    HookExecutionEvent::HookResponse {
                        id: observer_id,
                        event: event.to_owned(),
                        asynchronous: false,
                        outcome: action_outcome(&result).to_owned(),
                        exit_code: result.exit_code,
                        elapsed_ms: started.elapsed().as_millis(),
                        truncated: result.truncated,
                    },
                );
                if result.blocked {
                    return Err(HookBlocked::new(event, result.detail).into());
                }
                if !result.succeeded {
                    bail!(
                        "{event} hook failed with exit {}{}",
                        result.exit_code.unwrap_or(-1),
                        if result.detail.is_empty() {
                            String::new()
                        } else {
                            format!(": {}", result.detail)
                        }
                    )
                }
                if let Some(value) = parse_hook_json(&result.body)? {
                    merge_hook_response(event, value, &mut outcome)?;
                } else if !result.body.trim().is_empty() {
                    outcome
                        .additional_context
                        .push(result.body.trim().to_owned());
                }
                validate_outcome_size(&outcome)?;
            }
        }
        Ok(outcome)
    }

    pub async fn finalize_async(&self) {
        self.finalize_shared_async(ASYNC_FINALIZE_TIMEOUT).await;
    }

    async fn finalize_shared_async(&self, finalize_timeout: Duration) {
        let tasks = {
            let mut tasks = self
                .async_tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.async_finalizing.store(true, Ordering::Release);
            std::mem::take(&mut *tasks)
        };
        let deadline = Instant::now() + finalize_timeout;
        for mut task in tasks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || timeout(remaining, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }

    pub async fn pre_tool(
        &self,
        tool: &str,
        input: Value,
        cwd: &std::path::Path,
    ) -> Result<(Value, Vec<String>)> {
        let outcome = self
            .run(
                "PreToolUse",
                Some(tool),
                json!({"tool_name": tool, "tool_input": input}),
                cwd,
            )
            .await?;
        Ok((
            outcome.updated_input.unwrap_or(input),
            outcome.additional_context,
        ))
    }

    pub async fn post_tool(
        &self,
        tool: &str,
        input: &Value,
        mut output: ToolOutput,
        cwd: &std::path::Path,
    ) -> ToolOutput {
        let event = if output.is_error {
            "PostToolUseFailure"
        } else {
            "PostToolUse"
        };
        let payload = json!({
            "tool_name": tool,
            "tool_input": input,
            "tool_output": output.content,
            "is_error": output.is_error,
        });
        match self.run(event, Some(tool), payload, cwd).await {
            Ok(outcome) => {
                if let Some(updated) = outcome.updated_output {
                    output.content = updated;
                    output.model_content = None;
                }
                if !outcome.additional_context.is_empty() {
                    output.append_context("Hook context", &outcome.additional_context.join("\n"));
                }
                output
            }
            Err(error) => {
                output.is_error = true;
                output.append_context("Post-tool hook failed", &format!("{error:#}"));
                output
            }
        }
    }
}

fn emit_observer(observer: Option<&HookObserver>, event: HookExecutionEvent) {
    if let Some(observer) = observer {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(&event)));
    }
}

fn action_outcome(result: &ActionResult) -> &'static str {
    if result.blocked {
        "blocked"
    } else if result.succeeded {
        "completed"
    } else {
        "failed"
    }
}

fn validate_outcome_size(outcome: &HookOutcome) -> Result<()> {
    let input_bytes = outcome
        .updated_input
        .as_ref()
        .map(serde_json::to_vec)
        .transpose()?
        .map_or(0, |value| value.len());
    let output_bytes = outcome.updated_output.as_ref().map_or(0, String::len);
    let context_bytes = outcome
        .additional_context
        .iter()
        .map(String::len)
        .sum::<usize>();
    let watch_path_bytes = outcome.watch_paths.iter().map(String::len).sum::<usize>();
    if input_bytes
        .saturating_add(output_bytes)
        .saturating_add(context_bytes)
        .saturating_add(watch_path_bytes)
        > MAX_HOOK_COMBINED_OUTPUT_BYTES
    {
        bail!("hook combined output 超过 {MAX_HOOK_COMBINED_OUTPUT_BYTES} 字节限制")
    }
    Ok(())
}

impl HookMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::All => true,
            Self::Patterns(patterns) => patterns.iter().any(|pattern| pattern.is_match(value)),
        }
    }

    fn matches_any(&self, values: &[&str]) -> bool {
        match self {
            Self::All => true,
            Self::Patterns(_) if values.is_empty() => self.matches(""),
            Self::Patterns(_) => values.iter().any(|value| self.matches(value)),
        }
    }
}

fn parse_matcher(value: &str) -> Result<HookMatcher> {
    if value.trim().is_empty() {
        return Ok(HookMatcher::All);
    }
    let patterns = value
        .split('|')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(|pattern| {
            Glob::new(pattern)
                .with_context(|| format!("无效 hook matcher: {pattern}"))
                .map(|glob| glob.compile_matcher())
        })
        .collect::<Result<Vec<_>>>()?;
    if patterns.is_empty() {
        bail!("hook matcher 没有有效 pattern")
    }
    Ok(HookMatcher::Patterns(patterns))
}

fn parse_action(raw: RawHookAction) -> Result<Arc<HookAction>> {
    let action = match raw {
        RawHookAction::Command {
            command,
            args,
            shell,
            timeout,
            timeout_ms,
            asynchronous,
            once,
            workspace_relative,
            condition,
            status_message,
        } => {
            if command.trim().is_empty()
                || command.len() > MAX_COMMAND_BYTES
                || command.contains('\0')
            {
                bail!("hook command 为空、过长或包含 NUL")
            }
            if args.as_ref().is_some_and(|args| {
                args.len() > MAX_ARGS
                    || args
                        .iter()
                        .any(|argument| argument.len() > MAX_ARG_BYTES || argument.contains('\0'))
            }) {
                bail!("hook args 超过数量/长度限制或包含 NUL")
            }
            let shell = match shell.as_deref() {
                None | Some("bash") => HookShell::Default,
                Some("powershell") => HookShell::PowerShell,
                Some(value) => bail!("hook shell 不支持: {value}"),
            };
            HookAction::Command(Box::new(HookCommand {
                command,
                args,
                shell,
                timeout: parse_hook_timeout(timeout, timeout_ms)?,
                asynchronous,
                once,
                workspace_relative,
                condition: condition
                    .map(|value| parse_hook_condition(&value))
                    .transpose()?,
                status_message: validate_status_message(status_message)?,
                fired: AtomicBool::new(false),
            }))
        }
        RawHookAction::McpTool {
            server,
            tool,
            input,
            condition,
            timeout,
            status_message,
            once,
            asynchronous,
        } => {
            if server.trim().is_empty()
                || server.len() > MAX_MCP_SERVER_BYTES
                || server.contains('\0')
            {
                bail!("mcp_tool hook server 为空、过长或包含 NUL")
            }
            if tool.trim().is_empty() || tool.len() > MAX_MCP_TOOL_BYTES || tool.contains('\0') {
                bail!("mcp_tool hook tool 为空、过长或包含 NUL")
            }
            let input = Value::Object(input.unwrap_or_default());
            validate_mcp_input_shape(&input)?;
            HookAction::McpTool(Box::new(McpToolHook {
                server,
                tool,
                input,
                timeout: parse_hook_timeout(timeout, None)?,
                asynchronous,
                once,
                condition: condition
                    .map(|value| parse_hook_condition(&value))
                    .transpose()?,
                status_message: validate_status_message(status_message)?,
                fired: AtomicBool::new(false),
            }))
        }
    };
    Ok(Arc::new(action))
}

fn parse_hook_timeout(seconds: Option<f64>, milliseconds: Option<u64>) -> Result<Duration> {
    if seconds.is_some() && milliseconds.is_some() {
        bail!("hook timeout 与 timeoutMs 不能同时设置")
    }
    if let Some(seconds) = seconds {
        if !seconds.is_finite() || seconds <= 0.0 {
            bail!("hook timeout 必须是正数")
        }
        let millis = (seconds * 1000.0).ceil();
        if millis > MAX_TIMEOUT_MS as f64 {
            bail!("hook timeout 超过 {MAX_TIMEOUT_MS}ms 限制")
        }
        return Ok(Duration::from_millis((millis as u64).max(1)));
    }
    Ok(Duration::from_millis(
        milliseconds
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(1, MAX_TIMEOUT_MS),
    ))
}

fn validate_status_message(value: Option<String>) -> Result<Option<String>> {
    if value.as_ref().is_some_and(|value| {
        value.is_empty() || value.len() > MAX_HOOK_STATUS_BYTES || value.contains('\0')
    }) {
        bail!("hook statusMessage 为空、过长或包含 NUL")
    }
    Ok(value)
}

impl HookAction {
    fn asynchronous(&self) -> bool {
        match self {
            Self::Command(hook) => hook.asynchronous,
            Self::McpTool(hook) => hook.asynchronous,
        }
    }

    fn status_message(&self) -> Option<&str> {
        match self {
            Self::Command(hook) => hook.status_message.as_deref(),
            Self::McpTool(hook) => hook.status_message.as_deref(),
        }
    }

    fn matches_condition(&self, event: &str, payload: &Value) -> bool {
        let condition = match self {
            Self::Command(hook) => hook.condition.as_ref(),
            Self::McpTool(hook) => hook.condition.as_ref(),
        };
        condition.is_none_or(|condition| condition.matches(event, payload))
    }

    /// Returns true when a once-only action was already consumed.
    fn mark_once_fired(&self) -> bool {
        match self {
            Self::Command(hook) if hook.once => hook.fired.swap(true, Ordering::AcqRel),
            Self::McpTool(hook) if hook.once => hook.fired.swap(true, Ordering::AcqRel),
            Self::Command(_) | Self::McpTool(_) => false,
        }
    }

    fn reset_once(&self) {
        match self {
            Self::Command(hook) if hook.once => hook.fired.store(false, Ordering::Release),
            Self::McpTool(hook) if hook.once => hook.fired.store(false, Ordering::Release),
            Self::Command(_) | Self::McpTool(_) => {}
        }
    }
}

fn parse_hook_condition(value: &str) -> Result<HookCondition> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > MAX_HOOK_CONDITION_BYTES
        || value.contains(['\0', '\n', '\r'])
    {
        bail!("hook if 条件为空、过长或包含控制字符")
    }
    let (tool, content) = match value.find('(') {
        Some(open) if open > 0 && value.ends_with(')') => {
            let content = &value[open + 1..value.len() - 1];
            (&value[..open], (!content.is_empty()).then_some(content))
        }
        Some(_) => bail!("hook if 条件必须使用 Tool(pattern) 语法"),
        None => (value, None),
    };
    if !tool.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':' | b'*' | b'?')
    }) {
        bail!("hook if tool pattern 无效")
    }
    let tool = Glob::new(tool)
        .context("hook if tool pattern 无效")?
        .compile_matcher();
    let content = content
        .filter(|content| *content != "*")
        .map(|content| {
            Glob::new(content)
                .context("hook if content pattern 无效")
                .map(|glob| glob.compile_matcher())
        })
        .transpose()?;
    Ok(HookCondition { tool, content })
}

impl HookCondition {
    fn matches(&self, event: &str, payload: &Value) -> bool {
        if !matches!(
            event,
            "PreToolUse"
                | "PostToolUse"
                | "PostToolUseFailure"
                | "PermissionRequest"
                | "PermissionDenied"
        ) {
            return false;
        }
        let Some(tool) = payload.get("tool_name").and_then(Value::as_str) else {
            return false;
        };
        if !self.tool.is_match(tool) {
            return false;
        }
        let Some(content) = &self.content else {
            return true;
        };
        let input = payload.get("tool_input").and_then(Value::as_object);
        let candidates = [
            payload.get("summary").and_then(Value::as_str),
            input
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("file_path"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("path"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("notebook_path"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("pattern"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("query"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("uri"))
                .and_then(Value::as_str),
            input
                .and_then(|input| input.get("url"))
                .and_then(Value::as_str),
        ];
        candidates
            .into_iter()
            .flatten()
            .any(|value| content.is_match(value))
    }
}

fn validate_mcp_input_shape(input: &Value) -> Result<()> {
    if !input.is_object() {
        bail!("mcp_tool hook input 必须是 object")
    }
    if serde_json::to_vec(input)?.len() > MAX_HOOK_INPUT_BYTES {
        bail!("mcp_tool hook input 超过 {MAX_HOOK_INPUT_BYTES} 字节限制")
    }
    let mut stack = vec![(input, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_MCP_INPUT_NODES {
            bail!("mcp_tool hook input 超过 {MAX_MCP_INPUT_NODES} 节点限制")
        }
        if depth > MAX_MCP_INPUT_DEPTH {
            bail!("mcp_tool hook input 超过 {MAX_MCP_INPUT_DEPTH} 层限制")
        }
        match value {
            Value::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                stack.extend(values.values().map(|value| (value, depth + 1)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    Ok(())
}

async fn execute_action(
    action: &HookAction,
    encoded: &[u8],
    payload: &Value,
    cwd: &std::path::Path,
    mcp_invoker: Option<&Arc<dyn McpHookInvoker>>,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<ActionResult> {
    match action {
        HookAction::Command(hook) => {
            let result = execute_command(hook, encoded, cwd, secret_env_scrubber).await?;
            Ok(ActionResult {
                succeeded: result.status.success(),
                blocked: result.status.code() == Some(2),
                body: result.stdout.clone(),
                detail: hook_detail(&result),
                exit_code: result.status.code(),
                truncated: result.truncated,
            })
        }
        HookAction::McpTool(hook) => {
            let invoker =
                mcp_invoker.context("mcp_tool hook 不可用：当前会话没有已连接 MCP invoker")?;
            let input = interpolate_mcp_input(&hook.input, payload)?;
            let call = McpHookCall {
                server: hook.server.clone(),
                tool: hook.tool.clone(),
                input,
                timeout: hook.timeout,
            };
            let result = timeout(
                hook.timeout.saturating_add(STREAM_DRAIN_GRACE),
                invoker.invoke(call),
            )
            .await
            .with_context(|| {
                format!("mcp_tool hook 超过 {}ms timeout", hook.timeout.as_millis())
            })??;
            let detail = if result.is_error {
                result.output.trim().to_owned()
            } else {
                String::new()
            };
            Ok(ActionResult {
                succeeded: !result.is_error,
                blocked: false,
                body: result.output,
                detail,
                exit_code: Some(i32::from(result.is_error)),
                truncated: false,
            })
        }
    }
}

fn interpolate_mcp_input(template: &Value, payload: &Value) -> Result<Value> {
    let mut placeholders = 0usize;
    let value = interpolate_mcp_value(template, payload, 0, &mut placeholders)?;
    validate_mcp_input_shape(&value)?;
    Ok(value)
}

fn interpolate_mcp_value(
    value: &Value,
    payload: &Value,
    depth: usize,
    placeholders: &mut usize,
) -> Result<Value> {
    if depth > MAX_MCP_INPUT_DEPTH {
        bail!("mcp_tool hook interpolation 超过深度限制")
    }
    match value {
        Value::String(value) => {
            interpolate_mcp_string(value, payload, placeholders).map(Value::String)
        }
        Value::Array(values) => values
            .iter()
            .map(|value| interpolate_mcp_value(value, payload, depth + 1, placeholders))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| {
                interpolate_mcp_value(value, payload, depth + 1, placeholders)
                    .map(|value| (key.clone(), value))
            })
            .collect::<Result<serde_json::Map<_, _>>>()
            .map(Value::Object),
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(value.clone()),
    }
}

fn interpolate_mcp_string(
    template: &str,
    payload: &Value,
    placeholders: &mut usize,
) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("${") {
        output.push_str(&remaining[..start]);
        let after = &remaining[start + 2..];
        let end = after
            .find('}')
            .context("mcp_tool hook interpolation 缺少 `}`")?;
        let path = &after[..end];
        *placeholders = placeholders.saturating_add(1);
        if *placeholders > MAX_MCP_PLACEHOLDERS {
            bail!("mcp_tool hook interpolation placeholder 超过限制")
        }
        validate_interpolation_path(path)?;
        let resolved = resolve_interpolation_path(payload, path)
            .with_context(|| format!("mcp_tool hook interpolation 路径不存在: {path}"))?;
        let replacement = match resolved {
            Value::String(value) => value.clone(),
            Value::Null => "null".to_owned(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(resolved)?,
        };
        output.push_str(&replacement);
        if output.len() > MAX_HOOK_INPUT_BYTES {
            bail!("mcp_tool hook interpolation 输出超过限制")
        }
        remaining = &after[end + 1..];
    }
    output.push_str(remaining);
    if output.len() > MAX_HOOK_INPUT_BYTES {
        bail!("mcp_tool hook interpolation 输出超过限制")
    }
    Ok(output)
}

fn validate_interpolation_path(path: &str) -> Result<()> {
    if path.is_empty() || path.len() > MAX_MCP_PLACEHOLDER_PATH_BYTES {
        bail!("mcp_tool hook interpolation path 长度无效")
    }
    if path.split('.').any(|segment| {
        segment.is_empty()
            || !segment.bytes().enumerate().all(|(index, byte)| {
                if index == 0 {
                    byte.is_ascii_alphabetic() || byte == b'_'
                } else {
                    byte.is_ascii_alphanumeric() || byte == b'_'
                }
            })
    }) {
        bail!("mcp_tool hook interpolation 仅允许 dotted identifier path")
    }
    Ok(())
}

fn resolve_interpolation_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = payload;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }
    Some(current)
}

async fn execute_command(
    hook: &HookCommand,
    input: &[u8],
    cwd: &std::path::Path,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<CommandResult> {
    let mut command = if let Some(args) = &hook.args {
        let executable = resolve_trusted_executable(&hook.command, cwd)?;
        let mut command = Command::new(executable);
        command.args(args);
        command
    } else {
        match hook.shell {
            HookShell::Default => {
                #[cfg(windows)]
                let command = {
                    let mut command = Command::new("cmd.exe");
                    command.args(["/D", "/S", "/C", &hook.command]);
                    command
                };
                #[cfg(not(windows))]
                let command = {
                    let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned());
                    let mut command = Command::new(shell);
                    command.args(["-lc", &hook.command]);
                    command
                };
                command
            }
            HookShell::PowerShell => {
                let mut command = Command::new("pwsh");
                command.args(["-NoProfile", "-NonInteractive", "-Command", &hook.command]);
                command
            }
        }
    };
    let execution_cwd = if hook.workspace_relative {
        cwd.to_owned()
    } else {
        dirs::home_dir()
            .and_then(|home| std::fs::canonicalize(home).ok())
            .unwrap_or_else(|| std::path::PathBuf::from(std::path::MAIN_SEPARATOR.to_string()))
    };
    command
        .current_dir(execution_cwd)
        .env("HARNESS_WORKSPACE", cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    secret_env_scrubber.scrub_tokio(&mut command);
    let (mut child, process_guard) =
        spawn_managed(&mut command).context("无法启动 hook command")?;
    let mut stdin = child.stdin.take().context("无法打开 hook stdin")?;
    let stdout = child.stdout.take().context("无法捕获 hook stdout")?;
    let stderr = child.stderr.take().context("无法捕获 hook stderr")?;
    let input = input.to_vec();
    let mut stdin_task = tokio::spawn(async move {
        let result = stdin.write_all(&input).await;
        let _ = stdin.shutdown().await;
        result
    });
    let mut stdout_task = tokio::spawn(capture_stream(stdout));
    let mut stderr_task = tokio::spawn(capture_stream(stderr));
    let status = match timeout(hook.timeout, child.wait()).await {
        Ok(status) => status.context("等待 hook command 失败")?,
        Err(_) => {
            process_guard.terminate();
            let _ = child.start_kill();
            let _ = child.wait().await;
            stdin_task.abort();
            stdout_task.abort();
            stderr_task.abort();
            bail!("hook command 超过 {}ms timeout", hook.timeout.as_millis())
        }
    };
    // A hook shell may exit while descendants keep inherited stdio open or
    // continue detached. End the owned tree before draining either case.
    process_guard.terminate();
    let streams = timeout(STREAM_DRAIN_GRACE, async {
        let _ = (&mut stdin_task).await;
        let stdout = (&mut stdout_task)
            .await
            .context("hook stdout worker 失败")?;
        let stderr = (&mut stderr_task)
            .await
            .context("hook stderr worker 失败")?;
        Ok::<_, anyhow::Error>((stdout, stderr))
    })
    .await;
    let ((stdout, stdout_truncated), (stderr, stderr_truncated)) = match streams {
        Ok(streams) => streams?,
        Err(_) => {
            process_guard.terminate();
            let _ = child.start_kill();
            let _ = child.wait().await;
            stdin_task.abort();
            stdout_task.abort();
            stderr_task.abort();
            bail!("hook command output streams did not close after process exit")
        }
    };
    process_guard.disarm();
    Ok(CommandResult {
        status,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        truncated: stdout_truncated || stderr_truncated,
    })
}

async fn capture_stream(mut stream: impl AsyncRead + Unpin) -> (Vec<u8>, bool) {
    let mut stored = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        let count = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return (stored, truncated),
            Ok(count) => count,
        };
        let keep = count.min(MAX_HOOK_OUTPUT_BYTES.saturating_sub(stored.len()));
        stored.extend_from_slice(&buffer[..keep]);
        truncated |= keep < count;
    }
}

fn hook_detail(result: &CommandResult) -> String {
    let detail = if result.stderr.trim().is_empty() {
        result.stdout.trim()
    } else {
        result.stderr.trim()
    };
    format!(
        "{}{}",
        detail,
        if result.truncated {
            " [output truncated]"
        } else {
            ""
        }
    )
}

fn parse_hook_json(stdout: &str) -> Result<Option<Value>> {
    let stdout = stdout.trim();
    if stdout.is_empty() || !stdout.starts_with('{') {
        return Ok(None);
    }
    serde_json::from_str(stdout)
        .map(Some)
        .context("hook stdout 以 `{` 开头但不是有效 JSON")
}

fn merge_hook_response(event: &str, value: Value, outcome: &mut HookOutcome) -> Result<()> {
    let object = value
        .as_object()
        .context("hook JSON response 必须是 object")?;
    let decision_blocked = object
        .get("decision")
        .and_then(Value::as_str)
        .is_some_and(|decision| matches!(decision, "block" | "deny"))
        || object.get("continue").and_then(Value::as_bool) == Some(false);
    if decision_blocked {
        let reason = object
            .get("reason")
            .or_else(|| object.get("stopReason"))
            .and_then(Value::as_str)
            .unwrap_or("hook returned a blocking decision");
        return Err(HookBlocked::new(event, reason).into());
    }
    let specific = object.get("hookSpecificOutput").and_then(Value::as_object);
    if let Some(input) = specific
        .and_then(|specific| specific.get("updatedInput"))
        .cloned()
    {
        if !input.is_object() {
            bail!("hook updatedInput 必须是 object")
        }
        outcome.updated_input = Some(input);
    }
    if let Some(output) = specific
        .and_then(|specific| specific.get("updatedToolOutput"))
        .or_else(|| object.get("updatedToolOutput"))
    {
        outcome.updated_output = Some(match output {
            Value::String(value) => value.clone(),
            value => serde_json::to_string_pretty(value)?,
        });
    }
    if event == "MessageDisplay" {
        if let Some(output) = specific
            .and_then(|specific| specific.get("displayContent"))
            .or_else(|| object.get("displayContent"))
        {
            outcome.updated_output = Some(
                output
                    .as_str()
                    .context("MessageDisplay displayContent 必须是 string")?
                    .to_owned(),
            );
        }
    }
    for context in [
        object.get("additionalContext"),
        specific.and_then(|specific| specific.get("additionalContext")),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    {
        if !context.is_empty() {
            outcome.additional_context.push(context.to_owned());
        }
    }
    for paths in [
        object.get("watchPaths"),
        specific.and_then(|specific| specific.get("watchPaths")),
    ]
    .into_iter()
    .flatten()
    {
        let paths = paths
            .as_array()
            .context("hook watchPaths 必须是 string array")?;
        for path in paths {
            let path = path.as_str().context("hook watchPaths 只能包含 string")?;
            if path.is_empty() || path.len() > MAX_HOOK_WATCH_PATH_BYTES || path.contains('\0') {
                bail!("hook watch path 为空、过长或包含 NUL")
            }
            if !outcome.watch_paths.iter().any(|existing| existing == path) {
                if outcome.watch_paths.len() >= MAX_HOOK_WATCH_PATHS {
                    bail!("hook watchPaths 超过 {MAX_HOOK_WATCH_PATHS} 项限制")
                }
                outcome.watch_paths.push(path.to_owned());
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    struct TestMcpInvoker {
        calls: std::sync::Mutex<Vec<McpHookCall>>,
        output: String,
        is_error: bool,
        delay: Duration,
        failure: Option<String>,
    }

    impl TestMcpInvoker {
        fn success(output: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                calls: std::sync::Mutex::new(Vec::new()),
                output: output.into(),
                is_error: false,
                delay: Duration::ZERO,
                failure: None,
            })
        }

        fn call_count(&self) -> usize {
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }
    }

    struct PendingInvocationGuard {
        cancelled: Arc<std::sync::atomic::AtomicUsize>,
        completed: bool,
    }

    impl Drop for PendingInvocationGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    struct GatedMcpInvoker {
        calls: std::sync::atomic::AtomicUsize,
        gate: tokio::sync::Semaphore,
        cancelled: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl GatedMcpInvoker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                gate: tokio::sync::Semaphore::new(0),
                cancelled: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            })
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn cancelled_count(&self) -> usize {
            self.cancelled.load(Ordering::Relaxed)
        }

        fn release(&self, count: usize) {
            self.gate.add_permits(count);
        }
    }

    #[async_trait::async_trait]
    impl McpHookInvoker for GatedMcpInvoker {
        async fn invoke(&self, _call: McpHookCall) -> Result<crate::mcp::McpHookResult> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut guard = PendingInvocationGuard {
                cancelled: Arc::clone(&self.cancelled),
                completed: false,
            };
            self.gate.acquire().await?.forget();
            guard.completed = true;
            Ok(crate::mcp::McpHookResult {
                output: "released".to_owned(),
                is_error: false,
            })
        }
    }

    #[async_trait::async_trait]
    impl McpHookInvoker for TestMcpInvoker {
        async fn invoke(&self, call: McpHookCall) -> Result<crate::mcp::McpHookResult> {
            let request_timeout = call.timeout;
            self.calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(call);
            if !self.delay.is_zero() {
                tokio::time::timeout(request_timeout, tokio::time::sleep(self.delay))
                    .await
                    .context("test mcp_tool timeout")?;
            }
            if let Some(failure) = &self.failure {
                bail!(failure.clone())
            }
            Ok(crate::mcp::McpHookResult {
                output: self.output.clone(),
                is_error: self.is_error,
            })
        }
    }

    #[test]
    fn mcp_tool_hook_schema_is_strict_and_bounded() {
        let valid = Settings {
            raw: json!({"hooks":{"PreToolUse":[{
                "matcher":"Write",
                "hooks":[{
                    "type":"mcp_tool",
                    "server":"configured",
                    "tool":"inspect",
                    "input":{"path":"${tool_input.file_path}","flags":[true, 2]},
                    "if":"Write(*)",
                    "timeout":1.25,
                    "statusMessage":"Inspecting",
                    "once":true,
                    "async":true
                }]
            }]}}),
        };
        assert!(HookRunner::from_settings(&valid).is_ok());

        for invalid in [
            json!({"type":"mcp_tool","server":"configured","tool":"inspect","input":[]}),
            json!({"type":"mcp_tool","server":"configured","tool":"inspect","unknown":true}),
            json!({"type":"mcp_tool","server":"","tool":"inspect"}),
            json!({"type":"mcp_tool","server":"configured","tool":"inspect","timeout":601}),
            json!({"type":"mcp_tool","server":"configured","tool":"inspect","if":"Write(bad"}),
        ] {
            let settings = Settings {
                raw: json!({"hooks":{"PreToolUse":[{"matcher":"Write","hooks":[invalid]}]}}),
            };
            assert!(
                HookRunner::from_settings(&settings).is_err(),
                "{settings:?}"
            );
        }
    }

    #[tokio::test]
    async fn mcp_tool_interpolates_strict_paths_and_merges_hook_outcome() {
        let settings = Settings {
            raw: json!({"hooks":{"PreToolUse":[{
                "matcher":"Write",
                "hooks":[{
                    "type":"mcp_tool",
                    "server":"configured",
                    "tool":"inspect",
                    "input":{
                        "path":"${tool_input.file_path}",
                        "label":"${tool_name}:${tool_input.meta.label}",
                        "nested":["count=${tool_input.count}", true]
                    },
                    "if":"Write(*.rs)",
                    "statusMessage":"Inspecting write"
                }]
            }]}}),
        };
        let invoker = TestMcpInvoker::success(
            r#"{"hookSpecificOutput":{"additionalContext":"mcp checked"}}"#,
        );
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer: HookObserver = Arc::new(move |event| {
            let _ = sender.send(event.clone());
        });
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_observer(Some(observer))
            .with_mcp_invoker(Some(invoker.clone()));
        let temp = tempfile::tempdir().unwrap();
        let outcome = runner
            .run(
                "PreToolUse",
                Some("Write"),
                json!({
                    "tool_name":"Write",
                    "tool_input":{"file_path":"src/lib.rs","meta":{"label":"safe"},"count":3}
                }),
                temp.path(),
            )
            .await
            .unwrap();
        assert_eq!(outcome.additional_context, ["mcp checked"]);
        let calls = invoker
            .calls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].server, "configured");
        assert_eq!(calls[0].tool, "inspect");
        assert_eq!(calls[0].input["path"], "src/lib.rs");
        assert_eq!(calls[0].input["label"], "Write:safe");
        assert_eq!(calls[0].input["nested"][0], "count=3");
        drop(calls);

        let started = receiver.try_recv().unwrap();
        assert!(matches!(
            started,
            HookExecutionEvent::HookStarted {
                status_message: Some(ref status),
                ..
            } if status == "Inspecting write"
        ));
    }

    #[tokio::test]
    async fn mcp_tool_missing_interpolation_path_fails_before_invocation() {
        let settings = Settings {
            raw: json!({"hooks":{"PreToolUse":[{
                "matcher":"Write",
                "hooks":[{
                    "type":"mcp_tool",
                    "server":"configured",
                    "tool":"inspect",
                    "input":{"path":"${tool_input.missing}"}
                }]
            }]}}),
        };
        let invoker = TestMcpInvoker::success("unused");
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_mcp_invoker(Some(invoker.clone()));
        let temp = tempfile::tempdir().unwrap();
        let error = runner
            .pre_tool("Write", json!({"file_path":"src/lib.rs"}), temp.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("路径不存在"));
        assert_eq!(invoker.call_count(), 0);
    }

    #[tokio::test]
    async fn mcp_tool_timeout_and_once_are_enforced() {
        let settings = Settings {
            raw: json!({"hooks":{"SessionStart":[{
                "matcher":"",
                "hooks":[{
                    "type":"mcp_tool",
                    "server":"configured",
                    "tool":"slow",
                    "timeout":0.001,
                    "once":true
                }]
            }]}}),
        };
        let invoker = Arc::new(TestMcpInvoker {
            calls: std::sync::Mutex::new(Vec::new()),
            output: "late".to_owned(),
            is_error: false,
            delay: Duration::from_millis(50),
            failure: None,
        });
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_mcp_invoker(Some(invoker.clone()));
        let temp = tempfile::tempdir().unwrap();
        let error = runner
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timeout"));
        assert_eq!(invoker.call_count(), 1);
        let outcome = runner
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        assert!(outcome.additional_context.is_empty());
        assert_eq!(invoker.call_count(), 1);
    }

    #[tokio::test]
    async fn asynchronous_mcp_hooks_share_the_global_concurrency_cap() {
        let action = json!({
            "type":"mcp_tool",
            "server":"configured",
            "tool":"observe",
            "timeout":1,
            "async":true
        });
        let rules = [16usize, 16, 1]
            .into_iter()
            .map(|count| json!({"matcher":"","hooks":vec![action.clone(); count]}))
            .collect::<Vec<_>>();
        let settings = Settings {
            raw: json!({"hooks":{"SessionStart":rules}}),
        };
        let invoker = Arc::new(TestMcpInvoker {
            calls: std::sync::Mutex::new(Vec::new()),
            output: "observed".to_owned(),
            is_error: false,
            delay: Duration::from_millis(20),
            failure: None,
        });
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer: HookObserver = Arc::new(move |event| {
            let _ = sender.send(event.clone());
        });
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_observer(Some(observer))
            .with_mcp_invoker(Some(invoker.clone()));
        let temp = tempfile::tempdir().unwrap();
        runner
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        runner.finalize_async().await;
        assert_eq!(invoker.call_count(), MAX_ASYNC_HOOKS);

        let mut dropped = 0usize;
        let mut completed = 0usize;
        while let Ok(event) = receiver.try_recv() {
            if let HookExecutionEvent::HookResponse { outcome, .. } = event {
                dropped += usize::from(outcome == "dropped");
                completed += usize::from(outcome == "completed");
            }
        }
        assert_eq!(dropped, 1);
        assert_eq!(completed, MAX_ASYNC_HOOKS);
    }

    #[test]
    fn combined_hook_output_is_bounded() {
        let outcome = HookOutcome {
            additional_context: vec!["x".repeat(MAX_HOOK_COMBINED_OUTPUT_BYTES + 1)],
            ..HookOutcome::default()
        };
        assert!(validate_outcome_size(&outcome).is_err());
    }

    #[test]
    fn file_changed_matchers_are_exposed_as_bounded_watch_patterns() {
        let settings = Settings {
            raw: json!({"hooks":{"FileChanged":[{
                "matcher":".env | config/*.json | .env",
                "hooks":[{"type":"command","command":"true"}]
            }]}}),
        };
        let root = HookRunner::from_settings(&settings).unwrap();
        assert_eq!(
            root.file_watch_patterns().unwrap(),
            vec![".env".to_owned(), "config/*.json".to_owned()]
        );

        let scoped = root
            .with_scoped_hooks(&json!({"FileChanged":[{
                "matcher":"nested/**/settings.toml",
                "hooks":[{"type":"command","command":"true"}]
            }]}))
            .unwrap();
        assert_eq!(
            scoped.file_watch_patterns().unwrap(),
            vec![
                ".env".to_owned(),
                "config/*.json".to_owned(),
                "nested/**/settings.toml".to_owned()
            ]
        );
    }

    #[test]
    fn hook_watch_paths_parse_deduplicate_and_reject_invalid_shapes() {
        let mut outcome = HookOutcome::default();
        merge_hook_response(
            "SessionStart",
            json!({
                "watchPaths":["/tmp/one"],
                "hookSpecificOutput":{"watchPaths":["/tmp/two", "/tmp/one"]}
            }),
            &mut outcome,
        )
        .unwrap();
        assert_eq!(
            outcome.watch_paths,
            vec!["/tmp/one".to_owned(), "/tmp/two".to_owned()]
        );

        for invalid in [
            json!({"watchPaths":"/tmp/not-an-array"}),
            json!({"watchPaths":[1]}),
            json!({"watchPaths":[""]}),
            json!({"watchPaths":["x".repeat(MAX_HOOK_WATCH_PATH_BYTES + 1)]}),
        ] {
            assert!(
                merge_hook_response("FileChanged", invalid, &mut HookOutcome::default()).is_err()
            );
        }
    }

    #[test]
    fn plugin_hooks_share_the_same_parser_and_global_limits() {
        let settings = Settings {
            raw: json!({"hooks":{"SessionStart":[{
                "matcher":"", "hooks":[{"type":"command", "command":"true"}]
            }]}}),
        };
        let plugin = json!({"SessionEnd":[{
            "matcher":"", "hooks":[{"type":"command", "command":"true"}]
        }]});
        let runner = HookRunner::from_settings_and_plugins(&settings, &plugin).unwrap();
        assert!(!runner.is_empty());

        let invalid = json!({"UnknownEvent":[]});
        assert!(HookRunner::from_settings_and_plugins(&settings, &invalid).is_err());
    }

    #[test]
    fn scoped_hooks_are_visible_without_mutating_the_root_runner() {
        let root = HookRunner::from_settings(&Settings { raw: json!({}) }).unwrap();
        assert!(root.is_empty());
        let scoped = root
            .with_scoped_hooks(&json!({"UserPromptSubmit":[{
                "matcher":"", "hooks":[{"type":"command", "command":"true"}]
            }]}))
            .unwrap();
        assert!(!scoped.is_empty());
        assert!(root.is_empty());
    }

    fn scoped_async_mcp_hooks(count: usize) -> Value {
        let action = json!({
            "type":"mcp_tool",
            "server":"configured",
            "tool":"observe",
            "timeout":10,
            "async":true
        });
        let rules = (0..count.div_ceil(MAX_COMMANDS_PER_RULE))
            .map(|index| {
                let remaining = count.saturating_sub(index * MAX_COMMANDS_PER_RULE);
                let actions = remaining.min(MAX_COMMANDS_PER_RULE);
                json!({"matcher":"", "hooks":vec![action.clone(); actions]})
            })
            .collect::<Vec<_>>();
        json!({"SessionStart": rules})
    }

    async fn wait_for_invocation(invoker: &GatedMcpInvoker) {
        timeout(Duration::from_secs(1), async {
            while invoker.call_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("scoped async hook did not start");
    }

    #[tokio::test]
    async fn scoped_hooks_share_async_cap_task_registry_and_observer_ids() {
        let invoker = GatedMcpInvoker::new();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer: HookObserver = Arc::new(move |event| {
            let _ = sender.send(event.clone());
        });
        let root = HookRunner::from_settings(&Settings { raw: json!({}) })
            .unwrap()
            .with_observer(Some(observer))
            .with_mcp_invoker(Some(invoker.clone()));
        let first = root.with_scoped_hooks(&scoped_async_mcp_hooks(20)).unwrap();
        let second = root.with_scoped_hooks(&scoped_async_mcp_hooks(20)).unwrap();
        let temp = tempfile::tempdir().unwrap();

        first
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        second
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        drop(first);
        drop(second);
        invoker.release(MAX_ASYNC_HOOKS);
        root.finalize_async().await;

        assert_eq!(invoker.call_count(), MAX_ASYNC_HOOKS);
        assert_eq!(invoker.cancelled_count(), 0);
        let mut started_ids = Vec::new();
        let mut dropped = 0usize;
        while let Ok(event) = receiver.try_recv() {
            match event {
                HookExecutionEvent::HookStarted { id, .. } => started_ids.push(id),
                HookExecutionEvent::HookResponse { outcome, .. } => {
                    dropped += usize::from(outcome == "dropped");
                }
            }
        }
        assert_eq!(started_ids.len(), 40);
        assert_eq!(
            started_ids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            started_ids.len(),
            "scoped observers must share one id sequence"
        );
        assert_eq!(dropped, 40 - MAX_ASYNC_HOOKS);
    }

    #[tokio::test]
    async fn root_finalize_waits_for_dropped_scope_and_aborts_it_at_deadline() {
        let temp = tempfile::tempdir().unwrap();

        let waiting_invoker = GatedMcpInvoker::new();
        let waiting_root = HookRunner::from_settings(&Settings { raw: json!({}) })
            .unwrap()
            .with_mcp_invoker(Some(waiting_invoker.clone()));
        let waiting_scope = waiting_root
            .with_scoped_hooks(&scoped_async_mcp_hooks(1))
            .unwrap();
        waiting_scope
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        drop(waiting_scope);
        wait_for_invocation(&waiting_invoker).await;
        let waiting_finalize = tokio::spawn({
            let runner = waiting_root.clone();
            async move { runner.finalize_async().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting_finalize.is_finished(),
            "root finalize must retain async work launched by a dropped scope"
        );
        waiting_invoker.release(1);
        timeout(Duration::from_secs(1), waiting_finalize)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(waiting_invoker.cancelled_count(), 0);

        let aborting_invoker = GatedMcpInvoker::new();
        let aborting_root = HookRunner::from_settings(&Settings { raw: json!({}) })
            .unwrap()
            .with_mcp_invoker(Some(aborting_invoker.clone()));
        let aborting_scope = aborting_root
            .with_scoped_hooks(&scoped_async_mcp_hooks(1))
            .unwrap();
        aborting_scope
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        drop(aborting_scope);
        wait_for_invocation(&aborting_invoker).await;
        aborting_root
            .finalize_shared_async(Duration::from_millis(10))
            .await;
        assert_eq!(aborting_invoker.cancelled_count(), 1);

        let late_scope = aborting_root
            .with_scoped_hooks(&scoped_async_mcp_hooks(1))
            .unwrap();
        late_scope
            .run("SessionStart", None, json!({}), temp.path())
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            aborting_invoker.call_count(),
            1,
            "a finalized root lifecycle must reject new scoped async work"
        );
    }

    #[tokio::test]
    async fn pre_tool_hook_can_update_input_and_match_exact_tool() {
        let settings = Settings {
            raw: json!({"hooks": {"PreToolUse": [{
                "matcher": "Write|Edit",
                "hooks": [{
                    "type": "command",
                    "command": "printf '%s' '{\"hookSpecificOutput\":{\"updatedInput\":{\"file_path\":\"safe.txt\",\"content\":\"updated\"}}}'"
                }]
            }]}}),
        };
        let runner = HookRunner::from_settings(&settings).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let (input, _) = runner
            .pre_tool(
                "Write",
                json!({"file_path": "original.txt", "content": "old"}),
                temp.path(),
            )
            .await
            .unwrap();
        assert_eq!(input["file_path"], "safe.txt");
        assert_eq!(input["content"], "updated");
    }

    #[tokio::test]
    async fn exit_two_blocks_operation() {
        let settings = Settings {
            raw: json!({"hooks": {"PreToolUse": [{
                "matcher": "Bash",
                "hooks": [{"type": "command", "command": "printf denied >&2; exit 2"}]
            }]}}),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer: HookObserver = Arc::new(move |event| {
            let _ = sender.send(event.clone());
        });
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_observer(Some(observer));
        let temp = tempfile::tempdir().unwrap();
        let error = runner
            .pre_tool("Bash", json!({"command": "true"}), temp.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("blocked"));
        assert_eq!(blocking_feedback(&error).as_deref(), Some("denied"));

        let started = receiver.try_recv().unwrap();
        let response = receiver.try_recv().unwrap();
        let HookExecutionEvent::HookStarted {
            id: started_id,
            asynchronous: false,
            ..
        } = started
        else {
            panic!("expected synchronous hook_started event")
        };
        let HookExecutionEvent::HookResponse {
            id: response_id,
            asynchronous: false,
            outcome,
            exit_code: Some(2),
            ..
        } = response
        else {
            panic!("expected synchronous hook_response event")
        };
        assert_eq!(started_id, response_id);
        assert_eq!(outcome, "blocked");
    }

    #[tokio::test]
    async fn async_observer_emits_terminal_state_without_sensitive_payload() {
        let settings = Settings {
            raw: json!({"hooks": {"SessionStart": [{
                "matcher": "",
                "hooks": [{
                    "type": "command",
                    "command": "printf done",
                    "async": true
                }]
            }]}}),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let observer: HookObserver = Arc::new(move |event| {
            let _ = sender.send(event.clone());
        });
        let runner = HookRunner::from_settings(&settings)
            .unwrap()
            .with_observer(Some(observer));
        let temp = tempfile::tempdir().unwrap();
        runner
            .run(
                "SessionStart",
                None,
                json!({"token": "observer-secret-value"}),
                temp.path(),
            )
            .await
            .unwrap();

        runner.finalize_async().await;
        let started = receiver.try_recv().unwrap();
        let response = receiver.try_recv().unwrap();
        let HookExecutionEvent::HookStarted {
            id: started_id,
            event: started_event,
            asynchronous: true,
            ..
        } = &started
        else {
            panic!("expected asynchronous hook_started event")
        };
        let HookExecutionEvent::HookResponse {
            id: response_id,
            event: response_event,
            asynchronous: true,
            outcome,
            exit_code: Some(0),
            ..
        } = &response
        else {
            panic!("expected asynchronous hook_response event")
        };
        assert_eq!(started_id, response_id);
        assert_eq!(started_event, "SessionStart");
        assert_eq!(started_event, response_event);
        assert_eq!(outcome, "completed");

        let serialized = serde_json::to_string(&[started, response]).unwrap();
        assert!(serialized.contains("\"type\":\"hook_started\""));
        assert!(serialized.contains("\"type\":\"hook_response\""));
        assert!(!serialized.contains("observer-secret-value"));
        assert!(!serialized.contains(&temp.path().display().to_string()));
        assert!(!serialized.contains("command"));
        assert!(!serialized.contains("stdout"));
    }
}
