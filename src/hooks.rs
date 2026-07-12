use std::{
    collections::HashMap,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use globset::Glob;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
    time::timeout,
};

use crate::{config::Settings, process::ProcessTreeGuard, tools::ToolOutput};

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
const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const STREAM_DRAIN_GRACE: Duration = Duration::from_secs(1);

const SUPPORTED_EVENTS: &[&str] = &[
    "PreToolUse",
    "PostToolUse",
    "PostToolUseFailure",
    "UserPromptSubmit",
    "SessionStart",
    "SessionEnd",
    "SubagentStart",
    "SubagentStop",
    "PreCompact",
    "PostCompact",
    "WorktreeCreate",
    "WorktreeRemove",
    "CwdChanged",
];

#[derive(Clone)]
pub struct HookRunner {
    events: Arc<HashMap<String, Vec<HookRule>>>,
    async_slots: Arc<Semaphore>,
}

impl Default for HookRunner {
    fn default() -> Self {
        Self {
            events: Arc::new(HashMap::new()),
            async_slots: Arc::new(Semaphore::new(MAX_ASYNC_HOOKS)),
        }
    }
}

struct HookRule {
    matcher: HookMatcher,
    commands: Vec<Arc<HookCommand>>,
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
    fired: AtomicBool,
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
    hooks: Vec<RawHookCommand>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHookCommand {
    #[serde(rename = "type")]
    command_type: String,
    command: String,
    args: Option<Vec<String>>,
    shell: Option<String>,
    #[serde(rename = "timeoutMs")]
    timeout_ms: Option<u64>,
    #[serde(rename = "async", default)]
    asynchronous: bool,
    #[serde(default)]
    once: bool,
}

#[derive(Default)]
pub struct HookOutcome {
    pub updated_input: Option<Value>,
    pub updated_output: Option<String>,
    pub additional_context: Vec<String>,
}

struct CommandResult {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
    truncated: bool,
}

impl HookRunner {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let Some(events) = settings.raw.get("hooks") else {
            return Ok(Self::default());
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
                if raw.hooks.is_empty() || raw.hooks.len() > MAX_COMMANDS_PER_RULE {
                    bail!("hooks.{event} 每条规则必须有 1..={MAX_COMMANDS_PER_RULE} 个命令")
                }
                let matcher = parse_matcher(&raw.matcher)?;
                let commands = raw
                    .hooks
                    .into_iter()
                    .map(parse_command)
                    .collect::<Result<Vec<_>>>()?;
                parsed_rules.push(HookRule { matcher, commands });
            }
            parsed.insert(event.clone(), parsed_rules);
        }
        Ok(Self {
            events: Arc::new(parsed),
            async_slots: Arc::new(Semaphore::new(MAX_ASYNC_HOOKS)),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub async fn run(
        &self,
        event: &str,
        matcher_value: Option<&str>,
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
        let mut matched_commands = 0usize;
        for rule in rules {
            if !rule.matcher.matches(matcher_value.unwrap_or("")) {
                continue;
            }
            for command in &rule.commands {
                if command.once && command.fired.swap(true, Ordering::AcqRel) {
                    continue;
                }
                matched_commands = matched_commands.saturating_add(1);
                if matched_commands > MAX_MATCHED_COMMANDS_PER_EVENT {
                    bail!("{event} hook 匹配命令超过 {MAX_MATCHED_COMMANDS_PER_EVENT} 个限制")
                }
                if command.asynchronous {
                    let permit = match Arc::clone(&self.async_slots).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            if command.once {
                                command.fired.store(false, Ordering::Release);
                            }
                            continue;
                        }
                    };
                    let command = Arc::clone(command);
                    let encoded = encoded.clone();
                    let cwd = cwd.to_owned();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _ = execute_command(&command, &encoded, &cwd).await;
                    });
                    continue;
                }
                let result = execute_command(command, &encoded, cwd).await?;
                let detail = hook_detail(&result);
                if result.status.code() == Some(2) {
                    bail!(
                        "{event} hook blocked operation{}",
                        if detail.is_empty() {
                            String::new()
                        } else {
                            format!(": {detail}")
                        }
                    )
                }
                if !result.status.success() {
                    bail!(
                        "{event} hook failed with exit {}{}",
                        result.status.code().unwrap_or(-1),
                        if detail.is_empty() {
                            String::new()
                        } else {
                            format!(": {detail}")
                        }
                    )
                }
                if let Some(value) = parse_hook_json(&result.stdout)? {
                    merge_hook_response(event, value, &mut outcome)?;
                } else if !result.stdout.trim().is_empty() {
                    outcome
                        .additional_context
                        .push(result.stdout.trim().to_owned());
                }
                validate_outcome_size(&outcome)?;
            }
        }
        Ok(outcome)
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
                }
                if !outcome.additional_context.is_empty() {
                    output.content.push_str("\n\n[Hook context]\n");
                    output
                        .content
                        .push_str(&outcome.additional_context.join("\n"));
                }
                output
            }
            Err(error) => {
                output.is_error = true;
                output
                    .content
                    .push_str(&format!("\nPost-tool hook failed: {error:#}"));
                output
            }
        }
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
    if input_bytes
        .saturating_add(output_bytes)
        .saturating_add(context_bytes)
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

fn parse_command(raw: RawHookCommand) -> Result<Arc<HookCommand>> {
    if raw.command_type != "command" {
        bail!("当前只支持 type=command 的开放 hook")
    }
    if raw.command.trim().is_empty()
        || raw.command.len() > MAX_COMMAND_BYTES
        || raw.command.contains('\0')
    {
        bail!("hook command 为空、过长或包含 NUL")
    }
    if raw.args.as_ref().is_some_and(|args| {
        args.len() > MAX_ARGS
            || args
                .iter()
                .any(|argument| argument.len() > MAX_ARG_BYTES || argument.contains('\0'))
    }) {
        bail!("hook args 超过数量/长度限制或包含 NUL")
    }
    let shell = match raw.shell.as_deref() {
        None | Some("bash") => HookShell::Default,
        Some("powershell") => HookShell::PowerShell,
        Some(value) => bail!("hook shell 不支持: {value}"),
    };
    Ok(Arc::new(HookCommand {
        command: raw.command,
        args: raw.args,
        shell,
        timeout: Duration::from_millis(
            raw.timeout_ms
                .unwrap_or(DEFAULT_TIMEOUT_MS)
                .clamp(1, MAX_TIMEOUT_MS),
        ),
        asynchronous: raw.asynchronous,
        once: raw.once,
        fired: AtomicBool::new(false),
    }))
}

async fn execute_command(
    hook: &HookCommand,
    input: &[u8],
    cwd: &std::path::Path,
) -> Result<CommandResult> {
    let mut command = if let Some(args) = &hook.args {
        let mut command = Command::new(&hook.command);
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
    command
        .current_dir(cwd)
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().context("无法启动 hook command")?;
    let process_group = child.id();
    let mut stdin = child.stdin.take().context("无法打开 hook stdin")?;
    let stdout = child.stdout.take().context("无法捕获 hook stdout")?;
    let stderr = child.stderr.take().context("无法捕获 hook stderr")?;
    let mut process_guard = ProcessTreeGuard::new(process_group);
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
        bail!("{event} hook blocked operation: {reason}")
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
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn combined_hook_output_is_bounded() {
        let outcome = HookOutcome {
            additional_context: vec!["x".repeat(MAX_HOOK_COMBINED_OUTPUT_BYTES + 1)],
            ..HookOutcome::default()
        };
        assert!(validate_outcome_size(&outcome).is_err());
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
        let runner = HookRunner::from_settings(&settings).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let error = runner
            .pre_tool("Bash", json!({"command": "true"}), temp.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("blocked"));
    }
}
