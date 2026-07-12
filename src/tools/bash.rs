use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{fs::OpenOptions, process::Command, time::timeout};
use uuid::Uuid;

use super::{BackgroundTask, Tool, ToolContext, ToolOutput, object_schema, parse_input};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_OUTPUT_CHARS: usize = 30_000;

#[derive(Deserialize)]
struct Input {
    command: String,
    timeout: Option<u64>,
    #[serde(default)]
    run_in_background: bool,
    description: Option<String>,
}

pub struct BashTool;

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &'static str {
        "Bash"
    }
    fn description(&self) -> &'static str {
        "Executes a shell command in the working directory with timeout support. Long commands may run in the background."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "command": {"type": "string"},
                "timeout": {"type": "integer", "minimum": 1, "maximum": MAX_TIMEOUT_MS},
                "run_in_background": {"type": "boolean"},
                "description": {"type": "string"}
            }),
            &["command"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        false
    }
    fn destructive(&self, input: &Value) -> bool {
        let command = input.get("command").and_then(Value::as_str).unwrap_or("");
        [
            "rm ",
            "rm\t",
            "git reset",
            "git clean",
            "mkfs",
            "shutdown",
            "reboot",
        ]
        .iter()
        .any(|needle| command.contains(needle))
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        if input.command.trim().is_empty() {
            bail!("command 不能为空")
        }
        let _description = input.description;
        let shell = std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned());
        if input.run_in_background {
            return spawn_background(context, &shell, input.command).await;
        }
        let timeout_ms = input
            .timeout
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .min(MAX_TIMEOUT_MS);
        let mut command = Command::new(shell);
        command
            .arg("-lc")
            .arg(&input.command)
            .current_dir(&context.cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = timeout(Duration::from_millis(timeout_ms), command.output())
            .await
            .map_err(|_| anyhow::anyhow!("命令在 {timeout_ms}ms 后超时并已终止"))?
            .context("无法启动 shell 命令")?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let mut combined = match (stdout.trim().is_empty(), stderr.trim().is_empty()) {
            (false, false) => format!("{}\n{}", stdout.trim_end(), stderr.trim_end()),
            (false, true) => stdout.trim_end().to_owned(),
            (true, false) => stderr.trim_end().to_owned(),
            (true, true) => String::new(),
        };
        combined = truncate_middle(&combined, MAX_OUTPUT_CHARS);
        if !output.status.success() {
            if !combined.is_empty() {
                combined.push('\n');
            }
            combined.push_str(&format!("Exit code {}", output.status.code().unwrap_or(-1)));
            return Ok(ToolOutput::error(combined));
        }
        if combined.is_empty() {
            combined = "Command completed successfully with no output".into();
        }
        Ok(ToolOutput::success(combined))
    }
}

async fn spawn_background(
    context: &ToolContext,
    shell: &str,
    command_text: String,
) -> Result<ToolOutput> {
    let id = Uuid::new_v4().to_string();
    let base = dirs::home_dir()
        .context("无法确定主目录")?
        .join(".agent-harness/tasks");
    tokio::fs::create_dir_all(&base).await?;
    let output_path = base.join(format!("{id}.output"));
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)
        .await?
        .into_std()
        .await;
    let stderr = stdout.try_clone()?;
    let child = Command::new(shell)
        .arg("-lc")
        .arg(&command_text)
        .current_dir(&context.cwd)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .context("无法启动后台命令")?;
    context.tasks.lock().await.insert(
        id.clone(),
        BackgroundTask {
            child,
            output_path: output_path.clone(),
            command: command_text,
        },
    );
    Ok(ToolOutput::success(format!(
        "Command running in background with ID: {id}\nOutput: {}",
        output_path.display()
    )))
}

fn truncate_middle(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_owned();
    }
    let half = max / 2;
    let head = value.chars().take(half).collect::<String>();
    let tail = value
        .chars()
        .rev()
        .take(half)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{head}\n... [output truncated] ...\n{tail}")
}
