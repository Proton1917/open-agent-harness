use std::{
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    time::timeout,
};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

#[derive(Debug, Deserialize)]
struct Input {
    pattern: String,
    path: Option<String>,
    glob: Option<String>,
    #[serde(default)]
    output_mode: OutputMode,
    #[serde(rename = "-B")]
    before: Option<u32>,
    #[serde(rename = "-A")]
    after: Option<u32>,
    #[serde(rename = "-C")]
    context_short: Option<u32>,
    context: Option<u32>,
    #[serde(rename = "-n", default = "default_line_numbers")]
    line_numbers: bool,
    #[serde(rename = "-i", default)]
    case_insensitive: bool,
    r#type: Option<String>,
    head_limit: Option<usize>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    multiline: bool,
}

fn default_line_numbers() -> bool {
    true
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutputMode {
    Content,
    #[default]
    FilesWithMatches,
    Count,
}

pub struct GrepTool;

const MAX_STDOUT_BYTES: usize = 512 * 1024;
const MAX_STDERR_BYTES: usize = 64 * 1024;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(60);

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &'static str {
        "Grep"
    }
    fn description(&self) -> &'static str {
        "Searches file contents with ripgrep. Supports content, files_with_matches, and count modes plus pagination."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "pattern": {"type": "string", "maxLength": 65536}, "path": {"type": "string", "maxLength": 4096},
                "glob": {"type": "string", "maxLength": 4096},
                "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]},
                "-B": {"type": "integer", "minimum": 0}, "-A": {"type": "integer", "minimum": 0},
                "-C": {"type": "integer", "minimum": 0}, "context": {"type": "integer", "minimum": 0},
                "-n": {"type": "boolean"}, "-i": {"type": "boolean"}, "type": {"type": "string", "maxLength": 128},
                "head_limit": {"type": "integer", "minimum": 0, "maximum": 100000}, "offset": {"type": "integer", "minimum": 0, "maximum": 10000000},
                "multiline": {"type": "boolean"}
            }),
            &["pattern"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &["path"]
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("pattern")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let path = match &input.path {
            Some(path) => context.resolve_path(path)?,
            None => context.cwd.clone(),
        };
        if !path.exists() {
            bail!("搜索路径不存在: {}", path.display())
        }
        let mut command = Command::new("rg");
        command.arg("--color=never").arg("--no-heading");
        for dir in [".git", ".svn", ".hg", ".bzr", ".jj", ".sl"] {
            command.arg("--glob").arg(format!("!{dir}/**"));
        }
        match input.output_mode {
            OutputMode::Content => {
                if input.line_numbers {
                    command.arg("--line-number");
                }
                if let Some(value) = input.before {
                    command.arg("-B").arg(value.to_string());
                }
                if let Some(value) = input.after {
                    command.arg("-A").arg(value.to_string());
                }
                if let Some(value) = input.context.or(input.context_short) {
                    command.arg("-C").arg(value.to_string());
                }
            }
            OutputMode::FilesWithMatches => {
                command.arg("--files-with-matches");
            }
            OutputMode::Count => {
                command.arg("--count");
            }
        }
        if input.case_insensitive {
            command.arg("--ignore-case");
        }
        if input.multiline {
            command.arg("--multiline").arg("--multiline-dotall");
        }
        if let Some(glob) = &input.glob {
            command.arg("--glob").arg(glob);
        }
        if let Some(kind) = &input.r#type {
            command.arg("--type").arg(kind);
        }
        let search_path = path
            .strip_prefix(&context.cwd)
            .ok()
            .filter(|relative| !relative.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let search_path = if path.starts_with(&context.cwd) {
            search_path
        } else {
            &path
        };
        command
            .arg("--")
            .arg(&input.pattern)
            .arg(search_path)
            .current_dir(&context.cwd);
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env_remove("HARNESS_API_KEY")
            .env_remove("HARNESS_AUTH_TOKEN");
        #[cfg(unix)]
        command.process_group(0);
        let started = Instant::now();
        let mut child = command
            .spawn()
            .context("无法启动 rg；请确认 ripgrep 已安装")?;
        let process_group_id = child.id();
        let stdout = child.stdout.take().context("无法捕获 rg stdout")?;
        let stderr = child.stderr.take().context("无法捕获 rg stderr")?;
        let mut stdout_task = tokio::spawn(read_up_to(stdout, MAX_STDOUT_BYTES));
        let mut stderr_task = tokio::spawn(drain_capped(stderr, MAX_STDERR_BYTES));
        let (stdout, byte_truncated) = match timeout(remaining(started), &mut stdout_task).await {
            Ok(result) => result.context("rg stdout reader 失败")??,
            Err(_) => {
                terminate_search(&mut child, process_group_id).await;
                stdout_task.abort();
                stderr_task.abort();
                bail!("rg 搜索超过 {} 秒限制", SEARCH_TIMEOUT.as_secs())
            }
        };
        let status = if byte_truncated {
            terminate_search(&mut child, process_group_id).await;
            None
        } else {
            match timeout(remaining(started), child.wait()).await {
                Ok(status) => Some(status.context("等待 rg 结束失败")?),
                Err(_) => {
                    terminate_search(&mut child, process_group_id).await;
                    stderr_task.abort();
                    bail!("rg 搜索超过 {} 秒限制", SEARCH_TIMEOUT.as_secs())
                }
            }
        };
        let stderr = match timeout(remaining(started), &mut stderr_task).await {
            Ok(result) => result.context("rg stderr reader 失败")??,
            Err(_) => {
                terminate_search(&mut child, process_group_id).await;
                stderr_task.abort();
                bail!("rg 搜索超过 {} 秒限制", SEARCH_TIMEOUT.as_secs())
            }
        };
        if let Some(status) = status
            && !status.success()
            && status.code() != Some(1)
        {
            bail!("rg 失败: {}", String::from_utf8_lossy(&stderr).trim())
        }
        let raw = String::from_utf8_lossy(&stdout);
        let lines: Vec<&str> = raw.lines().collect();
        let limit = match input.head_limit {
            Some(0) => usize::MAX,
            Some(n) => n,
            None => 250,
        };
        let selected = lines
            .iter()
            .skip(input.offset)
            .take(limit)
            .copied()
            .collect::<Vec<_>>();
        if selected.is_empty() {
            if byte_truncated {
                return Ok(ToolOutput::success(format!(
                    "No results were captured at offset {}; rg output exceeded the {} byte limit",
                    input.offset, MAX_STDOUT_BYTES
                )));
            }
            return Ok(ToolOutput::success("No matches found"));
        }
        let truncated = byte_truncated || lines.len().saturating_sub(input.offset) > selected.len();
        let mut result = selected.join("\n");
        if truncated {
            result.push_str(&format!(
                "\n\n[Showing results with pagination = limit: {}, offset: {}]",
                limit, input.offset
            ));
        }
        Ok(ToolOutput::success(result))
    }
}

fn remaining(started: Instant) -> Duration {
    SEARCH_TIMEOUT.saturating_sub(started.elapsed())
}

async fn terminate_search(child: &mut Child, process_group_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(group) = process_group_id {
        // SAFETY: rg is placed in a dedicated process group at spawn time.
        unsafe {
            libc::kill(-(group as i32), libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    let _ = process_group_id;
    let _ = child.start_kill();
    let _ = child.wait().await;
}

async fn read_up_to(
    reader: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
) -> Result<(Vec<u8>, bool)> {
    let mut bytes = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    let truncated = bytes.len() > limit;
    bytes.truncate(limit);
    Ok((bytes, truncated))
}

async fn drain_capped(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>> {
    let mut kept = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(kept.len());
        kept.extend_from_slice(&chunk[..count.min(remaining)]);
    }
    Ok(kept)
}
