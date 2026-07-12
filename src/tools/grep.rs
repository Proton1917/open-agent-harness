use std::process::Stdio;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::process::Command;

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
                "pattern": {"type": "string"}, "path": {"type": "string"},
                "glob": {"type": "string"},
                "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]},
                "-B": {"type": "integer", "minimum": 0}, "-A": {"type": "integer", "minimum": 0},
                "-C": {"type": "integer", "minimum": 0}, "context": {"type": "integer", "minimum": 0},
                "-n": {"type": "boolean"}, "-i": {"type": "boolean"}, "type": {"type": "string"},
                "head_limit": {"type": "integer", "minimum": 0}, "offset": {"type": "integer", "minimum": 0},
                "multiline": {"type": "boolean"}
            }),
            &["pattern"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
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
        command.arg("--").arg(&input.pattern).arg(&path);
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let output = command
            .output()
            .await
            .context("无法启动 rg；请确认 ripgrep 已安装")?;
        if !output.status.success() && output.status.code() != Some(1) {
            bail!(
                "rg 失败: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
        }
        let raw = String::from_utf8_lossy(&output.stdout);
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
            return Ok(ToolOutput::success("No matches found"));
        }
        let truncated = lines.len().saturating_sub(input.offset) > selected.len();
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
