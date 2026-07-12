use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

const MAX_SIZE: u64 = 256 * 1024;
const MAX_APPROX_TOKENS: usize = 25_000;
const BLOCKED_DEVICES: &[&str] = &[
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/full",
    "/dev/stdin",
    "/dev/tty",
    "/dev/console",
    "/dev/stdout",
    "/dev/stderr",
    "/dev/fd/0",
    "/dev/fd/1",
    "/dev/fd/2",
];

#[derive(Deserialize)]
struct Input {
    file_path: String,
    #[serde(default = "default_offset")]
    offset: usize,
    limit: Option<usize>,
}

fn default_offset() -> usize {
    1
}

pub struct ReadTool;

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &'static str {
        "Read"
    }
    fn description(&self) -> &'static str {
        "Reads a text file with 1-based line numbers. Use offset and limit for targeted ranges."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "description": "Absolute or working-directory-relative file path"},
                "offset": {"type": "integer", "minimum": 1},
                "limit": {"type": "integer", "minimum": 1}
            }),
            &["file_path"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("file_path")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        if input.offset == 0 {
            bail!("offset 必须从 1 开始")
        }
        let path = context.resolve_path(&input.file_path)?;
        block_device(&path)?;
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("文件不存在或不可访问: {}", path.display()))?;
        if !metadata.is_file() {
            bail!("路径不是普通文件: {}", path.display())
        }
        if input.limit.is_none() && metadata.len() > MAX_SIZE {
            bail!(
                "文件大小 {} 字节，超过 {} 字节限制；请使用 offset/limit 分段读取",
                metadata.len(),
                MAX_SIZE
            )
        }
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("无法读取 {}", path.display()))?;
        if bytes.contains(&0) {
            bail!("文件看起来是二进制文件，当前 Read 文本路径不支持")
        }
        let content = String::from_utf8(bytes).context("文件不是有效 UTF-8 文本")?;
        let lines: Vec<&str> = content.split('\n').collect();
        let start = input.offset.saturating_sub(1);
        let end = input
            .limit
            .map(|n| start.saturating_add(n))
            .unwrap_or(lines.len())
            .min(lines.len());
        let selected = if start >= lines.len() {
            &[][..]
        } else {
            &lines[start..end]
        };
        let rendered = selected
            .iter()
            .enumerate()
            .map(|(index, line)| {
                format!(
                    "{:>6}→{}",
                    input.offset + index,
                    line.strip_suffix('\r').unwrap_or(line)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        if rendered.len() / 4 > MAX_APPROX_TOKENS {
            bail!("读取结果估算超过 {MAX_APPROX_TOKENS} tokens；请缩小 offset/limit 范围")
        }
        let partial = input.limit.is_some() || input.offset != 1;
        let result = if rendered.is_empty() {
            if lines.len() == 1 && lines[0].is_empty() {
                "Warning: file exists but is empty".to_owned()
            } else {
                format!(
                    "Warning: file has only {} lines; offset was {}",
                    lines.len(),
                    input.offset
                )
            }
        } else {
            rendered
        };
        context.remember_read(path, content, partial).await?;
        Ok(ToolOutput::success(result))
    }
}

fn block_device(path: &Path) -> Result<()> {
    let text = path.to_string_lossy();
    if BLOCKED_DEVICES.iter().any(|blocked| text == *blocked)
        || (text.starts_with("/proc/")
            && ["/fd/0", "/fd/1", "/fd/2"]
                .iter()
                .any(|suffix| text.ends_with(suffix)))
    {
        bail!("拒绝读取会阻塞或产生无限输出的设备文件: {text}")
    }
    Ok(())
}
