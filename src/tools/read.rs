use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader};

use super::{MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, object_schema, parse_input};

const MAX_SIZE: u64 = MAX_EDITABLE_FILE_BYTES as u64;
const MAX_APPROX_TOKENS: usize = 25_000;
const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_PARTIAL_SCAN_BYTES: usize = 16 * 1024 * 1024;
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
    fn name(&self) -> &str {
        "Read"
    }
    fn description(&self) -> &str {
        "Reads a text file with 1-based line numbers. Use offset and limit for targeted ranges."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "maxLength": 4096, "description": "Absolute or working-directory-relative file path"},
                "offset": {"type": "integer", "minimum": 1, "maximum": 10000000},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000000}
            }),
            &["file_path"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &["file_path"]
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
        if let Ok(canonical) = std::fs::canonicalize(&path) {
            block_device(&canonical)?;
        }
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("文件不存在或不可访问: {}", path.display()))?;
        if !metadata.is_file() {
            bail!("路径不是普通文件: {}", path.display())
        }
        let partial = input.limit.is_some() || input.offset != 1;
        if !partial && metadata.len() > MAX_SIZE {
            bail!(
                "文件大小 {} 字节，超过 {} 字节限制；请使用 offset/limit 分段读取",
                metadata.len(),
                MAX_SIZE
            )
        }
        if partial {
            let (rendered, observed, lines_seen) =
                read_partial(&path, input.offset, input.limit).await?;
            let result = if rendered.is_empty() {
                format!(
                    "Warning: file has only {lines_seen} lines; offset was {}",
                    input.offset
                )
            } else {
                rendered
            };
            context.remember_read(path, observed, true).await?;
            return Ok(ToolOutput::success(result));
        }
        let mut bytes = Vec::new();
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("无法打开 {}", path.display()))?
            .take(MAX_SIZE + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > MAX_SIZE as usize {
            bail!(
                "文件读取时增长到超过 {} 字节限制；请使用 offset/limit 分段读取",
                MAX_SIZE
            )
        }
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

async fn read_partial(
    path: &Path,
    offset: usize,
    limit: Option<usize>,
) -> Result<(String, String, usize)> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("无法读取 {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut raw_line = Vec::new();
    let mut rendered = String::new();
    let mut observed = String::new();
    let mut line_number = 0usize;
    let mut selected = 0usize;
    let mut scanned = 0usize;
    let limit = limit.unwrap_or(usize::MAX);

    loop {
        if selected >= limit {
            break;
        }
        if !read_line_limited(&mut reader, &mut raw_line).await? {
            break;
        }
        scanned = scanned
            .checked_add(raw_line.len())
            .context("partial Read 扫描大小溢出")?;
        if scanned > MAX_PARTIAL_SCAN_BYTES {
            bail!("partial Read 扫描超过 {MAX_PARTIAL_SCAN_BYTES} 字节限制；请缩小 offset")
        }
        line_number += 1;
        if raw_line.contains(&0) {
            bail!("文件看起来是二进制文件，当前 Read 文本路径不支持")
        }
        if line_number < offset {
            continue;
        }
        let line = std::str::from_utf8(&raw_line).context("文件不是有效 UTF-8 文本")?;
        let display = line.trim_end_matches(['\r', '\n']);
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&format!("{line_number:>6}→{display}"));
        observed.push_str(line);
        selected += 1;
        if rendered.len() / 4 > MAX_APPROX_TOKENS {
            bail!("读取结果估算超过 {MAX_APPROX_TOKENS} tokens；请缩小 offset/limit 范围")
        }
    }
    Ok((rendered, observed, line_number))
}

async fn read_line_limited<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    output: &mut Vec<u8>,
) -> Result<bool> {
    output.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!output.is_empty());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(take) > MAX_LINE_BYTES {
            bail!("单行超过 {MAX_LINE_BYTES} 字节限制")
        }
        output.extend_from_slice(&available[..take]);
        let found_newline = take < available.len() || available[take - 1] == b'\n';
        reader.consume(take);
        if found_newline {
            return Ok(true);
        }
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
